// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::table::filter::standard_bloom::Builder;
use std::{borrow::Cow, ops::Bound, sync::Arc};

/// Picks the prefixes of each key that a table's filter indexes, so reads
/// confined to one prefix can skip tables that do not contain it.
///
/// An extractor reports prefixes by their length, so an extracted prefix is
/// always a byte prefix of its key. A key may have several prefixes (for
/// example one per path segment) or none, which puts it *out of domain*.
///
/// # Contract
///
/// Reads skip a table when its filter lacks a prefix that every key in the
/// queried range has. That is only sound if the extractor obeys these rules;
/// breaking one makes reads miss keys.
///
/// 1. **Contiguity.** The keys that have a given prefix are contiguous in key
///    order: if `a < b < c` and `a` and `c` have prefix `p`, so does `b`.
/// 2. **[`Self::same_result_when_appended`]** returns `true` for `p` only if
///    every key that starts with `p` has prefix `p`. The default `false` is
///    always safe.
/// 3. **Determinism.** A key always yields the same prefixes, and
///    [`Self::name`] changes whenever that output would.
///
/// Debug builds check contiguity on every table written and panic on a
/// violation.
///
/// A common way to break contiguity is to also report the whole key. Say an
/// extractor reports the part of a key before `@` and the whole key: `a` and
/// `a@x` both have prefix `a`, but `a!`, which sorts between them, does not.
/// Report `a@` instead, with the delimiter, and leave whole keys to
/// [`crate::Config::whole_key_filtering`].
///
/// # Which reads use the filter
///
/// A range read can skip a table, whether its bounds are inclusive or
/// exclusive, if the filter lacks either of these:
///
/// - a prefix both bounds have, such as `abc` for `abc1..=abc9`;
/// - a prefix `p` of a byte string that every key in the range starts with,
///   where `same_result_when_appended(p)` is `true`. [`crate::AbstractTree::prefix`]
///   relies on this rule, because its upper bound is the first key after the
///   prefix.
///
/// Point reads use the longest prefix of the key when the table was written
/// with `whole_key_filtering` off.
///
/// # Example
///
/// Index each `/`-terminated segment, so `user/42/posts` has the prefixes
/// `user/` and `user/42/`:
///
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
///
/// struct PathSegments;
///
/// impl PrefixExtractor for PathSegments {
///     fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
///         for (i, &byte) in key.iter().enumerate() {
///             if byte == b'/' {
///                 emit(i + 1);
///             }
///         }
///     }
///
///     // Every key that starts with `user/42/` has `user/42/` as a prefix.
///     fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
///         prefix.ends_with(b"/")
///     }
///
///     fn name(&self) -> &str {
///         "mycrate::PathSegments:v1"
///     }
/// }
///
/// let mut prefixes = vec![];
/// PathSegments.extract(b"user/42/posts", &mut |len| prefixes.push(len));
/// assert_eq!(prefixes, [5, 8]);
/// ```
pub trait PrefixExtractor:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Calls `emit` with the length of each prefix of `key` to index.
    ///
    /// Lengths greater than `key.len()` are ignored.
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize));

    /// Returns `true` if every key that starts with `prefix` has `prefix`
    /// among its extracted prefixes.
    ///
    /// Like `RocksDB`'s `SliceTransform::SameResultWhenAppended`, this lets
    /// scans whose upper bound has a different prefix, such as
    /// [`crate::AbstractTree::prefix`], use the filter. Only called with
    /// prefixes `extract` reported.
    fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
        let _ = prefix;
        false
    }

    /// Identifies the extraction behaviour.
    ///
    /// Each table stores the name of the extractor that wrote it. On open, a
    /// table uses its prefix filter only if its stored name matches the
    /// configured extractor or a built-in one. Names starting with
    /// `fixed_prefix:` or `fixed_length:` are reserved for
    /// [`FixedPrefixExtractor`]; prefer a namespaced name such as
    /// `"mycrate::MyExtractor:v1"`.
    fn name(&self) -> &str;
}

/// A shared prefix extractor, as passed to [`crate::Config::prefix_extractor`].
pub type SharedPrefixExtractor = Arc<dyn PrefixExtractor>;

/// Extracts the first `length` bytes of each key.
///
/// Keys shorter than `length` are out of domain, like `RocksDB`'s
/// `NewFixedPrefixTransform`.
pub struct FixedPrefixExtractor {
    length: usize,
    name: String,
}

impl FixedPrefixExtractor {
    /// Creates an extractor for prefixes of `length` bytes.
    #[must_use]
    pub fn new(length: usize) -> Self {
        Self {
            length,
            name: format!("fixed_prefix:{length}"),
        }
    }
}

impl PrefixExtractor for FixedPrefixExtractor {
    fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
        if key.len() >= self.length {
            emit(self.length);
        }
    }

    fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
        prefix.len() == self.length
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Returns the extractor that reads a table written with the extractor `name`.
///
/// Prefers `configured` when its name matches, so tables share one instance.
/// Rebuilds [`FixedPrefixExtractor`] from its name, including the
/// `fixed_length:` alias written by earlier revisions of this feature.
pub(crate) fn resolve(
    name: &str,
    configured: Option<&SharedPrefixExtractor>,
) -> Option<SharedPrefixExtractor> {
    if let Some(extractor) = configured.filter(|x| x.name() == name) {
        return Some(extractor.clone());
    }

    let length = name
        .strip_prefix("fixed_prefix:")
        .or_else(|| name.strip_prefix("fixed_length:"))?
        .parse()
        .ok()?;

    Some(Arc::new(FixedPrefixExtractor::new(length)))
}

/// The prefix filter probe of a range read, computed once per extractor for
/// the tables the read visits.
pub(crate) struct RangeProbe<'a> {
    start: Bound<&'a [u8]>,
    end: Bound<&'a [u8]>,

    /// Probe hash for the extractor of the last table asked about
    cached: Option<(SharedPrefixExtractor, Option<u64>)>,
}

impl<'a> RangeProbe<'a> {
    pub fn new(start: Bound<&'a [u8]>, end: Bound<&'a [u8]>) -> Self {
        Self {
            start,
            end,
            cached: None,
        }
    }

    /// Returns a key no key in the range is below.
    pub fn seek_key(&self) -> &'a [u8] {
        match self.start {
            Bound::Included(key) | Bound::Excluded(key) => key,
            Bound::Unbounded => &[],
        }
    }

    /// Returns the probe hash for tables written with `extractor`.
    pub fn hash(&mut self, extractor: &SharedPrefixExtractor) -> Option<u64> {
        if let Some((cached, hash)) = &self.cached {
            if Arc::ptr_eq(cached, extractor) {
                return *hash;
            }
        }

        let hash = range_probe_hash(extractor.as_ref(), self.start, self.end);
        self.cached = Some((extractor.clone(), hash));
        hash
    }
}

/// Returns the length of the longest prefix `extractor` extracts from `key`.
pub(crate) fn longest_prefix_len(extractor: &dyn PrefixExtractor, key: &[u8]) -> Option<usize> {
    let mut longest = None;

    extractor.extract(key, &mut |len| {
        if len <= key.len() && longest.is_none_or(|x| len > x) {
            longest = Some(len);
        }
    });

    longest
}

/// Returns the filter hash of the longest prefix that every key in the range
/// has, or `None` if no prefix can be shown to cover the range.
///
/// See the "Which reads use the filter" section of [`PrefixExtractor`].
pub(crate) fn range_probe_hash(
    extractor: &dyn PrefixExtractor,
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Option<u64> {
    // Every key in the range is >= `min_key`, so every candidate prefix is a
    // prefix of `min_key`.
    let min_key: Cow<'_, [u8]> = match start {
        Bound::Included(key) => Cow::Borrowed(key),
        Bound::Excluded(key) => Cow::Owned([key, &[0]].concat()),
        Bound::Unbounded => Cow::Borrowed(&[]),
    };

    let mut best: Option<usize> = None;

    // By contiguity, a prefix both bounds have is a prefix of every key
    // between them.
    if let (Bound::Included(lo) | Bound::Excluded(lo), Bound::Included(hi) | Bound::Excluded(hi)) =
        (start, end)
    {
        let shared = common_prefix_len(lo, hi);

        let mut lo_lens = Vec::new();
        extractor.extract(lo, &mut |len| {
            if len <= shared {
                lo_lens.push(len);
            }
        });

        if !lo_lens.is_empty() {
            extractor.extract(hi, &mut |len| {
                if lo_lens.contains(&len) && best.is_none_or(|x| len > x) {
                    best = Some(len);
                }
            });
        }
    }

    // Every key in the range starts with `common`; a prefix of it that every
    // extension keeps is a prefix of every key in the range.
    let common = min_key
        .get(..range_common_prefix_len(&min_key, end))
        .unwrap_or_default();

    extractor.extract(common, &mut |len| {
        if let Some(prefix) = common.get(..len) {
            if best.is_none_or(|x| len > x) && extractor.same_result_when_appended(prefix) {
                best = Some(len);
            }
        }
    });

    best.and_then(|len| min_key.get(..len))
        .map(Builder::get_hash)
}

/// Returns the length of the longest byte string that every key in
/// `[min_key, end)` starts with; it is always a prefix of `min_key`.
fn range_common_prefix_len(min_key: &[u8], end: Bound<&[u8]>) -> usize {
    let ff_run = |from: usize| {
        min_key
            .get(from..)
            .unwrap_or_default()
            .iter()
            .take_while(|&&b| b == 0xFF)
            .count()
    };

    match end {
        Bound::Included(hi) => common_prefix_len(min_key, hi),

        // `head` is the largest key below `head + [0]`
        Bound::Excluded(hi) => match hi.split_last() {
            None => 0,
            Some((&0, head)) => common_prefix_len(min_key, head),

            // The keys in `[head + [last - 1], hi)` are exactly those starting
            // with `head + [last - 1]`, followed by any number of 0xFF bytes
            // the range cannot exceed.
            Some((&last, head)) => {
                let shared = common_prefix_len(min_key, head);

                if shared == head.len() && min_key.get(head.len()) == Some(&(last - 1)) {
                    head.len() + 1 + ff_run(head.len() + 1)
                } else {
                    shared
                }
            }
        },

        Bound::Unbounded => ff_run(0),
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
#[expect(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use std::ops::Bound::{Excluded, Included, Unbounded};

    fn lens(extractor: &dyn PrefixExtractor, key: &[u8]) -> Vec<usize> {
        let mut out = vec![];
        extractor.extract(key, &mut |len| out.push(len));
        out
    }

    struct PathSegments;

    impl PrefixExtractor for PathSegments {
        fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
            for (i, &byte) in key.iter().enumerate() {
                if byte == b'/' {
                    emit(i + 1);
                }
            }
        }

        fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
            prefix.ends_with(b"/")
        }

        fn name(&self) -> &'static str {
            "test::PathSegments"
        }
    }

    /// Like `PathSegments`, but never claims a prefix covers its extensions.
    struct PathSegmentsNoHook;

    impl PrefixExtractor for PathSegmentsNoHook {
        fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
            PathSegments.extract(key, emit);
        }

        fn name(&self) -> &'static str {
            "test::PathSegmentsNoHook"
        }
    }

    #[expect(clippy::unnecessary_wraps, reason = "compared with probe results")]
    fn hash(bytes: &[u8]) -> Option<u64> {
        Some(Builder::get_hash(bytes))
    }

    #[test]
    fn fixed_prefix_extracts_first_bytes() {
        let ex = FixedPrefixExtractor::new(3);
        assert_eq!(lens(&ex, b"abcdef"), [3]);
        assert_eq!(lens(&ex, b"abc"), [3]);
        assert!(lens(&ex, b"ab").is_empty());
        assert!(lens(&ex, b"").is_empty());
        assert_eq!(ex.name(), "fixed_prefix:3");

        // "ab" lacks a prefix and "abcd" has "abc", so only a 3-byte string
        // is the prefix of every key starting with it
        assert!(ex.same_result_when_appended(b"abc"));
        assert!(!ex.same_result_when_appended(b"ab"));
        assert!(!ex.same_result_when_appended(b"abcd"));
    }

    #[test]
    fn longest_prefix_len_picks_longest_in_bounds() {
        assert_eq!(longest_prefix_len(&PathSegments, b"a/b/c"), Some(4));
        assert_eq!(longest_prefix_len(&PathSegments, b"abc"), None);
        assert_eq!(
            longest_prefix_len(&FixedPrefixExtractor::new(3), b"ab"),
            None
        );
    }

    #[test]
    fn resolve_prefers_configured_then_rebuilds_fixed() {
        let configured: SharedPrefixExtractor = Arc::new(FixedPrefixExtractor::new(4));

        let same = resolve("fixed_prefix:4", Some(&configured)).unwrap();
        assert!(Arc::ptr_eq(&same, &configured));

        let rebuilt = resolve("fixed_prefix:8", Some(&configured)).unwrap();
        assert_eq!(rebuilt.name(), "fixed_prefix:8");

        let alias = resolve("fixed_length:2", None).unwrap();
        assert_eq!(lens(alias.as_ref(), b"abc"), [2]);

        assert!(resolve("mycrate::Unknown", Some(&configured)).is_none());
        assert!(resolve("fixed_prefix:x", None).is_none());
    }

    #[test]
    fn range_probe_bounds_sharing_a_prefix() {
        let ex = FixedPrefixExtractor::new(3);
        assert_eq!(
            range_probe_hash(&ex, Included(b"abc1"), Included(b"abc9")),
            hash(b"abc"),
        );
        assert_eq!(
            range_probe_hash(&ex, Excluded(b"abc1"), Excluded(b"abc9")),
            hash(b"abc"),
        );
        assert_eq!(
            range_probe_hash(&PathSegmentsNoHook, Included(b"a/b/1"), Included(b"a/b/9")),
            hash(b"a/b/"),
        );
        // The bounds share "a/" but not "a/b/"
        assert_eq!(
            range_probe_hash(&PathSegmentsNoHook, Included(b"a/b/1"), Included(b"a/c/9")),
            hash(b"a/"),
        );
    }

    /// The smallest key after an excluded start is the start followed by 0
    #[test]
    fn range_probe_excluded_start_is_followed_by_zero() {
        let ex = FixedPrefixExtractor::new(3);
        // Every key in ("ab", "ab\x01") starts with "ab\x00"
        assert_eq!(
            range_probe_hash(&ex, Excluded(b"ab"), Excluded(b"ab\x01")),
            hash(b"ab\x00")
        );
        assert_eq!(
            range_probe_hash(&ex, Included(b"ab"), Excluded(b"ab\x01")),
            None
        );
    }

    #[test]
    fn range_probe_prefix_scan_needs_hook() {
        // `prefix("abc")` is `abc..abd`: the bounds have different prefixes
        let ex = FixedPrefixExtractor::new(3);
        assert_eq!(
            range_probe_hash(&ex, Included(b"abc"), Excluded(b"abd")),
            hash(b"abc")
        );
        assert_eq!(
            range_probe_hash(&ex, Included(b"abcd"), Excluded(b"abce")),
            hash(b"abc")
        );

        // Too short to have a prefix
        assert_eq!(
            range_probe_hash(&ex, Included(b"ab"), Excluded(b"ac")),
            None
        );

        assert_eq!(
            range_probe_hash(&PathSegments, Included(b"a/b/"), Excluded(b"a/b0")),
            hash(b"a/b/"),
        );
        // Without the hook only the bounds' shared prefix "a/" is usable
        assert_eq!(
            range_probe_hash(&PathSegmentsNoHook, Included(b"a/b/"), Excluded(b"a/b0")),
            hash(b"a/"),
        );
        assert_eq!(
            range_probe_hash(&PathSegmentsNoHook, Included(b"a/b/"), Excluded(b"a0")),
            None,
        );
    }

    #[test]
    fn range_probe_unbounded_side_disables_bound_rule() {
        let ex = FixedPrefixExtractor::new(3);
        assert_eq!(range_probe_hash(&ex, Unbounded, Included(b"abc")), None);
        assert_eq!(range_probe_hash(&ex, Included(b"abc"), Unbounded), None);
        assert_eq!(range_probe_hash(&ex, Unbounded, Unbounded), None);
        // Every key >= "\xff\xff\xff" starts with "\xff\xff\xff"
        assert_eq!(
            range_probe_hash(&ex, Included(b"\xff\xff\xffa"), Unbounded),
            hash(b"\xff\xff\xff"),
        );
    }

    #[test]
    fn range_common_prefix_worked_examples() {
        // [abc5, abd): every key starts with "abc"
        assert_eq!(range_common_prefix_len(b"abc5", Excluded(b"abd")), 3);
        // [ab, abc): "aba..." and "abb..." are in range
        assert_eq!(range_common_prefix_len(b"ab", Excluded(b"abc")), 2);
        // [abc, abc\0) is just "abc"
        assert_eq!(range_common_prefix_len(b"abc", Excluded(b"abc\0")), 3);
        // [ab\xff\xffc, ac): every key starts with "ab\xff\xff"
        assert_eq!(range_common_prefix_len(b"ab\xff\xffc", Excluded(b"ac")), 4);
        // [a, b]: "a" and "b" are both in range
        assert_eq!(range_common_prefix_len(b"a", Included(b"b")), 0);
        // [x, "") is empty
        assert_eq!(range_common_prefix_len(b"x", Excluded(b"")), 0);
    }

    /// Checks `range_common_prefix_len` against brute force. Bounds use bytes
    /// 0x00, 0x7F and 0xFF; range members also use their neighbours, so every
    /// gap a bound leaves has a member in it.
    #[test]
    fn range_common_prefix_matches_brute_force() {
        fn keys(alphabet: &[u8], max_len: usize) -> Vec<Vec<u8>> {
            let mut out = vec![vec![]];
            let mut frontier = vec![vec![]];
            for _ in 0..max_len {
                let mut next = vec![];
                for key in &frontier {
                    for &b in alphabet {
                        let mut k: Vec<u8> = key.clone();
                        k.push(b);
                        next.push(k);
                    }
                }
                out.extend(next.iter().cloned());
                frontier = next;
            }
            out.sort();
            out
        }

        let bounds = keys(&[0x00, 0x7F, 0xFF], 3);
        let members = keys(&[0x00, 0x01, 0x7E, 0x7F, 0x80, 0xFE, 0xFF], 4);

        for min_key in &bounds {
            for end in &bounds {
                for end in [Included(end.as_slice()), Excluded(end.as_slice())] {
                    let in_range: Vec<&Vec<u8>> = members
                        .iter()
                        .filter(|k| k.as_slice() >= min_key.as_slice())
                        .filter(|k| match end {
                            Included(e) => k.as_slice() <= e,
                            Excluded(e) => k.as_slice() < e,
                            Unbounded => true,
                        })
                        .collect();

                    if in_range.is_empty() {
                        continue;
                    }

                    let len = range_common_prefix_len(min_key, end);
                    let prefix = &min_key[..len];

                    for k in &in_range {
                        assert!(
                            k.starts_with(prefix),
                            "{k:?} in [{min_key:?}, {end:?}) lacks {prefix:?}",
                        );
                    }

                    // No longer prefix of `min_key` is shared by every member
                    if let Some(longer) = min_key.get(..=len) {
                        assert!(
                            in_range.iter().any(|k| !k.starts_with(longer)),
                            "[{min_key:?}, {end:?}): {longer:?} is also common",
                        );
                    }
                }
            }
        }
    }
}
