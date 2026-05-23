// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    key::InternalKey,
    memtable::Memtable,
    merge::Merger,
    mvcc_stream::MvccStream,
    prefix::SharedPrefixExtractor,
    run_reader::RunReader,
    value::{SeqNo, UserKey},
    version::SuperVersion,
    BoxedIterator, InternalValue,
};
use self_cell::self_cell;
use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

#[must_use]
pub fn seqno_filter(item_seqno: SeqNo, seqno: SeqNo) -> bool {
    item_seqno < seqno
}

/// Calculates the prefix's upper range.
///
/// # Panics
///
/// Panics if the prefix is empty.
pub(crate) fn prefix_upper_range(prefix: &[u8]) -> Bound<UserKey> {
    use std::ops::Bound::{Excluded, Unbounded};

    assert!(!prefix.is_empty(), "prefix may not be empty");

    let mut end = prefix.to_vec();
    let len = end.len();

    for (idx, byte) in end.iter_mut().rev().enumerate() {
        let idx = len - 1 - idx;

        if *byte < 255 {
            *byte += 1;
            end.truncate(idx + 1);
            return Excluded(end.into());
        }
    }

    Unbounded
}

/// Converts a prefix to range bounds.
#[must_use]
#[expect(clippy::module_name_repetitions)]
pub fn prefix_to_range(prefix: &[u8]) -> (Bound<UserKey>, Bound<UserKey>) {
    use std::ops::Bound::{Included, Unbounded};

    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    (Included(prefix.into()), prefix_upper_range(prefix))
}

/// Owner state for a [`TreeIter`] range iterator.
///
/// Holds everything the dependent iterator borrows from for the lifetime of
/// the iteration: the version snapshot, memtables, and the prefix extractor /
/// hint that drive prefix-filter pruning.
///
/// Because the dependent iterator borrows from this state, we use `self_cell`
/// to express the self-referential structure safely.
pub struct IterState {
    pub(crate) version: SuperVersion,
    pub(crate) ephemeral: Option<(Arc<Memtable>, SeqNo)>,

    /// The current extractor used for prefix-filter pruning during iteration.
    /// `None` disables prefix filtering for this iter; the reader still
    /// honors each table's compatibility check via `Table::prefix_filter_allowed`.
    pub(crate) prefix_extractor: Option<SharedPrefixExtractor>,

    /// When set, this is the original prefix from a `tree.prefix()` call.
    /// It allows the filter layer to consult the prefix filter even when the
    /// range bounds (produced by `prefix_to_range`) have different extracted
    /// prefixes (e.g. prefix `"h"` → bounds `("h", "i")` with a 1-byte
    /// extractor). When `prefix_extractor` is also set, [`TreeIter::create_range`]
    /// validates the hint and precomputes its hash once, reusing it across
    /// the L0 single-table path and the multi-table [`RunReader`] path.
    pub(crate) prefix_hint: Option<UserKey>,
}

type BoxedMerge<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + Send + 'a>;

self_cell!(
    pub struct TreeIter {
        owner: IterState,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl Iterator for TreeIter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for TreeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

impl TreeIter {
    #[expect(
        clippy::too_many_lines,
        reason = "extended with prefix-hint validation and upfront pruning"
    )]
    pub fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        guard: IterState,
        range: R,
        seqno: SeqNo,
    ) -> Self {
        Self::new(guard, |lock| {
            // Precompute the validated prefix hash once for the L0
            // single-table path. The multi-table path (`RunReader::new`)
            // still re-validates internally — both produce identical
            // results since `validate_hint_and_hash` is deterministic, but
            // sharing the precomputed hash across L0 tables avoids
            // re-extracting and re-hashing per overlapping L0 table.
            //
            // (If the multi-table path is later refactored to accept the
            // precomputed hash, this same value can be passed through.)
            let precomputed_hint_hash: Option<u64> =
                match (lock.prefix_extractor.as_ref(), lock.prefix_hint.as_deref()) {
                    (Some(ex), Some(hint)) => {
                        crate::table::validate_hint_and_hash(ex.as_ref(), hint)
                    }
                    _ => None,
                };

            let lo = match range.start_bound() {
                // NOTE: See memtable.rs for range explanation
                Bound::Included(key) => Bound::Included(InternalKey::new(
                    key.as_ref(),
                    SeqNo::MAX,
                    crate::ValueType::Tombstone,
                )),
                Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                    key.as_ref(),
                    0,
                    crate::ValueType::Tombstone,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let hi = match range.end_bound() {
                // NOTE: See memtable.rs for range explanation, this is the reverse case
                // where we need to go all the way to the last seqno of an item
                //
                // Example: We search for (Unbounded..Excluded(abdef))
                //
                // key -> seqno
                //
                // a   -> 7 <<< This is the lowest key that matches the range
                // abc -> 5
                // abc -> 4
                // abc -> 3 <<< This is the highest key that matches the range
                // abcdef -> 6
                // abcdef -> 5
                //
                Bound::Included(key) => {
                    Bound::Included(InternalKey::new(key.as_ref(), 0, crate::ValueType::Value))
                }
                Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                    key.as_ref(),
                    SeqNo::MAX,
                    crate::ValueType::Value,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let range = (lo, hi);

            // Materialize user-key bounds ONCE for the per-run/per-table
            // iteration below. Each per-table call needs owned bounds
            // (consumed by `table.range()` and `RunReader::new`); materializing
            // them here lets us:
            //   - skip the per-call `range.start_bound().map(|x| &x.user_key).cloned()`
            //     dance, and
            //   - pass the same materialized bounds (via `as_ref()`) into
            //     `should_skip_range_by_prefix_filter` without an extra clone
            //     for the ref_range tuple.
            let user_start_owned: Bound<UserKey> = match range.start_bound() {
                Bound::Included(k) => Bound::Included(k.user_key.clone()),
                Bound::Excluded(k) => Bound::Excluded(k.user_key.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };
            let user_end_owned: Bound<UserKey> = match range.end_bound() {
                Bound::Included(k) => Bound::Included(k.user_key.clone()),
                Bound::Excluded(k) => Bound::Excluded(k.user_key.clone()),
                Bound::Unbounded => Bound::Unbounded,
            };

            let mut iters: Vec<BoxedIterator<'_>> = Vec::with_capacity(5);

            for run in lock
                .version
                .version
                .iter_levels()
                .flat_map(|lvl| lvl.iter())
            {
                match run.len() {
                    0 => {
                        // Do nothing
                    }
                    1 => {
                        #[expect(clippy::expect_used, reason = "we checked for length")]
                        let table = run.first().expect("should exist");

                        if table.check_key_range_overlap(&(
                            range.start_bound().map(|x| &*x.user_key),
                            range.end_bound().map(|x| &*x.user_key),
                        )) {
                            let mut skip = false;
                            if let Some(ex) = lock.prefix_extractor.as_ref() {
                                // Fast path: when the hint+hash was already
                                // validated above, probe directly with the
                                // precomputed hash. Otherwise fall through to
                                // bounds-based pruning via
                                // `should_skip_range_by_prefix_filter`.
                                //
                                // NOTE: behavior when a hint is provided but
                                // fails the stability guard
                                // (precomputed_hint_hash is None): we now
                                // fall through to bounds-based pruning. This
                                // is a strict improvement over the previous
                                // behavior (which short-circuited to "no
                                // skip" in that case) — bounds-based pruning
                                // is independently sound and matches the
                                // long-standing behavior of
                                // `RunReader::new`'s multi-table path.
                                // Aligning both paths eliminates a
                                // single-vs-multi-table inconsistency.
                                if let (Some(hint), Some(hash)) =
                                    (lock.prefix_hint.as_deref(), precomputed_hint_hash)
                                {
                                    if table.should_skip_with_precomputed_hash(
                                        hint,
                                        hash,
                                        ex.name(),
                                    ) {
                                        skip = true;
                                    }
                                } else {
                                    // Pass borrowed bounds via `as_ref()` —
                                    // `(Bound<&T>, Bound<&T>)` implements
                                    // `RangeBounds<T>`, so no clone needed.
                                    let ref_range: (Bound<&UserKey>, Bound<&UserKey>) =
                                        (user_start_owned.as_ref(), user_end_owned.as_ref());
                                    if table.should_skip_range_by_prefix_filter::<UserKey, _>(
                                        &ref_range,
                                        ex.as_ref(),
                                        None,
                                    ) {
                                        skip = true;
                                    }
                                }
                            }

                            if !skip {
                                let reader = table
                                    .range((user_start_owned.clone(), user_end_owned.clone()))
                                    .filter(move |item| match item {
                                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                                        Err(_) => true,
                                    });

                                iters.push(Box::new(reader));
                            }
                        }
                    }
                    _ => {
                        if let Some(reader) = RunReader::new(
                            run.clone(),
                            (user_start_owned.clone(), user_end_owned.clone()),
                            lock.prefix_extractor.clone(),
                            lock.prefix_hint.as_ref(),
                        ) {
                            iters.push(Box::new(reader.filter(move |item| match item {
                                Ok(item) => seqno_filter(item.key.seqno, seqno),
                                Err(_) => true,
                            })));
                        }
                    }
                }
            }

            // Sealed memtables
            for memtable in lock.version.sealed_memtables.iter() {
                let iter = memtable.range(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            // Active memtable
            {
                let iter = lock.version.active_memtable.range(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            if let Some((mt, seqno)) = &lock.ephemeral {
                let iter = Box::new(
                    mt.range(range)
                        .filter(move |item| seqno_filter(item.key.seqno, *seqno))
                        .map(Ok),
                );
                iters.push(iter);
            }

            let merged = Merger::new(iters);
            let iter = MvccStream::new(merged);

            Box::new(iter.filter(|x| match x {
                Ok(value) => !value.key.is_tombstone(),
                Err(_) => true,
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Slice;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use test_log::test;

    fn test_prefix(prefix: &[u8], upper_bound: Bound<&[u8]>) {
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (
                match prefix {
                    _ if prefix.is_empty() => Unbounded,
                    _ => Included(Slice::from(prefix)),
                },
                upper_bound.map(Slice::from),
            ),
        );
    }

    #[test]
    fn prefix_to_range_basic() {
        test_prefix(b"abc", Excluded(b"abd"));
    }

    #[test]
    fn prefix_to_range_empty() {
        test_prefix(b"", Unbounded);
    }

    #[test]
    fn prefix_to_range_single_char() {
        test_prefix(b"a", Excluded(b"b"));
    }

    #[test]
    fn prefix_to_range_1() {
        test_prefix(&[0, 250], Excluded(&[0, 251]));
    }

    #[test]
    fn prefix_to_range_2() {
        test_prefix(&[0, 250, 50], Excluded(&[0, 250, 51]));
    }

    #[test]
    fn prefix_to_range_3() {
        test_prefix(&[255, 255, 255], Unbounded);
    }

    #[test]
    fn prefix_to_range_char_max() {
        test_prefix(&[0, 255], Excluded(&[1]));
    }

    #[test]
    fn prefix_to_range_char_max_2() {
        test_prefix(&[0, 2, 255], Excluded(&[0, 3]));
    }
}
