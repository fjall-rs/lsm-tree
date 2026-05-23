// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

/// Trait for extracting prefixes from keys for prefix filters.
///
/// A prefix extractor allows the filter to index prefixes of keys
/// instead of (or in addition to) the full keys.
/// This enables efficient filtering for prefix-based queries.
///
/// # Contract
///
/// Implementations must satisfy the following invariants. Violating them can
/// cause incorrect filter pruning, which manifests as silent false negatives
/// on reads (data appears missing).
///
/// 1. **Method consistency**: `extract_first(key)` MUST equal
///    `extract(key).next()`. `extract_last(key)` MUST equal `extract(key).last()`.
///    The read path preferentially calls `extract_first` or `extract_last`;
///    if they disagree with `extract`, reads will probe with hashes the writer
///    never registered.
///
/// 2. **`name()` stability and uniqueness**: The string returned by `name()`
///    is persisted in table metadata and used as the *sole* compatibility key
///    on reopen. It MUST be:
///    - **Stable**: the same instance returns the same name across calls and
///      across process restarts.
///    - **Changed whenever extraction behavior changes**: e.g. changing the
///      prefix length, the delimiter, or any other parameter that affects
///      what bytes `extract` produces.
///
/// 3. **Reserved name namespaces**: User implementations MUST NOT return names
///    that collide with built-in extractors. The following prefixes are
///    reserved:
///    - `fixed_prefix:` — used by [`FixedPrefixExtractor`]
///    - `fixed_length:` — used by [`FixedLengthExtractor`]
///    - `full_key` — used by [`FullKeyExtractor`]
///
///    A user impl that returns e.g. `"fixed_prefix:4"` will silently appear
///    compatible with `FixedPrefixExtractor::new(4)` on reopen and produce
///    wrong results. Prefer a namespaced identifier such as
///    `"mycrate::MyExtractor:v1"`.
///
/// 4. **Determinism**: Given the same input bytes, `extract` (and the helper
///    methods) MUST return the same prefixes. Stateless implementations are
///    strongly recommended; stateful ones must be carefully synchronized
///    with writes vs reads.
///
/// 5. **Thread safety**: Implementations are required to be `Send + Sync` so
///    they can be shared via `Arc<dyn PrefixExtractor>`. All trait methods
///    take `&self`; internal mutation must be synchronized.
///
/// 6. **Panic safety**: Implementations must be `UnwindSafe + RefUnwindSafe`.
///    This lets callers wrap tree operations in `std::panic::catch_unwind`
///    without the extractor's panic safety becoming a barrier. Pure /
///    stateless implementations satisfy this automatically; if you hold
///    interior mutability (e.g. `RefCell`, `Mutex`), wrap it appropriately
///    or use `AssertUnwindSafe` at the type boundary.
///
/// # Examples
///
/// ## Simple fixed-length
///
/// ```
/// use lsm_tree::prefix::{PrefixExtractor, FixedPrefixExtractor};
///
/// let ex = FixedPrefixExtractor::new(3);
/// assert_eq!(ex.extract_first(b"abcdef"), Some(b"abc".as_ref()));
/// assert_eq!(ex.extract_first(b"ab"), None); // shorter than prefix length
/// ```
///
/// ## Segmented prefixes (e.g., `account_id#user_id`)
///
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
///
/// struct SegmentedPrefixExtractor;
///
/// impl PrefixExtractor for SegmentedPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         let mut prefixes = vec![];
///         let mut end = 0;
///         for (i, &byte) in key.iter().enumerate() {
///             if byte == b'#' {
///                 prefixes.push(&key[0..i]);
///                 end = i;
///             }
///         }
///         if end < key.len() {
///             prefixes.push(key);
///         }
///         Box::new(prefixes.into_iter())
///     }
///
///     fn name(&self) -> &str {
///         "mycrate::SegmentedPrefixExtractor:v1"
///     }
/// }
///
/// let ex = SegmentedPrefixExtractor;
/// let prefixes: Vec<_> = ex.extract(b"acc#usr#data").collect();
/// assert_eq!(prefixes, vec![b"acc".as_ref(), b"acc#usr", b"acc#usr#data"]);
/// ```
///
/// ## Hierarchical prefixes with a delimiter
///
/// For key `user/123/data` with delimiter `/`, generates `user`, `user/123`,
/// and `user/123/data` (the full key).
///
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
///
/// struct HierarchicalPrefixExtractor {
///     delimiter: u8,
/// }
///
/// impl PrefixExtractor for HierarchicalPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         let delimiter = self.delimiter;
///         let mut prefixes = Vec::new();
///         for (i, &byte) in key.iter().enumerate() {
///             if byte == delimiter {
///                 prefixes.push(&key[0..i]);
///             }
///         }
///         prefixes.push(key);
///         Box::new(prefixes.into_iter())
///     }
///
///     fn name(&self) -> &str {
///         "mycrate::HierarchicalPrefixExtractor:v1"
///     }
/// }
/// ```
///
/// ## Domain prefix for flipped email keys
///
/// For `example.com@user`, extracts `example.com` (domain prefix for range
/// scans) and `example.com@user` (the full key).
///
/// ```
/// use lsm_tree::prefix::PrefixExtractor;
///
/// struct DomainPrefixExtractor;
///
/// impl PrefixExtractor for DomainPrefixExtractor {
///     fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         if let Ok(key_str) = std::str::from_utf8(key) {
///             if let Some(at_pos) = key_str.find('@') {
///                 let domain_prefix = &key[..at_pos];
///                 return Box::new(vec![domain_prefix, key].into_iter());
///             }
///         }
///         Box::new(std::iter::once(key))
///     }
///
///     fn name(&self) -> &str {
///         "mycrate::DomainPrefixExtractor:v1"
///     }
/// }
///
/// let ex = DomainPrefixExtractor;
/// let prefixes: Vec<_> = ex.extract(b"example.com@user").collect();
/// assert_eq!(prefixes, vec![b"example.com".as_ref(), b"example.com@user"]);
/// ```
pub trait PrefixExtractor:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Extracts zero or more prefixes from a key.
    ///
    /// All prefixes will be added to the filter during table construction.
    ///
    /// An empty iterator means the key is "out of domain" and won't be added to the filter.
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>;

    /// Extracts the first prefix from a key.
    ///
    /// By default, this is derived from `extract`, meaning it is equivalent to `extract(key).next()`,
    /// however it can be overridden to skip the Box allocation of `extract` in some cases.
    ///
    /// Implementations that override this method must remain semantically identical to
    /// `self.extract(key).next()`: return `None` if `extract` would yield no prefixes,
    /// and otherwise return the same first prefix. Violating this invariant can cause
    /// incorrect filter pruning during reads.
    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        self.extract(key).next()
    }

    /// Extracts the most specific (last) prefix from a key.
    ///
    /// For single-prefix extractors, this is the same as `extract_first`.
    /// For multi-prefix extractors, this returns the highest-cardinality
    /// prefix, which gives the best Bloom filter pruning.
    ///
    /// Defaults to consuming `extract(key)` to get the last element.
    /// Can be overridden to avoid the Box allocation when the last prefix
    /// can be computed directly.
    ///
    /// Implementations that override this method must remain semantically
    /// identical to `self.extract(key).last()`: return `None` if `extract`
    /// would yield no prefixes, and otherwise return the same last prefix.
    /// Violating this invariant can cause incorrect filter pruning during
    /// range reads, which preferentially call `extract_last` for best
    /// pruning specificity.
    fn extract_last<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        self.extract(key).last()
    }

    /// Returns a stable compatibility identifier for this prefix extractor.
    ///
    /// This value is persisted in table metadata and compared on reopen as the
    /// *sole* compatibility key. See the trait-level [Contract](#contract)
    /// section for the full set of requirements; in summary:
    ///
    /// - Must change whenever extraction behavior changes (length, delimiter,
    ///   etc.).
    /// - Must be stable across process restarts for the same instance.
    /// - Must not collide with built-in name namespaces (`fixed_prefix:`,
    ///   `fixed_length:`, `full_key`). Prefer a namespaced identifier such
    ///   as `"mycrate::MyExtractor:v1"`.
    fn name(&self) -> &str;

    /// Optional optimization hint: returns `true` if this extractor produces
    /// at most one prefix per key (i.e. `extract(key).count() <= 1` for all
    /// possible inputs).
    ///
    /// When `true`, the writer hot path can call `extract_first` directly and
    /// skip the `Box<dyn Iterator>` allocation that `extract` requires. This
    /// avoids one heap allocation per user key written for the common case
    /// of fixed-length / single-prefix extractors.
    ///
    /// Default: `false` (conservative — the writer iterates `extract` to
    /// collect all prefixes). Override to `true` only when the invariant
    /// genuinely holds. Returning `true` while `extract` actually yields
    /// multiple prefixes will cause every prefix *after the first* to be
    /// omitted from the filter, resulting in false negatives on read.
    fn is_single_prefix(&self) -> bool {
        false
    }
}

/// A prefix extractor that returns the full key.
///
/// Useful when callers want prefix-aware filtering that behaves identically to
/// full-key filtering (e.g., for testing or as an explicit no-op extractor).
pub struct FullKeyExtractor;

impl PrefixExtractor for FullKeyExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(std::iter::once(key))
    }

    fn extract_last<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        self.extract_first(key)
    }

    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        Some(key)
    }

    fn name(&self) -> &'static str {
        "full_key"
    }

    fn is_single_prefix(&self) -> bool {
        true
    }
}

/// A prefix extractor that returns a fixed-length prefix.
///
/// Keys shorter than the prefix length are considered "out of domain" and
/// return `None`. This prevents the prefix filter from producing false
/// negatives when a prefix query key is shorter than the configured length.
pub struct FixedPrefixExtractor {
    length: usize,
    name: String,
}

impl FixedPrefixExtractor {
    /// Creates a new fixed-length prefix extractor.
    #[must_use]
    pub fn new(length: usize) -> Self {
        Self {
            length,
            name: format!("fixed_prefix:{length}"),
        }
    }
}

impl PrefixExtractor for FixedPrefixExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(self.extract_first(key).into_iter())
    }

    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        if key.len() < self.length {
            None
        } else {
            key.get(..self.length)
        }
    }

    fn extract_last<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        self.extract_first(key)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_prefix(&self) -> bool {
        true
    }
}

/// A prefix extractor that requires keys to be at least a certain length.
///
/// Keys shorter than the required length are considered "out of domain"
/// and won't be added to the filter.
/// This matches `RocksDB`'s behavior.
pub struct FixedLengthExtractor {
    length: usize,
    name: String,
}

impl FixedLengthExtractor {
    /// Creates a new fixed-length extractor.
    #[must_use]
    pub fn new(length: usize) -> Self {
        Self {
            length,
            name: format!("fixed_length:{length}"),
        }
    }
}

impl PrefixExtractor for FixedLengthExtractor {
    fn extract<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(self.extract_first(key).into_iter())
    }

    fn extract_first<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        if key.len() < self.length {
            // Key is too short - out of domain
            None
        } else {
            key.get(..self.length)
        }
    }

    fn extract_last<'a>(&self, key: &'a [u8]) -> Option<&'a [u8]> {
        self.extract_first(key)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn is_single_prefix(&self) -> bool {
        true
    }
}

/// A shared (`Arc`-backed) prefix extractor.
///
/// This is the canonical owned type plugged into [`crate::Config::prefix_extractor`].
/// Cloning is cheap (atomic refcount bump).
///
/// See the [`PrefixExtractor`] trait for the implementation contract.
pub type SharedPrefixExtractor = Arc<dyn PrefixExtractor>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_key_extractor() {
        let extractor = FullKeyExtractor;
        let key = b"test_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"test_key".as_ref()));
    }

    #[test]
    fn test_fixed_prefix_extractor() {
        let extractor = FixedPrefixExtractor::new(5);

        // Key longer than prefix
        let key = b"longer_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"longe".as_ref()));

        // Key shorter than prefix — out of domain
        let key = b"key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 0);

        // Key exactly prefix length
        let key = b"exact";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"exact".as_ref()));
    }

    #[test]
    fn test_empty_key() {
        let full_key = FullKeyExtractor;
        let fixed = FixedPrefixExtractor::new(5);

        let key = b"";

        let prefixes: Vec<_> = full_key.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"".as_ref()));

        let prefixes: Vec<_> = fixed.extract(key).collect();
        assert_eq!(prefixes.len(), 0); // empty key is shorter than prefix length
    }

    #[test]
    fn test_fixed_length_extractor() {
        let extractor = FixedLengthExtractor::new(5);

        // Key shorter than required length - out of domain
        let key = b"abc";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 0); // Empty iterator

        // Key exactly required length
        let key = b"exact";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"exact".as_ref()));

        // Key longer than required length
        let key = b"longer_key";
        let prefixes: Vec<_> = extractor.extract(key).collect();
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes.first(), Some(&b"longe".as_ref()));
    }

    #[test]
    fn test_extractor_names() {
        assert_eq!(FullKeyExtractor.name(), "full_key");
        assert_eq!(FixedPrefixExtractor::new(4).name(), "fixed_prefix:4");
        assert_eq!(FixedPrefixExtractor::new(3).name(), "fixed_prefix:3");
        assert_eq!(FixedLengthExtractor::new(4).name(), "fixed_length:4");
        assert_eq!(FixedLengthExtractor::new(3).name(), "fixed_length:3");
    }
}
