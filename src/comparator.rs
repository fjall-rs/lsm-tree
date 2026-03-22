// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::Arc;

/// Trait for custom user key comparison.
///
/// Comparators must be safe across unwind boundaries since they are stored
/// in tree structures that may be referenced inside `catch_unwind` blocks.
///
/// Implementations must define a **strict total order** suitable for use in
/// sorted data structures (memtable skip list, SST block index, merge heap).
/// Specifically:
///
/// - **Totality**: for all `a`, `b`, exactly one of `Less`, `Equal`, `Greater` holds
/// - **Transitivity**: `a < b` and `b < c` implies `a < c`
/// - **Antisymmetry**: `compare(a, b) == Less` iff `compare(b, a) == Greater`
/// - **Reflexivity**: `compare(a, a) == Equal`
///
/// - **Bytewise equality**: `compare(a, b) == Equal` **must** imply `a == b`
///   byte-for-byte. Bloom filters and hash indexes operate on raw bytes;
///   if two byte-different keys compare as equal, hash-based lookups will
///   produce false negatives.
///
/// Violating these invariants corrupts the sort order and produces incorrect
/// query results.
///
/// # Important
///
/// Once a tree is created with a comparator, it must always be opened with the
/// same comparator. Using a different comparator on an existing tree will produce
/// incorrect results.
///
/// # Examples
///
/// ```
/// use lsm_tree::UserComparator;
/// use std::cmp::Ordering;
///
/// /// Comparator that orders u64 keys stored as big-endian bytes.
/// struct U64Comparator;
///
/// impl UserComparator for U64Comparator {
///     fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
///         if a.len() == 8 && b.len() == 8 {
///             // Length checked, conversion cannot fail.
///             let a_u64 = u64::from_be_bytes(a.try_into().unwrap());
///             let b_u64 = u64::from_be_bytes(b.try_into().unwrap());
///             a_u64.cmp(&b_u64)
///         } else {
///             // Non-8-byte keys: fall back to lexicographic ordering
///             // to preserve the bytewise-equality invariant.
///             a.cmp(b)
///         }
///     }
/// }
/// ```
pub trait UserComparator: Send + Sync + std::panic::RefUnwindSafe + 'static {
    /// Compares two user keys, returning their ordering.
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering;

    /// Returns `true` if this comparator is lexicographic byte ordering.
    ///
    /// When `true`, internal optimizations can avoid allocations in
    /// prefix-compressed block comparisons. Override only if your
    /// comparator is truly equivalent to `a.cmp(b)` on raw bytes.
    fn is_lexicographic(&self) -> bool {
        false
    }
}

/// Default comparator using lexicographic byte ordering.
///
/// This is the comparator used when no custom comparator is configured,
/// preserving backward compatibility with existing trees.
#[derive(Clone, Debug)]
pub struct DefaultUserComparator;

impl UserComparator for DefaultUserComparator {
    #[inline]
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    #[inline]
    fn is_lexicographic(&self) -> bool {
        true
    }
}

/// Shared reference to a [`UserComparator`].
pub type SharedComparator = Arc<dyn UserComparator>;

/// Returns the default comparator (lexicographic byte ordering).
///
/// Uses a shared static instance to avoid repeated allocations.
#[must_use]
pub fn default_comparator() -> SharedComparator {
    // LazyLock creates the Arc once; subsequent calls just clone the Arc (ref-count bump).
    static DEFAULT: std::sync::LazyLock<SharedComparator> =
        std::sync::LazyLock::new(|| Arc::new(DefaultUserComparator));
    DEFAULT.clone()
}
