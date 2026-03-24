// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Extracts prefixes from keys for prefix bloom filter indexing.
///
/// When a `PrefixExtractor` is configured on a tree, the bloom filter indexes
/// not only full keys but also the prefixes returned by [`PrefixExtractor::prefixes`].
/// This allows prefix scans to skip entire segments that contain no keys with a
/// matching prefix, dramatically reducing I/O for prefix-heavy workloads (e.g.,
/// graph adjacency lists, time-series buckets).
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync + UnwindSafe + RefUnwindSafe`.
/// The extractor is shared across flush, compaction, and read threads via `Arc`,
/// and may be accessed across panic boundaries (e.g., `catch_unwind` in tests).
///
/// # Example
///
/// ```
/// use lsm_tree::PrefixExtractor;
///
/// /// Extracts prefixes at each ':' separator boundary.
/// ///
/// /// For key `adj:out:42:KNOWS`, yields:
/// ///   `adj:`, `adj:out:`, `adj:out:42:`
/// struct ColonSeparatedPrefix;
///
/// impl PrefixExtractor for ColonSeparatedPrefix {
///     fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
///         Box::new(
///             key.iter()
///                 .enumerate()
///                 .filter(|(_, b)| **b == b':')
///                 .map(move |(i, _)| &key[..=i]),
///         )
///     }
/// }
/// ```
pub trait PrefixExtractor:
    Send + Sync + std::panic::UnwindSafe + std::panic::RefUnwindSafe
{
    /// Returns an iterator of prefixes to index for the given key.
    ///
    /// Each yielded prefix will be hashed and inserted into the segment's
    /// bloom filter. During a prefix scan, the scan prefix is hashed and
    /// checked against the bloom — segments without a match are skipped.
    ///
    /// Implementations should return prefixes from shortest to longest.
    /// The full key itself is always indexed separately by the standard bloom
    /// path; including it in the returned prefixes is allowed but redundant
    /// and generally unnecessary.
    ///
    /// The returned iterator must be finite and yield a small number of
    /// prefixes per key (typically 1–5). It is called on the write path
    /// for every key during flush and compaction.
    ///
    /// # Performance note
    ///
    /// Returns `Box<dyn Iterator>` for object safety (`Arc<dyn PrefixExtractor>`).
    /// Most extractors yield 1–5 prefixes per key, so the allocation is negligible
    /// compared to the bloom hash + I/O cost. A callback-based `for_each_prefix`
    /// alternative could avoid this allocation but would expand the trait API
    /// surface; consider adding it if profiling shows measurable overhead.
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a>;

    // NOTE: Renamed from `is_valid_prefix_boundary` (added in PR #43, never
    // released). No deprecated shim needed — no downstream consumers exist.

    /// Returns `true` if `prefix` is a valid scan boundary for this extractor.
    ///
    /// A scan boundary is valid when **every key** that the tree would consider
    /// a match for this prefix in a prefix scan had `prefix` indexed via
    /// [`prefixes`](Self::prefixes) at write time. This is the contract that
    /// makes bloom-based table skipping safe: if the bloom filter says "no
    /// match", we can skip the table because every matching key would have
    /// produced the prefix hash during flush/compaction.
    ///
    /// # Default implementation
    ///
    /// Checks whether `prefixes(prefix)` emits `prefix` itself — i.e.,
    /// whether the extractor considers this byte sequence a boundary.
    /// This is correct for well-behaved extractors whose `prefixes()` returns
    /// sub-slices of the input key.
    ///
    /// # When to override
    ///
    /// Override this method when the default self-referential check is either:
    /// - **Too expensive** — e.g., the extractor can check a sentinel byte in
    ///   O(1) instead of iterating all prefixes.
    /// - **Incorrect** — e.g., the extractor produces prefixes that are *not*
    ///   sub-slices of the input, so the default `any(|p| p == prefix)` check
    ///   would never match even for valid boundaries.
    fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
        !prefix.is_empty() && self.prefixes(prefix).any(|p| p == prefix)
    }
}

/// Computes the prefix hash for bloom-filter-based table skipping.
///
/// Returns `Some(hash)` only when the scan prefix is non-empty and is a valid
/// boundary for the configured extractor. Returns `None` otherwise (no bloom
/// skip will be attempted).
///
/// Used by both `Tree::create_prefix` and `BlobTree::prefix` to avoid
/// duplicating the boundary-check + hashing logic.
pub fn compute_prefix_hash(
    extractor: Option<&std::sync::Arc<dyn PrefixExtractor>>,
    prefix_bytes: &[u8],
) -> Option<u64> {
    use crate::table::filter::standard_bloom::Builder;

    if prefix_bytes.is_empty() {
        return None;
    }

    extractor
        .filter(|e| e.is_valid_scan_boundary(prefix_bytes))
        .map(|_| Builder::get_hash(prefix_bytes))
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    // Shadows std's #[test] with test_log's version for structured logging.
    // This IS used — #[test] on each function below resolves to this import.
    use test_log::test;

    struct ColonSeparatedPrefix;

    impl PrefixExtractor for ColonSeparatedPrefix {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            Box::new(
                key.iter()
                    .enumerate()
                    .filter(|(_, b)| **b == b':')
                    .map(move |(i, _)| &key[..=i]),
            )
        }
    }

    #[test]
    fn colon_separated_prefixes() {
        let extractor = ColonSeparatedPrefix;
        let key = b"adj:out:42:KNOWS";
        let prefixes: Vec<&[u8]> = extractor.prefixes(key).collect();
        assert_eq!(
            prefixes,
            vec![
                b"adj:" as &[u8],
                b"adj:out:" as &[u8],
                b"adj:out:42:" as &[u8],
            ]
        );
    }

    #[test]
    fn no_separator() {
        let extractor = ColonSeparatedPrefix;
        let key = b"noseparator";
        let prefixes: Vec<&[u8]> = extractor.prefixes(key).collect();
        assert!(prefixes.is_empty());
    }

    #[test]
    fn single_separator_at_end() {
        let extractor = ColonSeparatedPrefix;
        let key = b"prefix:";
        let prefixes: Vec<&[u8]> = extractor.prefixes(key).collect();
        assert_eq!(prefixes, vec![b"prefix:" as &[u8]]);
    }

    #[test]
    fn empty_key() {
        let extractor = ColonSeparatedPrefix;
        let prefixes: Vec<&[u8]> = extractor.prefixes(b"").collect();
        assert!(prefixes.is_empty());
    }

    #[test]
    fn is_valid_scan_boundary_colon_terminated() {
        let extractor = ColonSeparatedPrefix;
        // "adj:" is a valid boundary — extractor emits it for "adj:" input
        assert!(extractor.is_valid_scan_boundary(b"adj:"));
        assert!(extractor.is_valid_scan_boundary(b"adj:out:"));
        assert!(extractor.is_valid_scan_boundary(b"adj:out:42:"));
    }

    #[test]
    fn is_valid_scan_boundary_non_boundary() {
        let extractor = ColonSeparatedPrefix;
        // "adj" (no trailing colon) is NOT a valid boundary
        assert!(!extractor.is_valid_scan_boundary(b"adj"));
        assert!(!extractor.is_valid_scan_boundary(b"adj:out"));
        assert!(!extractor.is_valid_scan_boundary(b"noseparator"));
    }

    #[test]
    fn is_valid_scan_boundary_empty() {
        let extractor = ColonSeparatedPrefix;
        assert!(!extractor.is_valid_scan_boundary(b""));
    }

    /// Extractor that overrides `is_valid_scan_boundary` with an O(1) length
    /// check instead of iterating all prefixes via the default implementation.
    struct FixedLengthPrefix;

    impl PrefixExtractor for FixedLengthPrefix {
        fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
            if let Some(prefix) = key.get(..4) {
                Box::new(std::iter::once(prefix))
            } else {
                Box::new(std::iter::empty())
            }
        }

        fn is_valid_scan_boundary(&self, prefix: &[u8]) -> bool {
            prefix.len() == 4
        }
    }

    #[test]
    fn fixed_length_prefixes() {
        let extractor = FixedLengthPrefix;
        // Key longer than 4 bytes yields a single 4-byte prefix
        let prefixes: Vec<&[u8]> = extractor.prefixes(b"usr:data").collect();
        assert_eq!(prefixes, vec![b"usr:" as &[u8]]);

        // Key shorter than 4 bytes yields nothing
        let prefixes: Vec<&[u8]> = extractor.prefixes(b"ab").collect();
        assert!(prefixes.is_empty());

        // Key exactly 4 bytes yields itself
        let prefixes: Vec<&[u8]> = extractor.prefixes(b"abcd").collect();
        assert_eq!(prefixes, vec![b"abcd" as &[u8]]);
    }

    #[test]
    fn custom_scan_boundary_valid() {
        let extractor = FixedLengthPrefix;
        assert!(extractor.is_valid_scan_boundary(b"usr:"));
        assert!(extractor.is_valid_scan_boundary(b"abcd"));
    }

    #[test]
    fn custom_scan_boundary_invalid() {
        let extractor = FixedLengthPrefix;
        assert!(!extractor.is_valid_scan_boundary(b"ab"));
        assert!(!extractor.is_valid_scan_boundary(b"toolong"));
        assert!(!extractor.is_valid_scan_boundary(b""));
    }
}
