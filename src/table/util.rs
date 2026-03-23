// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, GlobalTableId};
use crate::{
    encryption::EncryptionProvider, file_accessor::FileAccessor, table::block::BlockType,
    version::run::Ranged, Cache, CompressionType, KeyRange, Table,
};
use std::{path::Path, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

#[must_use]
pub fn aggregate_run_key_range(tables: &[Table]) -> KeyRange {
    #[expect(clippy::expect_used, reason = "runs are never empty by definition")]
    let lo = tables.first().expect("run should never be empty");
    #[expect(clippy::expect_used, reason = "runs are never empty by definition")]
    let hi = tables.last().expect("run should never be empty");
    KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct SliceIndexes(pub usize, pub usize);

/// Loads a block from disk or block cache, if cached.
///
/// Also handles file descriptor opening and caching.
#[expect(
    clippy::too_many_arguments,
    reason = "block loading requires table id, path, file accessor, cache, handle, block type, compression, and encryption context"
)]
pub fn load_block(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
) -> crate::Result<Block> {
    #[cfg(feature = "metrics")]
    use std::sync::atomic::Ordering::Relaxed;

    log::trace!("load {block_type:?} block {handle:?}");

    if let Some(block) = cache.get_block(table_id, handle.offset()) {
        #[cfg(feature = "metrics")]
        match block_type {
            BlockType::Filter => {
                metrics.filter_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::Index => {
                metrics.index_block_load_cached.fetch_add(1, Relaxed);
            }
            BlockType::RangeTombstone => {
                metrics
                    .range_tombstone_block_load_cached
                    .fetch_add(1, Relaxed);
            }
            BlockType::Data | BlockType::Meta => {
                metrics.data_block_load_cached.fetch_add(1, Relaxed);
            }
        }

        return Ok(block);
    }

    let (fd, fd_cache_miss) = if let Some(cached_fd) = file_accessor.access_for_table(&table_id) {
        #[cfg(feature = "metrics")]
        metrics.table_file_opened_cached.fetch_add(1, Relaxed);

        (cached_fd, false)
    } else {
        let fd = std::fs::File::open(path)?;

        #[cfg(feature = "metrics")]
        metrics.table_file_opened_uncached.fetch_add(1, Relaxed);

        (Arc::new(fd), true)
    };

    let block = Block::from_file(&*fd, *handle, compression, encryption)?;

    if block.header.block_type != block_type {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    #[cfg(feature = "metrics")]
    match block_type {
        BlockType::Filter => {
            metrics.filter_block_load_io.fetch_add(1, Relaxed);

            metrics
                .filter_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        BlockType::Index => {
            metrics.index_block_load_io.fetch_add(1, Relaxed);

            metrics
                .index_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        BlockType::RangeTombstone => {
            metrics.range_tombstone_block_load_io.fetch_add(1, Relaxed);

            metrics
                .range_tombstone_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        BlockType::Data | BlockType::Meta => {
            metrics.data_block_load_io.fetch_add(1, Relaxed);

            metrics
                .data_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
    }

    // Cache FD
    if fd_cache_miss {
        file_accessor.insert_for_table(table_id, fd);
    }

    cache.insert_block(table_id, handle.offset(), block.clone());

    Ok(block)
}

#[must_use]
pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|(c1, c2)| c1 == c2)
        .count()
}

/// Compares the conceptual concatenation `prefix + suffix` against `needle`
/// using the given comparator.
///
/// For the default lexicographic comparator this performs a zero-allocation
/// bytewise comparison. Custom comparators fall back to concatenating prefix
/// and suffix into a temporary `Vec` so that `UserComparator::compare` always
/// receives a complete key.
#[must_use]
pub fn compare_prefixed_slice(
    prefix: &[u8],
    suffix: &[u8],
    needle: &[u8],
    cmp: &dyn crate::comparator::UserComparator,
) -> std::cmp::Ordering {
    // Fast path: zero-allocation bytewise comparison for the default
    // (lexicographic) comparator. This is the hot path for block index
    // and data block binary searches.
    if cmp.is_lexicographic() {
        return compare_prefixed_slice_lexicographic(prefix, suffix, needle);
    }

    // Slow path: materialize prefix+suffix into a contiguous buffer for
    // custom comparators. Uses a stack buffer for typical key sizes to
    // avoid heap allocation on the hot binary-search path.
    let total_len = prefix.len() + suffix.len();

    if total_len <= 256 {
        let mut buf = [0_u8; 256];

        // SAFETY (indexing): total_len <= 256 == buf.len(), and
        // prefix.len() + suffix.len() == total_len, so all slices are in bounds.
        #[expect(clippy::indexing_slicing, reason = "total_len <= 256 checked above")]
        {
            buf[..prefix.len()].copy_from_slice(prefix);
            buf[prefix.len()..total_len].copy_from_slice(suffix);
        }

        #[expect(clippy::indexing_slicing, reason = "total_len <= 256 checked above")]
        return cmp.compare(&buf[..total_len], needle);
    }

    // Fallback for unusually large keys: allocate a temporary Vec.
    let mut full_key = Vec::with_capacity(total_len);
    full_key.extend_from_slice(prefix);
    full_key.extend_from_slice(suffix);
    cmp.compare(&full_key, needle)
}

/// Zero-allocation lexicographic comparison of `prefix + suffix` against `needle`.
#[must_use]
fn compare_prefixed_slice_lexicographic(
    prefix: &[u8],
    suffix: &[u8],
    needle: &[u8],
) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Equal, Greater};

    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();
        return if combined_len > 0 { Greater } else { Equal };
    }

    let max_pfx_len = prefix.len().min(needle.len());

    {
        // SAFETY: max_pfx_len = min(prefix.len(), needle.len()), so both
        // slices [0..max_pfx_len] are within bounds by construction.
        #[expect(
            unsafe_code,
            reason = "max_pfx_len <= prefix.len() && max_pfx_len <= needle.len()"
        )]
        let pfx = unsafe { prefix.get_unchecked(0..max_pfx_len) };

        #[expect(
            unsafe_code,
            reason = "max_pfx_len <= prefix.len() && max_pfx_len <= needle.len()"
        )]
        let ndl = unsafe { needle.get_unchecked(0..max_pfx_len) };

        match pfx.cmp(ndl) {
            Equal => {}
            ordering => return ordering,
        }
    }

    let rest_len = prefix.len().saturating_sub(needle.len());
    if rest_len > 0 {
        return Greater;
    }

    // SAFETY: rest_len == 0 means prefix.len() <= needle.len(), so
    // max_pfx_len == prefix.len() <= needle.len() and needle[max_pfx_len..] is in-bounds.
    #[expect(
        unsafe_code,
        reason = "max_pfx_len <= needle.len() guaranteed by rest_len == 0 guard above"
    )]
    let remaining_needle = unsafe { needle.get_unchecked(max_pfx_len..) };
    suffix.cmp(remaining_needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::DefaultUserComparator;
    use test_log::test;

    #[test]
    fn test_longest_shared_prefix_length() {
        assert_eq!(3, longest_shared_prefix_length(b"abc", b"abc"));
        assert_eq!(1, longest_shared_prefix_length(b"abc", b"a"));
        assert_eq!(1, longest_shared_prefix_length(b"a", b"abc"));
        assert_eq!(0, longest_shared_prefix_length(b"abc", b""));
        assert_eq!(0, longest_shared_prefix_length(b"", b"abc"));
        assert_eq!(0, longest_shared_prefix_length(b"", b""));
        assert_eq!(0, longest_shared_prefix_length(b"", b""));
        assert_eq!(0, longest_shared_prefix_length(b"abc", b"def"));
        assert_eq!(1, longest_shared_prefix_length(b"abc", b"acc"));
    }

    #[test]
    fn test_compare_prefixed_slice() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        assert_eq!(
            Greater,
            compare_prefixed_slice(&[0, 161], &[], &[0], &DefaultUserComparator)
        );

        assert_eq!(
            Equal,
            compare_prefixed_slice(b"abc", b"xyz", b"abcxyz", &DefaultUserComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"abc", b"", b"abc", &DefaultUserComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"abc", b"abc", b"abcabc", &DefaultUserComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"", b"", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"a", b"", b"y", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"a", b"", b"yyy", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"a", b"", b"yyy", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"yyyy", b"a", b"yyyyb", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"yyy", b"b", b"yyyyb", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"abc", b"d", b"abce", &DefaultUserComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"ab", b"", b"ac", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"a", b"", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"", b"a", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"a", b"a", b"", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"b", b"a", b"a", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"a", b"b", b"a", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abc", b"xy", b"abcw", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"ab", b"cde", b"a", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abcd", b"zz", b"abc", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abc", b"d", b"abc", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"aaaa", b"aaab", b"aaaaaaaa", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"aaaa", b"aaba", b"aaaaaaaa", &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"abcd", b"x", b"abc", &DefaultUserComparator)
        );

        assert_eq!(
            Less,
            compare_prefixed_slice(&[0x7F], &[], &[0x80], &DefaultUserComparator)
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(&[0xFF], &[], &[0x10], &DefaultUserComparator)
        );
    }

    /// Reverse comparator to exercise the Vec-allocation slow path.
    struct ReverseComparator;
    impl crate::comparator::UserComparator for ReverseComparator {
        fn name(&self) -> &'static str {
            "test-reverse"
        }

        fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            b.cmp(a)
        }
    }

    #[test]
    fn test_compare_prefixed_slice_custom_comparator() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        use crate::comparator::UserComparator as _;
        assert_eq!(ReverseComparator.name(), "test-reverse");

        // With reverse comparator, "abc" > "xyz" (reversed)
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"ab", b"c", b"xyz", &ReverseComparator)
        );
        assert_eq!(
            Less,
            compare_prefixed_slice(b"xy", b"z", b"abc", &ReverseComparator)
        );
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"ab", b"c", b"abc", &ReverseComparator)
        );
        // Empty cases
        assert_eq!(
            Equal,
            compare_prefixed_slice(b"", b"", b"", &ReverseComparator)
        );
        assert_eq!(
            Less, // reversed: non-empty > empty
            compare_prefixed_slice(b"a", b"", b"", &ReverseComparator)
        );
    }
}
