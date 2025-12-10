// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, GlobalTableId};
use crate::{
    table::block::BlockType, version::run::Ranged, Cache, CompressionType, DescriptorTable,
    KeyRange, Table,
};
use std::{path::Path, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

#[must_use]
pub fn aggregate_run_key_range(tables: &[Table]) -> KeyRange {
    #[expect(clippy::expect_used, reason = "run is expected to not be empty")]
    let lo = tables.first().expect("run should never be empty");
    #[expect(clippy::expect_used, reason = "run is expected to not be empty")]
    let hi = tables.last().expect("run should never be empty");
    KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct SliceIndexes(pub usize, pub usize);

/// Loads a block from disk or block cache, if cached.
///
/// Also handles file descriptor opening and caching.
#[warn(clippy::too_many_arguments)]
pub fn load_block(
    table_id: GlobalTableId,
    path: &Path,
    descriptor_table: &DescriptorTable,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
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
            BlockType::Data | BlockType::Meta => {
                metrics.data_block_load_cached.fetch_add(1, Relaxed);
            }
            _ => {}
        }

        return Ok(block);
    }

    let cached_fd = descriptor_table.access_for_table(&table_id);
    let fd_cache_miss = cached_fd.is_none();

    let fd = if let Some(fd) = cached_fd {
        #[cfg(feature = "metrics")]
        metrics.table_file_opened_cached.fetch_add(1, Relaxed);

        fd
    } else {
        let fd = std::fs::File::open(path)?;

        #[cfg(feature = "metrics")]
        metrics.table_file_opened_uncached.fetch_add(1, Relaxed);

        Arc::new(fd)
    };

    let block = Block::from_file(&fd, *handle, compression)?;

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
        BlockType::Data | BlockType::Meta => {
            metrics.data_block_load_io.fetch_add(1, Relaxed);

            metrics
                .data_block_io_requested
                .fetch_add(handle.size().into(), Relaxed);
        }
        _ => {}
    }

    // Cache FD
    if fd_cache_miss {
        descriptor_table.insert_for_table(table_id, fd);
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

// TODO: Fuzz test
#[must_use]
pub fn compare_prefixed_slice(prefix: &[u8], suffix: &[u8], needle: &[u8]) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Equal, Greater};

    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();
        return if combined_len > 0 { Greater } else { Equal };
    }

    let max_pfx_len = prefix.len().min(needle.len());

    {
        #[expect(unsafe_code, reason = "We checked for max_pfx_len")]
        let prefix = unsafe { prefix.get_unchecked(0..max_pfx_len) };

        #[expect(unsafe_code, reason = "We checked for max_pfx_len")]
        let needle = unsafe { needle.get_unchecked(0..max_pfx_len) };

        match prefix.cmp(needle) {
            Equal => {}
            ordering => return ordering,
        }
    }

    let rest_len = prefix.len().saturating_sub(needle.len());
    if rest_len > 0 {
        return Greater;
    }

    #[expect(
        unsafe_code,
        reason = "We know that the prefix is definitely not longer than the needle so we can safely truncate"
    )]
    let needle = unsafe { needle.get_unchecked(max_pfx_len..) };
    suffix.cmp(needle)
}

#[cfg(test)]
mod tests {
    use super::*;
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

        assert_eq!(Greater, compare_prefixed_slice(&[0, 161], &[], &[0]));

        assert_eq!(Equal, compare_prefixed_slice(b"abc", b"xyz", b"abcxyz"));
        assert_eq!(Equal, compare_prefixed_slice(b"abc", b"", b"abc"));
        assert_eq!(Equal, compare_prefixed_slice(b"abc", b"abc", b"abcabc"));
        assert_eq!(Equal, compare_prefixed_slice(b"", b"", b""));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"y"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"yyyy", b"a", b"yyyyb"));
        assert_eq!(Less, compare_prefixed_slice(b"yyy", b"b", b"yyyyb"));
        assert_eq!(Less, compare_prefixed_slice(b"abc", b"d", b"abce"));
        assert_eq!(Less, compare_prefixed_slice(b"ab", b"", b"ac"));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"b", b"a", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"b", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"abc", b"xy", b"abcw"));
        assert_eq!(Greater, compare_prefixed_slice(b"ab", b"cde", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"abcd", b"zz", b"abc"));
        assert_eq!(Greater, compare_prefixed_slice(b"abc", b"d", b"abc"));
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"aaaa", b"aaab", b"aaaaaaaa")
        );
        assert_eq!(
            Greater,
            compare_prefixed_slice(b"aaaa", b"aaba", b"aaaaaaaa")
        );
        assert_eq!(Greater, compare_prefixed_slice(b"abcd", b"x", b"abc"));

        assert_eq!(Less, compare_prefixed_slice(&[0x7F], &[], &[0x80]));
        assert_eq!(Greater, compare_prefixed_slice(&[0xFF], &[], &[0x10]));
    }
}
