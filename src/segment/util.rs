// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use super::{Block, BlockHandle, GlobalSegmentId};
use crate::{segment::block::BlockType, Cache, CompressionType, DescriptorTable};
use std::{path::Path, sync::Arc};

/// [start, end] slice indexes
#[derive(Debug)]
pub struct SliceIndexes(pub usize, pub usize);

/// Loads a block from disk or block cache, if cached.
///
/// Also handles file descriptor opening and caching.
pub fn load_block(
    segment_id: GlobalSegmentId,
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

    log::trace!("load block {handle:?}");

    if let Some(block) = cache.get_block(segment_id, handle.offset()) {
        #[cfg(feature = "metrics")]
        metrics.block_load_cached.fetch_add(1, Relaxed);

        return Ok(block);
    }

    let cached_fd = descriptor_table.access_for_table(&segment_id);
    let fd_cache_miss = cached_fd.is_none();

    let fd = if let Some(fd) = cached_fd {
        fd
    } else {
        Arc::new(std::fs::File::open(path)?)
    };

    let block = Block::from_file(&fd, *handle, compression)?;

    if block.header.block_type != block_type {
        return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        ))));
    }

    #[cfg(feature = "metrics")]
    metrics.block_load_io.fetch_add(1, Relaxed);

    // Cache FD
    if fd_cache_miss {
        descriptor_table.insert_for_table(segment_id, fd);
    }

    cache.insert_block(segment_id, handle.offset(), block.clone());

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
        // SAFETY: We checked for max_pfx_len
        #[allow(unsafe_code)]
        let prefix = unsafe { prefix.get_unchecked(0..max_pfx_len) };

        // SAFETY: We checked for max_pfx_len
        #[allow(unsafe_code)]
        let needle = unsafe { needle.get_unchecked(0..max_pfx_len) };

        match prefix.cmp(needle) {
            Equal => {}
            ordering => return ordering,
        }
    }

    let rest_len = needle.len() - max_pfx_len;
    if rest_len == 0 {
        if !suffix.is_empty() {
            return std::cmp::Ordering::Greater;
        }
        return std::cmp::Ordering::Equal;
    }

    // SAFETY: We know that the prefix is definitely not longer than the needle
    // so we can safely truncate
    #[allow(unsafe_code)]
    let needle = unsafe { needle.get_unchecked(max_pfx_len..) };
    suffix.cmp(needle)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn v3_compare_prefixed_slice() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        assert_eq!(Equal, compare_prefixed_slice(b"", b"", b""));

        assert_eq!(Greater, compare_prefixed_slice(b"a", b"", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"b", b"a", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"b", b"a"));

        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"y"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"yyyy", b"a", b"yyyyb"));
        assert_eq!(Less, compare_prefixed_slice(b"yyy", b"b", b"yyyyb"));
    }
}
