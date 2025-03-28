// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::either::Either::{self, Left, Right};
use crate::segment::block::offset::BlockOffset;
use crate::segment::id::GlobalSegmentId;
use crate::segment::{block_index::IndexBlock, value_block::ValueBlock};
use quick_cache::Weighter;
use quick_cache::{sync::Cache, Equivalent};
use std::sync::Arc;

type Item = Either<Arc<ValueBlock>, Arc<IndexBlock>>;

#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey(GlobalSegmentId, BlockOffset);

impl Equivalent<CacheKey> for (GlobalSegmentId, BlockOffset) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.0 == key.0 && self.1 == key.1
    }
}

impl From<(GlobalSegmentId, BlockOffset)> for CacheKey {
    fn from((gid, bid): (GlobalSegmentId, BlockOffset)) -> Self {
        Self(gid, bid)
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, block: &Item) -> u64 {
        #[allow(clippy::cast_possible_truncation)]
        match block {
            Either::Left(block) => block.header.uncompressed_length.into(),
            Either::Right(block) => block.header.uncompressed_length.into(),
        }
    }
}

/// Block cache, in which blocks are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive queries to nearby data, improving
/// read performance for hot data.
///
/// # Examples
///
/// Sharing block cache between multiple trees
///
/// ```
/// # use lsm_tree::{Tree, Config, BlockCache};
/// # use std::sync::Arc;
/// #
/// // Provide 40 MB of cache capacity
/// let block_cache = Arc::new(BlockCache::with_capacity_bytes(40 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub struct BlockCache {
    // NOTE: rustc_hash performed best: https://fjall-rs.github.io/post/fjall-2-1
    /// Concurrent cache implementation
    data: Cache<CacheKey, Item, BlockWeighter, rustc_hash::FxBuildHasher>,

    /// Capacity in bytes
    capacity: u64,
}

impl BlockCache {
    /// Creates a new block cache with roughly `n` bytes of capacity.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        use quick_cache::sync::DefaultLifecycle;

        #[allow(clippy::default_trait_access)]
        let quick_cache = Cache::with(
            1_000_000,
            bytes,
            BlockWeighter,
            Default::default(),
            DefaultLifecycle::default(),
        );

        Self {
            data: quick_cache,
            capacity: bytes,
        }
    }

    /// Returns the amount of cached bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.data.weight()
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the number of cached blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no cached blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[doc(hidden)]
    pub fn insert_disk_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: BlockOffset,
        value: Arc<ValueBlock>,
    ) {
        if self.capacity > 0 {
            self.data.insert((segment_id, offset).into(), Left(value));
        }
    }

    #[doc(hidden)]
    pub fn insert_index_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: BlockOffset,
        value: Arc<IndexBlock>,
    ) {
        if self.capacity > 0 {
            self.data.insert((segment_id, offset).into(), Right(value));
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_disk_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: BlockOffset,
    ) -> Option<Arc<ValueBlock>> {
        let key = (segment_id, offset);
        let item = self.data.get(&key)?;
        Some(item.left())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_index_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: BlockOffset,
    ) -> Option<Arc<IndexBlock>> {
        let key = (segment_id, offset);
        let item = self.data.get(&key)?;
        Some(item.right())
    }
}
