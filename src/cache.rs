// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::super_segment::block::Header;
use crate::super_segment::{Block, BlockOffset, DataBlock};
use crate::GlobalSegmentId;
use quick_cache::Weighter;
use quick_cache::{sync::Cache as QuickCache, Equivalent};

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;

/* #[derive(Clone)]
enum Item {
    DataBlock(Arc<ValueBlock>),
    IndexBlock(Arc<IndexBlock>),
    Blob(UserValue),
} */

type Item = Block;

#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey(u8, u64, u64, u64);

impl Equivalent<CacheKey> for (u8, u64, u64, u64) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.0 == key.0 && self.1 == key.1 && self.2 == key.2 && self.3 == key.3
    }
}

impl From<(u8, u64, u64, u64)> for CacheKey {
    fn from((tag, root_id, segment_id, offset): (u8, u64, u64, u64)) -> Self {
        Self(tag, root_id, segment_id, offset)
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, block: &Item) -> u64 {
        (Header::serialized_len() as u64) + Into::<u64>::into(block.header.uncompressed_length)
    }
}

/// Cache, in which blocks or blobs are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive queries to nearby data, improving
/// read performance for hot data.
///
/// # Examples
///
/// Sharing cache between multiple trees
///
/// ```
/// # use lsm_tree::{Tree, Config, Cache};
/// # use std::sync::Arc;
/// #
/// // Provide 40 MB of cache capacity
/// let cache = Arc::new(Cache::with_capacity_bytes(40 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder).use_cache(cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder).use_cache(cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub struct Cache {
    // NOTE: rustc_hash performed best: https://fjall-rs.github.io/post/fjall-2-1
    /// Concurrent cache implementation
    data: QuickCache<CacheKey, Item, BlockWeighter, rustc_hash::FxBuildHasher>,

    /// Capacity in bytes
    capacity: u64,
}

impl Cache {
    /// Creates a new block cache with roughly `n` bytes of capacity.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        use quick_cache::sync::DefaultLifecycle;

        #[allow(clippy::default_trait_access)]
        let quick_cache = QuickCache::with(
            100_000,
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
    #[must_use]
    pub fn get_data_block(&self, id: GlobalSegmentId, offset: BlockOffset) -> Option<DataBlock> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.segment_id(), *offset).into();
        self.data.get(&key).map(DataBlock::new)
    }

    #[doc(hidden)]
    pub fn insert_block(&self, id: GlobalSegmentId, offset: BlockOffset, value: Item) {
        self.data.insert(
            (TAG_BLOCK, id.tree_id(), id.segment_id(), *offset).into(),
            value,
        );
    }

    /* #[doc(hidden)]
    pub fn insert_index_block(
        &self,
        id: GlobalSegmentId,
        offset: BlockOffset,
        value: Arc<IndexBlock>,
    ) {
            self.data.insert(
                (TAG_BLOCK, id.tree_id(), id.segment_id(), *offset).into(),
                Item::IndexBlock(value),
            );
    } */

    /*  #[doc(hidden)]
    #[must_use]
    pub fn get_index_block(
        &self,
        id: GlobalSegmentId,
        offset: BlockOffset,
    ) -> Option<Arc<IndexBlock>> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.segment_id(), *offset).into();

        if let Item::IndexBlock(block) = self.data.get(&key)? {
            Some(block)
        } else {
            log::warn!("cache item type was unexpected - this is a bug");
            None
        }
    } */

    /*  #[doc(hidden)]
    pub fn insert_blob(
        &self,
        vlog_id: value_log::ValueLogId,
        vhandle: &value_log::ValueHandle,
        value: UserValue,
    ) {
            self.data.insert(
                (TAG_BLOB, vlog_id, vhandle.segment_id, vhandle.offset).into(),
                Item::Blob(value),
            );
    } */

    /*  #[doc(hidden)]
    #[must_use]
    pub fn get_blob(
        &self,
        vlog_id: value_log::ValueLogId,
        vhandle: &value_log::ValueHandle,
    ) -> Option<UserValue> {
        let key: CacheKey = (TAG_BLOB, vlog_id, vhandle.segment_id, vhandle.offset).into();

        if let Item::Blob(blob) = self.data.get(&key)? {
            Some(blob)
        } else {
            log::warn!("cache item type was unexpected - this is a bug");
            None
        }
    } */
}
