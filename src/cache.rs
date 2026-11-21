// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::table::block::Header;
use crate::table::{Block, BlockOffset};
use crate::{GlobalTableId, UserValue};
use quick_cache::sync::Cache as QuickCache;
use quick_cache::Weighter;

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;

#[derive(Clone)]
enum Item {
    Block(Block),
    Blob(UserValue),
}

#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey(u8, u64, u64, u64);

impl From<(u8, u64, u64, u64)> for CacheKey {
    fn from((tag, root_id, table_id, offset): (u8, u64, u64, u64)) -> Self {
        Self(tag, root_id, table_id, offset)
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    fn weight(&self, _: &CacheKey, item: &Item) -> u64 {
        use Item::{Blob, Block};

        match item {
            Block(b) => (Header::serialized_len() as u64) + u64::from(b.header.uncompressed_length),
            Blob(b) => b.len() as u64,
        }
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
/// // Provide 64 MB of cache capacity
/// let cache = Arc::new(Cache::with_capacity_bytes(64 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder, Default::default(), Default::default()).use_cache(cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder, Default::default(), Default::default()).use_cache(cache.clone()).open()?;
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

        #[expect(clippy::expect_used, reason = "nothing we can do if it fails")]
        let opts = quick_cache::OptionsBuilder::new()
            .weight_capacity(bytes)
            .hot_allocation(0.8)
            .estimated_items_capacity(10_000)
            .build()
            .expect("cache options should be valid");

        let quick_cache = QuickCache::with_options(
            opts,
            BlockWeighter,
            rustc_hash::FxBuildHasher,
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

    /// Returns a cache shard's capacity in bytes.
    pub(crate) fn shard_capacity(&self) -> u64 {
        self.data.shard_capacity()
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_block(&self, id: GlobalTableId, offset: BlockOffset) -> Option<Block> {
        let key: CacheKey = (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into();

        Some(match self.data.get(&key)? {
            Item::Block(block) => block,
            Item::Blob(_) => unreachable!("invalid cache item"),
        })
    }

    #[doc(hidden)]
    pub fn insert_block(&self, id: GlobalTableId, offset: BlockOffset, block: Block) {
        self.data.insert(
            (TAG_BLOCK, id.tree_id(), id.table_id(), *offset).into(),
            Item::Block(block),
        );
    }

    /// Evicts all cached blocks of a table.
    ///
    /// Used when a table is deleted, because its blocks can never be read again.
    ///
    /// This needs to visit every cache entry, because blocks are keyed by their offset.
    pub(crate) fn remove_table_blocks(&self, id: GlobalTableId) {
        let (tree_id, table_id) = (id.tree_id(), id.table_id());

        self.data.retain(|key, _| {
            let CacheKey(tag, key_tree_id, key_table_id, _) = key;
            !(*tag == TAG_BLOCK && *key_tree_id == tree_id && *key_table_id == table_id)
        });
    }

    #[doc(hidden)]
    pub fn insert_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
        value: UserValue,
    ) {
        self.data.insert(
            (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into(),
            Item::Blob(value),
        );
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_blob(
        &self,
        vlog_id: crate::TreeId,
        vhandle: &crate::vlog::ValueHandle,
    ) -> Option<UserValue> {
        let key: CacheKey = (TAG_BLOB, vlog_id, vhandle.blob_file_id, vhandle.offset).into();

        Some(match self.data.get(&key)? {
            Item::Blob(blob) => blob,
            Item::Block(_) => unreachable!("invalid cache item"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::block::{BlockType, Header};
    use crate::vlog::ValueHandle;
    use crate::Checksum;
    use test_log::test;

    fn block() -> Block {
        Block {
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 4,
                uncompressed_length: 4,
            },
            data: b"data".into(),
        }
    }

    #[test]
    fn remove_table_blocks_keeps_other_entries() {
        let cache = Cache::with_capacity_bytes(1_024 * 1_024);

        let target = GlobalTableId::from((1, 7));
        let same_tree_other_table = GlobalTableId::from((1, 8));
        let other_tree_same_table = GlobalTableId::from((2, 7));

        for offset in [0, 100] {
            cache.insert_block(target, BlockOffset(offset), block());
        }
        cache.insert_block(same_tree_other_table, BlockOffset(0), block());
        cache.insert_block(other_tree_same_table, BlockOffset(0), block());

        // A blob whose (vlog id, blob file id, offset) equals the target block's key apart from the tag
        let vhandle = ValueHandle {
            blob_file_id: 7,
            offset: 0,
            on_disk_size: 4,
        };
        cache.insert_blob(1, &vhandle, b"blob".into());

        cache.remove_table_blocks(target);

        for offset in [0, 100] {
            assert!(cache.get_block(target, BlockOffset(offset)).is_none());
        }
        assert!(cache
            .get_block(same_tree_other_table, BlockOffset(0))
            .is_some());
        assert!(cache
            .get_block(other_tree_same_table, BlockOffset(0))
            .is_some());
        assert!(cache.get_blob(1, &vhandle).is_some());
    }
}
