pub mod block_handle;
pub mod top_level;
pub mod writer;

use self::block_handle::KeyedBlockHandle;
use super::block::CachePolicy;
use super::id::GlobalSegmentId;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use crate::disk_block::DiskBlock;
use crate::file::{BLOCKS_FILE, TOP_LEVEL_INDEX_FILE};
use std::path::Path;
use std::sync::Arc;
use top_level::TopLevelIndex;

// TODO: rename index block?
pub type BlockHandleBlock = DiskBlock<KeyedBlockHandle>;

impl BlockHandleBlock {
    /*   pub(crate) fn get_previous_data_block_handle(&self, key: &[u8]) -> Option<&KeyedBlockHandle> {
        self.items.iter().rev().find(|x| &*x.start_key < key)
    }

    pub(crate) fn get_next_data_block_handle(&self, key: &[u8]) -> Option<&KeyedBlockHandle> {
        self.items.iter().find(|x| &*x.start_key > key)
    } */

    /// Finds the block that (possibly) contains a key
    pub fn get_lowest_data_block_containing_item(&self, key: &[u8]) -> Option<&KeyedBlockHandle> {
        self.items.iter().rev().find(|x| &*x.start_key <= key)
    }
}

/// Allows reading index blocks - just a wrapper around a block cache
#[allow(clippy::module_name_repetitions)]
pub struct IndexBlockFetcher(Arc<BlockCache>);

impl IndexBlockFetcher {
    pub fn insert(&self, segment_id: GlobalSegmentId, offset: u64, value: Arc<BlockHandleBlock>) {
        self.0.insert_index_block(segment_id, offset, value);
    }

    #[must_use]
    pub fn get(&self, segment_id: GlobalSegmentId, offset: u64) -> Option<Arc<BlockHandleBlock>> {
        self.0.get_index_block(segment_id, offset)
    }
}

// TODO: use BlockIndex as compound type for most stuff... less stuff to pass... less duplicate fields... just pass a BlockIndex to SegmentReader and that's it!
// no need for blocks anymore...?

/// Index that translates item keys to block handles.
///
/// The index is only partially loaded into memory.
///
/// See <https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html>
#[allow(clippy::module_name_repetitions)]
pub struct BlockIndex {
    descriptor_table: Arc<FileDescriptorTable>,

    /// Segment ID
    segment_id: GlobalSegmentId,

    /// Level-0 index. Is read-only and always fully loaded.
    ///
    /// This index points to index blocks inside the level-1 index.
    top_level_index: TopLevelIndex,

    // TODO: block_cache instead of "blocks" i guess
    /// Level-1 index. This index is only partially loaded into memory, decreasing memory usage, compared to a fully loaded one.
    ///
    /// However to find a disk block, one layer of indirection is required:
    ///
    /// To find a reference to a segment block, first the level-0 index needs to be checked,
    /// then the corresponding index block needs to be loaded, which contains the wanted disk block handle.
    blocks: IndexBlockFetcher,
}

impl BlockIndex {
    // Gets the next first block handle of an index block that is untouched by the given prefix
    pub fn get_prefix_upper_bound(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        let Some(block_handle) = self.top_level_index.get_prefix_upper_bound(key) else {
            return Ok(None);
        };

        let index_block = self.load_index_block(block_handle, cache_policy)?;
        Ok(index_block.items.first().cloned())
    }

    #[must_use]
    pub fn get_lowest_index_block_handle_containing_key(
        &self,
        key: &[u8],
    ) -> Option<&KeyedBlockHandle> {
        self.top_level_index.get_lowest_block_containing_key(key)
    }

    #[must_use]
    pub fn get_lowest_index_block_handle_not_containing_key(
        &self,
        key: &[u8],
    ) -> Option<&KeyedBlockHandle> {
        self.top_level_index
            .get_lowest_block_not_containing_key(key)
    }

    /// Gets the lowest block handle that may contain the given item
    pub fn get_lowest_data_block_handle_containing_item(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        let Some(block_handle) = self.get_lowest_index_block_handle_containing_key(key) else {
            return Ok(None);
        };

        let index_block = self.load_index_block(block_handle, cache_policy)?;
        Ok(index_block
            .get_lowest_data_block_containing_item(key)
            .cloned())
    }

    pub fn get_upper_bound_block_info(
        &self,
        key: &[u8],
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        todo!();
        /* let Some(first_block_handle) = self.top_level_index.get_lowest_block_containing_item(key)
        else {
            return Ok(None);
        };

        let index_block =
            self.load_index_block(first_block_handle, CachePolicy::Write /* TODO: */)?;

        let next_block = index_block.get_next_block_info(key);

        if let Some(block) = next_block {
            Ok(Some(block).cloned())
        } else {
            // The upper bound block is not in the same index block as the key, so load next index block
            let Some(next_block_handle) = self
                .top_level_index
                .get_next_block_handle(first_block_handle.offset)
            else {
                return Ok(None);
            };

            Ok(Some(next_block_handle.clone()))
        } */
    }

    /// Returns the previous index block's key, if it exists, or None
    pub fn get_previous_block_key(&self, key: &[u8]) -> crate::Result<Option<KeyedBlockHandle>> {
        todo!();

        /*   let Some(first_block_handle) = self.top_level_index.get_lowest_block_containing_item(key)
        else {
            return Ok(None);
        };

        let index_block =
            self.load_index_block(first_block_handle, CachePolicy::Write /* TODO: */)?;

        let maybe_prev = index_block.get_previous_block_info(key);

        if let Some(item) = maybe_prev {
            Ok(Some(item).cloned())
        } else {
            let Some(prev_block_handle) = self
                .top_level_index
                .get_previous_block_handle(first_block_handle.offset)
            else {
                return Ok(None);
            };

            let index_block =
                self.load_index_block(prev_block_handle, CachePolicy::Write /* TODO: */)?;

            Ok(index_block.items.last().cloned())
        } */
    }

    /// Returns the next index block's key, if it exists, or None
    #[must_use]
    pub fn get_next_index_block_handle(
        &self,
        block_handle: &KeyedBlockHandle,
    ) -> Option<&KeyedBlockHandle> {
        self.top_level_index
            .get_next_block_handle(block_handle.offset)
    }

    /// Returns the previous index block's key, if it exists, or None
    #[must_use]
    pub fn get_prev_index_block_handle(
        &self,
        block_handle: &KeyedBlockHandle,
    ) -> Option<&KeyedBlockHandle> {
        self.top_level_index
            .get_prev_block_handle(block_handle.offset)
    }

    /* let Some(first_block_handle) = self.top_level_index.get_lowest_block_containing_item(key)
    else {
        return Ok(None);
    };

    let index_block = self.load_index_block(first_block_handle, cache_policy)?;

    let maybe_next = index_block.get_next_block_info(key);

    if let Some(item) = maybe_next {
        Ok(Some(item).cloned())
    } else {
        let Some(next_block_handle) = self
            .top_level_index
            .get_next_block_handle(first_block_handle.offset)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(next_block_handle, cache_policy)?;

        Ok(index_block.items.first().cloned())
    } */

    #[must_use]
    pub fn get_first_index_block_handle(&self) -> &KeyedBlockHandle {
        self.top_level_index.get_first_block_handle()
    }

    /// Returns the last block handle
    #[must_use]
    pub fn get_last_block_handle(&self) -> &KeyedBlockHandle {
        self.top_level_index.get_last_block_handle()
    }

    /// Loads an index block from disk
    pub fn load_index_block(
        &self,
        block_handle: &KeyedBlockHandle,
        cache_policy: CachePolicy,
    ) -> crate::Result<Arc<DiskBlock<KeyedBlockHandle>>> {
        if let Some(block) = self.blocks.get(self.segment_id, block_handle.offset) {
            // Cache hit: Copy from block

            Ok(block)
        } else {
            // Cache miss: load from disk

            let file_guard = self
                .descriptor_table
                .access(&self.segment_id)?
                .expect("should acquire file handle");

            let block = BlockHandleBlock::from_file_compressed(
                &mut *file_guard.file.lock().expect("lock is poisoned"),
                block_handle.offset,
                block_handle.size,
            )?;

            drop(file_guard);

            let block = Arc::new(block);

            if cache_policy == CachePolicy::Write {
                self.blocks
                    .insert(self.segment_id, block_handle.offset, Arc::clone(&block));
            }

            Ok(block)
        }
    }

    /// Only used for tests
    #[allow(dead_code, clippy::expect_used)]
    #[doc(hidden)]
    pub(crate) fn new(segment_id: GlobalSegmentId, block_cache: Arc<BlockCache>) -> Self {
        let index_block_index = IndexBlockFetcher(block_cache);

        Self {
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            segment_id,
            blocks: index_block_index,
            top_level_index: TopLevelIndex::from_boxed_slice(Box::default()),
        }
    }

    /* pub fn preload(&self) -> crate::Result<()> {
        for (block_key, block_handle) in &self.top_level_index.data {
            // TODO: this function seeks every time
            // can and should probably be optimized
            self.load_and_cache_index_block(block_key, block_handle)?;
        }

        Ok(())
    } */

    pub fn from_file<P: AsRef<Path>>(
        segment_id: GlobalSegmentId,
        descriptor_table: Arc<FileDescriptorTable>,
        folder: P,
        block_cache: Arc<BlockCache>,
    ) -> crate::Result<Self> {
        let folder = folder.as_ref();

        log::trace!("Reading block index from {folder:?}");

        debug_assert!(folder.try_exists()?, "{folder:?} missing");
        debug_assert!(
            folder.join(TOP_LEVEL_INDEX_FILE).try_exists()?,
            "{folder:?} missing",
        );
        debug_assert!(folder.join(BLOCKS_FILE).try_exists()?, "{folder:?} missing");

        let tli_path = folder.join(TOP_LEVEL_INDEX_FILE);
        let top_level_index = TopLevelIndex::from_file(tli_path)?;

        Ok(Self {
            descriptor_table,
            segment_id,
            top_level_index,
            blocks: IndexBlockFetcher(block_cache),
        })
    }
}
