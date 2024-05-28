pub mod block_handle;
pub mod top_level;
pub mod writer;

use self::block_handle::KeyedBlockHandle;
use super::block::Block;
use super::id::GlobalSegmentId;
use super::value_block::CachePolicy;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use std::path::Path;
use std::sync::Arc;
use top_level::TopLevelIndex;

pub type IndexBlock = Block<KeyedBlockHandle>;

impl IndexBlock {
    // TODO: same as TLI::get_lowest_block_containing_key

    /// Finds the block that (possibly) contains a key
    #[must_use]
    pub fn get_lowest_data_block_handle_containing_item(
        &self,
        key: &[u8],
    ) -> Option<&KeyedBlockHandle> {
        let idx = self.items.partition_point(|x| &*x.end_key < key);

        let handle = self.items.get(idx)?;

        if key > &*handle.end_key {
            None
        } else {
            Some(handle)
        }
    }
}

/// Allows reading index blocks - just a wrapper around a block cache
#[allow(clippy::module_name_repetitions)]
pub struct IndexBlockFetcher(Arc<BlockCache>);

impl IndexBlockFetcher {
    pub fn insert(&self, segment_id: GlobalSegmentId, offset: u64, value: Arc<IndexBlock>) {
        self.0.insert_index_block(segment_id, offset, value);
    }

    #[must_use]
    pub fn get(&self, segment_id: GlobalSegmentId, offset: u64) -> Option<Arc<IndexBlock>> {
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
    pub(crate) top_level_index: TopLevelIndex,

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
        let Some(index_block_handle) = self.get_lowest_index_block_handle_containing_key(key)
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block
            .get_lowest_data_block_handle_containing_item(key)
            .cloned())
    }

    pub fn get_lowest_data_block_handle_not_containing_item(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        let Some(index_block_handle) = self.get_lowest_index_block_handle_not_containing_key(key)
        else {
            return Ok(Some(self.get_last_data_block_handle(cache_policy)?));
        };

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block.items.first().cloned())
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

    #[must_use]
    pub fn get_first_index_block_handle(&self) -> &KeyedBlockHandle {
        self.top_level_index.get_first_block_handle()
    }

    pub fn get_first_data_block_handle(
        &self,
        cache_policy: CachePolicy,
    ) -> crate::Result<KeyedBlockHandle> {
        let index_block_handle = self.top_level_index.get_first_block_handle();

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block
            .items
            .first()
            .expect("index block should not be empty")
            .clone())
    }

    pub fn get_last_data_block_handle(
        &self,
        cache_policy: CachePolicy,
    ) -> crate::Result<KeyedBlockHandle> {
        let index_block_handle = self.top_level_index.get_last_block_handle();

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block
            .items
            .last()
            .expect("index block should not be empty")
            .clone())
    }

    /// Returns the last index_block handle
    #[must_use]
    #[allow(clippy::doc_markdown)]
    pub fn get_last_index_block_handle(&self) -> &KeyedBlockHandle {
        self.top_level_index.get_last_block_handle()
    }

    /// Loads an index block from disk
    pub fn load_index_block(
        &self,
        block_handle: &KeyedBlockHandle,
        cache_policy: CachePolicy,
    ) -> crate::Result<Arc<IndexBlock>> {
        log::trace!("loading index block {:?}/{block_handle:?}", self.segment_id);

        if let Some(block) = self.blocks.get(self.segment_id, block_handle.offset) {
            // Cache hit: Copy from block

            Ok(block)
        } else {
            // Cache miss: load from disk

            let file_guard = self
                .descriptor_table
                .access(&self.segment_id)?
                .expect("should acquire file handle");

            let block = IndexBlock::from_file_compressed(
                &mut *file_guard.file.lock().expect("lock is poisoned"),
                block_handle.offset,
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

    #[cfg(test)]
    #[allow(dead_code, clippy::expect_used)]
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
        file_path: P,
        offset: u64,
        segment_id: GlobalSegmentId,
        descriptor_table: Arc<FileDescriptorTable>,
        block_cache: Arc<BlockCache>,
    ) -> crate::Result<Self> {
        let file_path = file_path.as_ref();
        log::trace!("Reading block index from {file_path:?}");

        let top_level_index = TopLevelIndex::from_file(file_path, offset)?;

        Ok(Self {
            descriptor_table,
            segment_id,
            top_level_index,
            blocks: IndexBlockFetcher(block_cache),
        })
    }
}
