use self::super::block_handle::KeyedBlockHandle;
use super::super::id::GlobalSegmentId;
use super::super::value_block::CachePolicy;
use super::top_level::TopLevelIndex;
use super::{BlockIndex, IndexBlock};
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use std::path::Path;
use std::sync::Arc;

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

/// Index that translates item keys to block handles
///
/// The index is only partially loaded into memory.
///
/// See <https://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html>
#[allow(clippy::module_name_repetitions)]
pub struct TwoLevelBlockIndex {
    descriptor_table: Arc<FileDescriptorTable>,

    /// Segment ID
    segment_id: GlobalSegmentId,

    /// Level-0 index. Is read-only and always fully loaded.
    ///
    /// This index points to index blocks inside the level-1 index.
    pub(crate) top_level_index: TopLevelIndex,

    /// Level-1 index. This index is only partially loaded into memory, decreasing memory usage, compared to a fully loaded one.
    ///
    /// However to find a disk block, one layer of indirection is required:
    ///
    /// To find a reference to a segment block, first the level-0 index needs to be checked,
    /// then the corresponding index block needs to be loaded, which contains the wanted disk block handle.
    index_block_fetcher: IndexBlockFetcher,
}

impl TwoLevelBlockIndex {
    /// Gets the lowest block handle that may contain the given item
    pub fn get_lowest_data_block_handle_containing_item(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        let Some(index_block_handle) = self
            .top_level_index
            .get_lowest_block_containing_key(key, cache_policy)
            .expect("cannot fail")
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block
            .items
            .get_lowest_block_containing_key(key, cache_policy)
            .expect("cannot fail")
            .cloned())
    }

    pub fn get_lowest_data_block_handle_not_containing_item(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        let Some(index_block_handle) = self
            .top_level_index
            .get_lowest_block_not_containing_key(key, cache_policy)
            .expect("cannot fail")
        else {
            return Ok(Some(self.get_last_data_block_handle(cache_policy)?));
        };

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block.items.first().cloned())
    }

    pub fn get_last_data_block_handle(
        &self,
        cache_policy: CachePolicy,
    ) -> crate::Result<KeyedBlockHandle> {
        let index_block_handle = self
            .top_level_index
            .get_last_block_handle(cache_policy)
            .expect("cannot fail");

        let index_block = self.load_index_block(index_block_handle, cache_policy)?;

        Ok(index_block
            .items
            .last()
            .expect("index block should not be empty")
            .clone())
    }

    /// Loads an index block from disk
    pub fn load_index_block(
        &self,
        block_handle: &KeyedBlockHandle,
        cache_policy: CachePolicy,
    ) -> crate::Result<Arc<IndexBlock>> {
        log::trace!("loading index block {:?}/{block_handle:?}", self.segment_id);

        if let Some(block) = self
            .index_block_fetcher
            .get(self.segment_id, block_handle.offset)
        {
            // Cache hit: Copy from block

            Ok(block)
        } else {
            // Cache miss: load from disk

            let file_guard = self
                .descriptor_table
                .access(&self.segment_id)?
                .expect("should acquire file handle");

            let block = IndexBlock::from_file(
                &mut *file_guard.file.lock().expect("lock is poisoned"),
                block_handle.offset,
            )?;

            drop(file_guard);

            let block = Arc::new(block);

            if cache_policy == CachePolicy::Write {
                self.index_block_fetcher.insert(
                    self.segment_id,
                    block_handle.offset,
                    block.clone(),
                );
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
            index_block_fetcher: index_block_index,
            top_level_index: TopLevelIndex::from_boxed_slice(Box::default()),
        }
    }

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
            index_block_fetcher: IndexBlockFetcher(block_cache),
        })
    }
}
