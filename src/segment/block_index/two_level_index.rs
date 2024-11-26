// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    super::{id::GlobalSegmentId, value_block::CachePolicy},
    top_level::TopLevelIndex,
    BlockIndex, IndexBlock,
};
use crate::{
    block_cache::BlockCache,
    descriptor_table::FileDescriptorTable,
    segment::{meta::Metadata, value_block::BlockOffset},
};
use std::{path::Path, sync::Arc};

/// Allows reading index blocks - just a wrapper around a block cache
#[allow(clippy::module_name_repetitions)]
pub struct IndexBlockFetcher(Arc<BlockCache>);

impl IndexBlockFetcher {
    pub fn insert(&self, segment_id: GlobalSegmentId, offset: BlockOffset, value: Arc<IndexBlock>) {
        self.0.insert_index_block(segment_id, offset, value);
    }

    #[must_use]
    pub fn get(&self, segment_id: GlobalSegmentId, offset: BlockOffset) -> Option<Arc<IndexBlock>> {
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

impl BlockIndex for TwoLevelBlockIndex {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        self.get_lowest_data_block_handle_containing_item(key, cache_policy)
    }

    fn get_last_block_handle(&self, cache_policy: CachePolicy) -> crate::Result<BlockOffset> {
        self.get_last_data_block_handle(cache_policy)
    }

    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        self.get_last_data_block_handle_containing_item(key, cache_policy)
    }
}

impl TwoLevelBlockIndex {
    /// Gets the lowest block handle that may contain the given item
    pub fn get_lowest_data_block_handle_containing_item(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        let Some(index_block_handle) = self
            .top_level_index
            .get_lowest_block_containing_key(key, cache_policy)
            .expect("cannot fail")
        else {
            return Ok(None);
        };

        let index_block = self.load_index_block(index_block_handle.offset, cache_policy)?;

        Ok({
            use super::RawBlockIndex;

            index_block
                .items
                .get_lowest_block_containing_key(key, cache_policy)
                .expect("cannot fail")
                .map(|x| x.offset)
        })
    }

    /// Gets the last block handle that may contain the given item
    pub fn get_last_data_block_handle_containing_item(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        let Some(index_block_handle) = self
            .top_level_index
            .get_last_block_containing_key(key, cache_policy)
            .expect("cannot fail")
        else {
            return Ok(Some(self.get_last_data_block_handle(cache_policy)?));
        };

        let index_block = self.load_index_block(index_block_handle.offset, cache_policy)?;

        Ok({
            use super::RawBlockIndex;

            index_block
                .items
                .get_last_block_containing_key(key, cache_policy)
                .expect("cannot fail")
                .map(|x| x.offset)
        })
    }

    pub fn get_last_data_block_handle(
        &self,
        cache_policy: CachePolicy,
    ) -> crate::Result<BlockOffset> {
        let index_block_handle = self
            .top_level_index
            .get_last_block_handle(cache_policy)
            .expect("cannot fail");

        let index_block = self.load_index_block(index_block_handle.offset, cache_policy)?;

        Ok(index_block
            .items
            .last()
            .expect("index block should not be empty")
            .offset)
    }

    /// Loads an index block from disk
    pub fn load_index_block(
        &self,
        offset: BlockOffset,
        cache_policy: CachePolicy,
    ) -> crate::Result<Arc<IndexBlock>> {
        log::trace!("loading index block {:?}/{offset:?}", self.segment_id);

        if let Some(block) = self.index_block_fetcher.get(self.segment_id, offset) {
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
                offset,
            )
            .map_err(|e| {
                log::error!(
                    "Failed to load index block {:?}/{:?}: {e:?}",
                    self.segment_id,
                    offset
                );
                e
            })?;
            // TODO: ^ inspect_err instead: 1.76

            drop(file_guard);

            let block = Arc::new(block);

            if cache_policy == CachePolicy::Write {
                self.index_block_fetcher
                    .insert(self.segment_id, offset, block.clone());
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
        path: P,
        metadata: &Metadata,
        tli_ptr: BlockOffset,
        segment_id: GlobalSegmentId,
        descriptor_table: Arc<FileDescriptorTable>,
        block_cache: Arc<BlockCache>,
    ) -> crate::Result<Self> {
        let file_path = path.as_ref();
        log::trace!("Reading block index from {file_path:?}");

        let top_level_index = TopLevelIndex::from_file(file_path, metadata, tli_ptr)?;

        Ok(Self {
            descriptor_table,
            segment_id,
            top_level_index,
            index_block_fetcher: IndexBlockFetcher(block_cache),
        })
    }
}
