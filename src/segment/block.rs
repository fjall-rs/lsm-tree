use super::{
    block_index::{block_handle::BlockHandle, BlockIndex},
    id::GlobalSegmentId,
};
use crate::{descriptor_table::FileDescriptorTable, disk_block::DiskBlock, BlockCache, Value};
use std::sync::Arc;

/// Value blocks are the building blocks of a [`crate::segment::Segment`]. Each block is a sorted list of [`Value`]s,
/// and stored in compressed form on disk, in sorted order.
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
#[allow(clippy::module_name_repetitions)]
pub type ValueBlock = DiskBlock<Value>;

impl ValueBlock {
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.items.iter().map(Value::size).sum::<usize>()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    /// Read cached blocks, but do not change cache
    Read,

    /// Read cached blocks, and update cache
    Write,
}

pub fn load_by_block_handle(
    descriptor_table: &FileDescriptorTable,
    block_cache: &BlockCache,
    segment_id: GlobalSegmentId,
    block_handle: &BlockHandle,
    cache_policy: CachePolicy,
) -> crate::Result<Option<Arc<ValueBlock>>> {
    Ok(
        if let Some(block) = block_cache.get_disk_block(segment_id, &block_handle.start_key) {
            // Cache hit: Copy from block

            Some(block)
        } else {
            // Cache miss: load from disk

            let file_guard = descriptor_table
                .access(&segment_id)?
                .expect("should acquire file handle");

            let block = ValueBlock::from_file_compressed(
                &mut *file_guard.file.lock().expect("lock is poisoned"),
                block_handle.offset,
                block_handle.size,
            )?;

            drop(file_guard);

            let block = Arc::new(block);

            if cache_policy == CachePolicy::Write {
                block_cache.insert_disk_block(
                    segment_id,
                    block_handle.start_key.clone(),
                    Arc::clone(&block),
                );
            }

            Some(block)
        },
    )
}

pub fn load_by_item_key<K: AsRef<[u8]>>(
    descriptor_table: &FileDescriptorTable,
    block_index: &BlockIndex,
    block_cache: &BlockCache,
    segment_id: GlobalSegmentId,
    item_key: K,
    cache_policy: CachePolicy,
) -> crate::Result<Option<Arc<ValueBlock>>> {
    Ok(
        if let Some(block_handle) =
            block_index.get_block_containing_item(item_key.as_ref(), cache_policy)?
        {
            load_by_block_handle(
                descriptor_table,
                block_cache,
                segment_id,
                &block_handle,
                cache_policy,
            )?
        } else {
            None
        },
    )
}
