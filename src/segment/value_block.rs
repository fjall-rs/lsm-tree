use super::{block::Block, id::GlobalSegmentId};
use crate::{descriptor_table::FileDescriptorTable, BlockCache, Value};
use std::sync::Arc;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    /// Read cached blocks, but do not change cache
    Read,

    /// Read cached blocks, and update cache
    Write,
}

/// Value blocks are the building blocks of a [`crate::segment::Segment`]. Each block is a sorted list of [`Value`]s,
/// and stored in compressed form on disk, in sorted order.
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
#[allow(clippy::module_name_repetitions)]
pub type ValueBlock = Block<Value>;

impl ValueBlock {
    #[must_use]
    pub fn get_latest(&self, key: &[u8]) -> Option<&Value> {
        let idx = self.items.partition_point(|item| &*item.key < key);
        self.items.get(idx).filter(|&item| &*item.key == key)
    }

    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.items.iter().map(Value::size).sum::<usize>()
    }

    pub fn load_by_block_handle(
        descriptor_table: &FileDescriptorTable,
        block_cache: &BlockCache,
        segment_id: GlobalSegmentId,
        offset: u64,
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<Arc<Self>>> {
        Ok(
            if let Some(block) = block_cache.get_disk_block(segment_id, offset) {
                // Cache hit: Copy from block

                Some(block)
            } else {
                // Cache miss: load from disk

                log::trace!("loading value block from disk: {segment_id:?}/{offset:?}");

                let file_guard = descriptor_table
                    .access(&segment_id)?
                    .expect("should acquire file handle");

                let block = Self::from_file_compressed(
                    &mut *file_guard.file.lock().expect("lock is poisoned"),
                    offset,
                )?;

                drop(file_guard);

                let block = Arc::new(block);

                if cache_policy == CachePolicy::Write {
                    block_cache.insert_disk_block(segment_id, offset, Arc::clone(&block));
                }

                Some(block)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        segment::{block::header::Header as BlockHeader, meta::CompressionType},
        ValueType,
    };
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn value_block_find_latest() {
        let items = vec![
            Value::new(*b"b", *b"b", 2, ValueType::Value),
            Value::new(*b"b", *b"b", 1, ValueType::Value),
            Value::new(*b"b", *b"b", 0, ValueType::Value),
            Value::new(*b"c", *b"c", 0, ValueType::Value),
            Value::new(*b"d", *b"d", 5, ValueType::Value),
        ];

        let block = ValueBlock {
            items: items.into_boxed_slice(),
            header: BlockHeader {
                compression: CompressionType::Lz4,
                crc: 0,
                data_length: 0,
                previous_block_offset: 0,
            },
        };

        assert_eq!(block.get_latest(b"a"), None);
        assert_eq!(
            block.get_latest(b"b"),
            Some(&Value::new(*b"b", *b"b", 2, ValueType::Value))
        );
        assert_eq!(
            block.get_latest(b"c"),
            Some(&Value::new(*b"c", *b"c", 0, ValueType::Value))
        );
        assert_eq!(
            block.get_latest(b"d"),
            Some(&Value::new(*b"d", *b"d", 5, ValueType::Value))
        );
        assert_eq!(block.get_latest(b"e"), None);
    }
}
