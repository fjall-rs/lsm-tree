// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    block::{offset::BlockOffset, Block},
    id::GlobalSegmentId,
};
use crate::{cache::Cache, descriptor_table::FileDescriptorTable, value::InternalValue};
use std::sync::Arc;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    /// Read cached blocks, but do not change cache
    Read,

    /// Read cached blocks, and update cache
    Write,
}

/// Value blocks are the building blocks of a [`crate::segment::Segment`]. Each block is a sorted list of [`InternalValue`]s,
/// and stored in compressed form on disk, in sorted order.
///
/// The integrity of a block can be checked using the checksum value that is saved in it.
#[allow(clippy::module_name_repetitions)]
pub type ValueBlock = Block<InternalValue>;

impl ValueBlock {
    #[must_use]
    pub fn get_latest(&self, key: &[u8]) -> Option<&InternalValue> {
        let idx = self.items.partition_point(|item| &*item.key.user_key < key);

        self.items
            .get(idx)
            .filter(|&item| &*item.key.user_key == key)
    }

    pub fn load_by_block_handle(
        descriptor_table: &FileDescriptorTable,
        block_cache: &Cache,
        segment_id: GlobalSegmentId,
        offset: BlockOffset,
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<Arc<Self>>> {
        Ok(
            if let Some(block) = block_cache.get_data_block(segment_id, offset) {
                // Cache hit: Copy from block

                Some(block)
            } else {
                // Cache miss: load from disk

                log::trace!("loading value block from disk: {segment_id:?}/{offset:?}");

                let file_guard = descriptor_table
                    .access(&segment_id)?
                    .ok_or(())
                    .map_err(|()| {
                        log::error!("Failed to get file guard for segment {segment_id:?}");
                    })
                    .expect("should acquire file handle");
                // TODO: ^ use inspect instead: 1.76

                let block = Self::from_file(
                    &mut *file_guard.file.lock().expect("lock is poisoned"),
                    offset,
                )
                .map_err(|e| {
                    log::error!("Failed to load value block {segment_id:?}/{offset:?}: {e:?}");
                    e
                })?;
                // TODO: ^ inspect_err instead: 1.76

                drop(file_guard);

                let block = Arc::new(block);

                if cache_policy == CachePolicy::Write {
                    block_cache.insert_data_block(segment_id, offset, block.clone());
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
        segment::{
            block::{checksum::Checksum, header::Header as BlockHeader, ItemSize},
            meta::CompressionType,
        },
        ValueType,
    };
    use test_log::test;

    #[test]
    fn value_block_size() {
        let items = [
            InternalValue::from_components(*b"ba", *b"asd", 2, ValueType::Value),
            InternalValue::from_components(*b"bb", *b"def", 1, ValueType::Value),
        ];
        assert_eq!(28, items.size());
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn value_block_find_latest() {
        let items = [
            InternalValue::from_components(*b"b", *b"b", 2, ValueType::Value),
            InternalValue::from_components(*b"b", *b"b", 1, ValueType::Value),
            InternalValue::from_components(*b"b", *b"b", 0, ValueType::Value),
            InternalValue::from_components(*b"c", *b"c", 0, ValueType::Value),
            InternalValue::from_components(*b"d", *b"d", 5, ValueType::Value),
        ];

        let block = ValueBlock {
            items: items.into(),
            header: BlockHeader {
                compression: CompressionType::None,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                previous_block_offset: BlockOffset(0),
                uncompressed_length: 0,
            },
        };

        assert_eq!(block.get_latest(b"a"), None);
        assert_eq!(
            block.get_latest(b"b"),
            Some(&InternalValue::from_components(
                *b"b",
                *b"b",
                2,
                ValueType::Value
            ))
        );
        assert_eq!(
            block.get_latest(b"c"),
            Some(&InternalValue::from_components(
                *b"c",
                *b"c",
                0,
                ValueType::Value
            ))
        );
        assert_eq!(
            block.get_latest(b"d"),
            Some(&InternalValue::from_components(
                *b"d",
                *b"d",
                5,
                ValueType::Value
            ))
        );
        assert_eq!(block.get_latest(b"e"), None);
    }
}
