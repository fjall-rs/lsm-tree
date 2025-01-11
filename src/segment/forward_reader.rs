// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    value_block::{BlockOffset, CachePolicy, ValueBlock},
    value_block_consumer::ValueBlockConsumer,
};
use crate::{
    descriptor_table::FileDescriptorTable, segment::block::header::Header, value::InternalValue,
    BlockCache, GlobalSegmentId,
};

/// Segment forward reader specialized for point reads
pub struct ForwardReader<'a> {
    segment_id: GlobalSegmentId,

    descriptor_table: &'a FileDescriptorTable,
    block_cache: &'a BlockCache,

    data_block_boundary: BlockOffset,

    pub lo_block_offset: BlockOffset,
    pub(crate) lo_block_size: u64,
    pub(crate) lo_block_items: Option<ValueBlockConsumer>,
    pub(crate) lo_initialized: bool,

    cache_policy: CachePolicy,
}

impl<'a> ForwardReader<'a> {
    #[must_use]
    pub fn new(
        data_block_boundary: BlockOffset,
        descriptor_table: &'a FileDescriptorTable,
        segment_id: GlobalSegmentId,
        block_cache: &'a BlockCache,
        lo_block_offset: BlockOffset,
    ) -> Self {
        Self {
            descriptor_table,
            segment_id,
            block_cache,

            data_block_boundary,

            lo_block_offset,
            lo_block_size: 0,
            lo_block_items: None,
            lo_initialized: false,

            cache_policy: CachePolicy::Write,
        }
    }

    fn load_data_block(
        &self,
        offset: BlockOffset,
    ) -> crate::Result<Option<(u64, BlockOffset, ValueBlockConsumer)>> {
        let block = ValueBlock::load_by_block_handle(
            self.descriptor_table,
            self.block_cache,
            self.segment_id,
            offset,
            self.cache_policy,
        )?;

        // Truncate as many items as possible
        block.map_or(Ok(None), |block| {
            Ok(Some((
                block.header.data_length.into(),
                block.header.previous_block_offset,
                ValueBlockConsumer::with_bounds(block, None, None),
            )))
        })
    }

    fn initialize_lo(&mut self) -> crate::Result<()> {
        if let Some((size, _, items)) = self.load_data_block(self.lo_block_offset)? {
            self.lo_block_items = Some(items);
            self.lo_block_size = size;
        }

        self.lo_initialized = true;

        Ok(())
    }
}

impl<'a> Iterator for ForwardReader<'a> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.lo_initialized {
            fail_iter!(self.initialize_lo());
        }

        if let Some(head) = self.lo_block_items.as_mut()?.next() {
            // Just consume item
            return Some(Ok(head));
        }

        // Load next block
        let next_block_offset = BlockOffset(
            *self.lo_block_offset + Header::serialized_len() as u64 + self.lo_block_size,
        );

        if next_block_offset >= self.data_block_boundary {
            // We are done
            return None;
        }

        assert_ne!(
            self.lo_block_offset, next_block_offset,
            "invalid next block offset",
        );

        match fail_iter!(self.load_data_block(next_block_offset)) {
            Some((size, _, items)) => {
                self.lo_block_items = Some(items);
                self.lo_block_size = size;
                self.lo_block_offset = next_block_offset;

                // We just loaded the block
                self.lo_block_items.as_mut()?.next().map(Ok)
            }
            None => {
                panic!("searched for invalid data block");
            }
        }
    }
}
