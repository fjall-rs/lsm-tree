// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    value_block::{BlockOffset, CachePolicy, ValueBlock},
    value_block_consumer::ValueBlockConsumer,
};
use crate::{
    descriptor_table::FileDescriptorTable, segment::block::header::Header, value::InternalValue,
    BlockCache, GlobalSegmentId, UserKey,
};
use std::sync::Arc;

pub struct Reader {
    segment_id: GlobalSegmentId,

    descriptor_table: Arc<FileDescriptorTable>,
    block_cache: Arc<BlockCache>,

    data_block_boundary: BlockOffset,

    pub lo_block_offset: BlockOffset,
    pub(crate) lo_block_size: u64,
    pub(crate) lo_block_items: Option<ValueBlockConsumer>,
    pub(crate) lo_initialized: bool,

    pub hi_block_offset: Option<BlockOffset>,
    pub hi_block_backlink: BlockOffset,
    pub hi_block_items: Option<ValueBlockConsumer>,
    pub hi_initialized: bool,

    start_key: Option<UserKey>,
    end_key: Option<UserKey>,

    cache_policy: CachePolicy,
}

impl Reader {
    #[must_use]
    pub fn new(
        data_block_boundary: BlockOffset,
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        lo_block_offset: BlockOffset,
        hi_block_offset: Option<BlockOffset>,
    ) -> Self {
        Self {
            data_block_boundary,

            descriptor_table,
            segment_id,
            block_cache,

            lo_block_offset,
            lo_block_size: 0,
            lo_block_items: None,
            lo_initialized: false,

            hi_block_offset,
            hi_block_backlink: BlockOffset(0),
            hi_block_items: None,
            hi_initialized: false,

            cache_policy: CachePolicy::Write,

            start_key: None,
            end_key: None,
        }
    }

    /// Sets the lower bound block, such that as many blocks as possible can be skipped.
    pub fn set_lower_bound(&mut self, key: UserKey) {
        self.start_key = Some(key);
    }

    /// Sets the upper bound block, such that as many blocks as possible can be skipped.
    pub fn set_upper_bound(&mut self, key: UserKey) {
        self.end_key = Some(key);
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    fn load_data_block(
        &self,
        offset: BlockOffset,
    ) -> crate::Result<Option<(u64, BlockOffset, ValueBlockConsumer)>> {
        let block = ValueBlock::load_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            self.segment_id,
            offset,
            self.cache_policy,
        )?;

        // TODO: we only need to truncate items from blocks that are not the first and last block
        // TODO: because any block inbetween must (trivially) only contain relevant items

        // Truncate as many items as possible
        block.map_or(Ok(None), |block| {
            Ok(Some((
                block.header.data_length.into(),
                block.header.previous_block_offset,
                ValueBlockConsumer::with_bounds(
                    block,
                    self.start_key.as_deref(),
                    self.end_key.as_deref(),
                ),
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

    fn initialize_hi(&mut self) -> crate::Result<()> {
        let offset = self
            .hi_block_offset
            .expect("no hi offset configured for segment reader");

        if let Some((_, backlink, items)) = self.load_data_block(offset)? {
            self.hi_block_items = Some(items);
            self.hi_block_backlink = backlink;
        }

        self.hi_initialized = true;

        Ok(())
    }
}

impl Iterator for Reader {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.lo_initialized {
            fail_iter!(self.initialize_lo());
        }

        if let Some(head) = self.lo_block_items.as_mut()?.next() {
            // Just consume item
            return Some(Ok(head));
        }

        // Front buffer is empty

        // Load next block
        let next_block_offset = BlockOffset(
            *self.lo_block_offset + Header::serialized_len() as u64 + self.lo_block_size,
        );

        assert_ne!(
            self.lo_block_offset, next_block_offset,
            "invalid next block offset"
        );

        if next_block_offset >= self.data_block_boundary {
            // We are done
            return None;
        }

        if let Some(hi_offset) = self.hi_block_offset {
            if next_block_offset == hi_offset {
                if !self.hi_initialized {
                    fail_iter!(self.initialize_hi());
                }

                // We reached the last block, consume from it instead
                return self.hi_block_items.as_mut()?.next().map(Ok);
            }
        }

        // TODO: when loading the next data block, we unnecessarily do binary search through it
        // (ValueBlock::with_bounds), but we may be able to skip it sometimes
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

impl DoubleEndedIterator for Reader {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.hi_initialized {
            fail_iter!(self.initialize_hi());
        }

        loop {
            // NOTE: See init function
            let hi_offset = self
                .hi_block_offset
                .expect("no hi offset configured for segment reader");

            if hi_offset == self.lo_block_offset {
                if !self.lo_initialized {
                    fail_iter!(self.initialize_lo());
                }

                // We reached the last block, consume from it instead
                return self.lo_block_items.as_mut()?.next_back().map(Ok);
            }

            if let Some(tail) = self.hi_block_items.as_mut()?.next_back() {
                // Just consume item
                return Some(Ok(tail));
            }

            // Back buffer is empty

            if hi_offset == BlockOffset(0) {
                // We are done
                return None;
            }

            // Load prev block
            let prev_block_offset = self.hi_block_backlink;

            if prev_block_offset == self.lo_block_offset {
                if !self.lo_initialized {
                    fail_iter!(self.initialize_lo());
                }

                // We reached the last block, consume from it instead
                return self.lo_block_items.as_mut()?.next_back().map(Ok);
            }

            // TODO: when loading the next data block, we unnecessarily do binary search through it
            // (ValueBlock::with_bounds), but we may be able to skip it sometimes
            match fail_iter!(self.load_data_block(prev_block_offset)) {
                Some((_, backlink, items)) => {
                    self.hi_block_items = Some(items);
                    self.hi_block_backlink = backlink;
                    self.hi_block_offset = Some(prev_block_offset);

                    // We just loaded the block
                    if let Some(item) = self.hi_block_items.as_mut()?.next_back() {
                        return Some(Ok(item));
                    }
                }
                None => {
                    panic!("searched for invalid data block");
                }
            }
        }
    }
}
