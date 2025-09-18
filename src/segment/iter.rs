// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{data_block::Iter as DataBlockIter, BlockOffset, DataBlock, GlobalSegmentId};
use crate::{
    segment::{
        block::ParsedItem, block_index::iter::OwnedIndexBlockIter, util::load_block, BlockHandle,
    },
    Cache, CompressionType, DescriptorTable, InternalValue, SeqNo, UserKey,
};
use self_cell::self_cell;
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

type InnerIter<'a> = DataBlockIter<'a>;

self_cell!(
    pub struct OwnedDataBlockIter {
        owner: DataBlock,

        #[covariant]
        dependent: InnerIter,
    }
);

impl OwnedDataBlockIter {
    pub fn seek_lower(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle /* TODO: , seqno */))
    }

    pub fn seek_upper(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle /* TODO: , seqno */))
    }
}

impl Iterator for OwnedDataBlockIter {
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedDataBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

pub fn create_data_block_reader(block: DataBlock) -> OwnedDataBlockIter {
    OwnedDataBlockIter::new(block, super::data_block::DataBlock::iter)
}

pub struct Iter {
    segment_id: GlobalSegmentId,
    path: Arc<PathBuf>,

    #[allow(clippy::struct_field_names)]
    index_iter: OwnedIndexBlockIter,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    compression: CompressionType,

    index_initialized: bool,

    lo_offset: BlockOffset,
    lo_data_block: Option<OwnedDataBlockIter>,

    hi_offset: BlockOffset,
    hi_data_block: Option<OwnedDataBlockIter>,

    range: (Option<UserKey>, Option<UserKey>),

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    pub fn new(
        segment_id: GlobalSegmentId,
        path: Arc<PathBuf>,
        index_iter: OwnedIndexBlockIter,
        descriptor_table: Arc<DescriptorTable>,
        cache: Arc<Cache>,
        compression: CompressionType,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            segment_id,
            path,

            index_iter,
            descriptor_table,
            cache,
            compression,

            index_initialized: false,

            lo_offset: BlockOffset(0),
            lo_data_block: None,

            hi_offset: BlockOffset(u64::MAX),
            hi_data_block: None,

            range: (None, None),

            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    pub fn set_lower_bound(&mut self, key: UserKey) {
        self.range.0 = Some(key);
    }

    pub fn set_upper_bound(&mut self, key: UserKey) {
        self.range.1 = Some(key);
    }
}

impl Iterator for Iter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(block) = &mut self.lo_data_block {
            if let Some(item) = block.next().map(Ok) {
                return Some(item);
            }
        }

        if !self.index_initialized {
            if let Some(key) = &self.range.0 {
                self.index_iter.seek_lower(key);
            }
            if let Some(key) = &self.range.1 {
                self.index_iter.seek_upper(key);
            }
            self.index_initialized = true;
        }

        loop {
            let Some(handle) = self.index_iter.next() else {
                // NOTE: No more block handles from index,
                // Now check hi buffer if it exists
                if let Some(block) = &mut self.hi_data_block {
                    if let Some(item) = block.next().map(Ok) {
                        return Some(item);
                    }
                }

                // NOTE: If there is no more item, we are done
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };

            // NOTE: Load next lo block
            #[allow(clippy::single_match_else)]
            let block = match self.cache.get_block(self.segment_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block(
                        self.segment_id,
                        &self.path,
                        &self.descriptor_table,
                        &self.cache,
                        &BlockHandle::new(handle.offset(), handle.size()),
                        crate::segment::block::BlockType::Data,
                        self.compression,
                        #[cfg(feature = "metrics")]
                        &self.metrics,
                    ))
                }
            };
            let block = DataBlock::new(block);

            let mut reader = create_data_block_reader(block);

            if let Some(key) = &self.range.0 {
                reader.seek_lower(key, SeqNo::MAX);
            }
            if let Some(key) = &self.range.1 {
                reader.seek_upper(key, SeqNo::MAX);
            }

            let item = reader.next();

            self.lo_offset = handle.offset();
            self.lo_data_block = Some(reader);

            if let Some(item) = item {
                return Some(Ok(item));
            }
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(block) = &mut self.hi_data_block {
            if let Some(item) = block.next_back().map(Ok) {
                return Some(item);
            }
        }

        if !self.index_initialized {
            if let Some(key) = &self.range.0 {
                self.index_iter.seek_lower(key);
            }
            if let Some(key) = &self.range.1 {
                self.index_iter.seek_upper(key);
            }
            self.index_initialized = true;
        }

        loop {
            let Some(handle) = self.index_iter.next_back() else {
                // NOTE: No more block handles from index,
                // Now check lo buffer if it exists
                if let Some(block) = &mut self.lo_data_block {
                    if let Some(item) = block.next_back().map(Ok) {
                        return Some(item);
                    }
                }

                // NOTE: If there is no more item, we are done
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };

            // NOTE: Load next hi block
            #[allow(clippy::single_match_else)]
            let block = match self.cache.get_block(self.segment_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block(
                        self.segment_id,
                        &self.path,
                        &self.descriptor_table,
                        &self.cache,
                        &BlockHandle::new(handle.offset(), handle.size()),
                        crate::segment::block::BlockType::Data,
                        self.compression,
                        #[cfg(feature = "metrics")]
                        &self.metrics,
                    ))
                }
            };
            let block = DataBlock::new(block);

            let mut reader = create_data_block_reader(block);

            if let Some(key) = &self.range.0 {
                reader.seek_lower(key, SeqNo::MAX);
            }
            if let Some(key) = &self.range.1 {
                reader.seek_upper(key, SeqNo::MAX);
            }

            let item = reader.next_back();

            self.hi_offset = handle.offset();
            self.hi_data_block = Some(reader);

            if let Some(item) = item {
                return Some(Ok(item));
            }
        }
    }
}
