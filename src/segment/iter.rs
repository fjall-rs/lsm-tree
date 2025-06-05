// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{BlockOffset, DataBlock, GlobalSegmentId, KeyedBlockHandle};
use crate::{
    segment::{util::load_block, BlockHandle},
    Cache, CompressionType, DescriptorTable, InternalValue,
};
use self_cell::self_cell;
use std::{path::PathBuf, sync::Arc};

type BoxedIter<'a> = Box<dyn DoubleEndedIterator<Item = InternalValue> + 'a>;

self_cell!(
    pub struct DataBlockConsumer {
        owner: DataBlock,

        #[covariant]
        dependent: BoxedIter,
    }
);

pub fn create_data_block_reader(block: DataBlock) -> DataBlockConsumer {
    DataBlockConsumer::new(block, |block| Box::new(block.iter()))
}

impl Iterator for DataBlockConsumer {
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for DataBlockConsumer {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

pub struct Iter<I>
where
    I: DoubleEndedIterator<Item = KeyedBlockHandle>,
{
    segment_id: GlobalSegmentId,
    path: Arc<PathBuf>,

    #[allow(clippy::struct_field_names)]
    index_iter: I,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    compression: CompressionType,

    lo_offset: BlockOffset,
    lo_data_block: Option<DataBlockConsumer>,

    hi_offset: BlockOffset,
    hi_data_block: Option<DataBlockConsumer>,
}

impl<I> Iter<I>
where
    I: DoubleEndedIterator<Item = KeyedBlockHandle>,
{
    pub fn new(
        segment_id: GlobalSegmentId,
        path: Arc<PathBuf>,
        index_iter: I,
        descriptor_table: Arc<DescriptorTable>,
        cache: Arc<Cache>,
        compression: CompressionType,
    ) -> Self {
        Self {
            segment_id,
            path,

            index_iter,
            descriptor_table,
            cache,
            compression,

            lo_offset: BlockOffset(0),
            lo_data_block: None,

            hi_offset: BlockOffset(u64::MAX),
            hi_data_block: None,
        }
    }
}

impl<I> Iterator for Iter<I>
where
    I: DoubleEndedIterator<Item = KeyedBlockHandle>,
{
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(block) = &mut self.lo_data_block {
            if let Some(item) = block.next().map(Ok) {
                return Some(item);
            }
        }

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
                    self.compression
                ))
            }
        };
        let block = DataBlock::new(block);

        let mut reader = create_data_block_reader(block);

        let item = reader.next();

        self.lo_offset = handle.offset();
        self.lo_data_block = Some(reader);

        item.map(Ok)
    }
}

impl<I> DoubleEndedIterator for Iter<I>
where
    I: DoubleEndedIterator<Item = KeyedBlockHandle>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(block) = &mut self.hi_data_block {
            if let Some(item) = block.next_back().map(Ok) {
                return Some(item);
            }
        }

        let Some(handle) = self.index_iter.next_back() else {
            // NOTE: No more block handles from index,
            // Now check lo buffer if it exists
            if let Some(block) = &mut self.lo_data_block {
                // eprintln!("=== lo block ===");

                // for item in block.borrow_owner().iter() {
                //     eprintln!(
                //         r#"InternalValue::from_components({:?}, {:?}, {}, {:?}),"#,
                //         item.key.user_key, item.value, item.key.seqno, item.key.value_type,
                //     );
                // }

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
                    self.compression
                ))
            }
        };
        let block = DataBlock::new(block);

        let mut reader = create_data_block_reader(block);

        let item = reader.next_back();

        self.hi_offset = handle.offset();
        self.hi_data_block = Some(reader);

        item.map(Ok)
    }
}
