// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{data_block::Iter as DataBlockIter, BlockOffset, DataBlock, GlobalTableId};
use crate::{
    segment::{
        block::ParsedItem,
        block_index::{BlockIndexIter, BlockIndexIterImpl},
        util::load_block,
        BlockHandle,
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

fn create_data_block_reader(block: DataBlock) -> OwnedDataBlockIter {
    OwnedDataBlockIter::new(block, super::data_block::DataBlock::iter)
}

pub struct Iter {
    table_id: GlobalTableId,
    path: Arc<PathBuf>,

    #[allow(clippy::struct_field_names)]
    index_iter: BlockIndexIterImpl,
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
        table_id: GlobalTableId,
        path: Arc<PathBuf>,
        index_iter: BlockIndexIterImpl,
        descriptor_table: Arc<DescriptorTable>,
        cache: Arc<Cache>,
        compression: CompressionType,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            table_id,
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
        // Always try to keep iterating inside the already-materialized low data block first; this
        // lets callers consume multiple entries without touching the index or cache again.
        if let Some(block) = &mut self.lo_data_block {
            if let Some(item) = block.next().map(Ok) {
                return Some(item);
            }
        }

        if !self.index_initialized {
            // The index iterator is lazy-initialized on the first call so that the constructor does
            // not eagerly seek.  This is important because range bounds might be configured *after*
            // `Iter::new`, and we only want to pay the seek cost if iteration actually happens.
            let mut ok = true;

            if let Some(key) = &self.range.0 {
                // Seek to the first block whose end key is ≥ lower bound.
                // If this fails we can immediately conclude the range is empty.
                ok = self.index_iter.seek_lower(key);
            }

            if ok {
                if let Some(key) = &self.range.1 {
                    // Narrow the iterator further by skipping any blocks strictly above the upper
                    // bound.
                    // Again, a miss means the range is empty.
                    ok = self.index_iter.seek_upper(key);
                }
            }

            self.index_initialized = true;

            if !ok {
                // No block in the index overlaps the requested window, so we clear state and return
                // EOF without attempting to touch any data blocks.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next() else {
                // No more block handles coming from the index.  Flush any pending items buffered on
                // the high side (used by reverse iteration) before signalling completion.
                if let Some(block) = &mut self.hi_data_block {
                    if let Some(item) = block.next().map(Ok) {
                        return Some(item);
                    }
                }

                // Nothing left to serve; drop both buffers so the iterator can be reused safely.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = fail_iter!(handle);

            // Load the next data block referenced by the index handle.  We try the shared block
            // cache first to avoid hitting the filesystem, and fall back to `load_block` on miss.
            #[allow(clippy::single_match_else)]
            let block = match self.cache.get_block(self.table_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block(
                        self.table_id,
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
                // Each block is self-contained, so we have to apply range bounds again to discard
                // entries that precede the requested lower key.
                reader.seek_lower(key, SeqNo::MAX);
            }
            if let Some(key) = &self.range.1 {
                // Ditto for the upper bound: advance the block-local iterator to the right spot.
                reader.seek_upper(key, SeqNo::MAX);
            }

            let item = reader.next();

            self.lo_offset = handle.offset();
            self.lo_data_block = Some(reader);

            if let Some(item) = item {
                // Serving the first item immediately avoids stashing it in a temporary buffer and
                // keeps block iteration semantics identical to the simple case at the top.
                return Some(Ok(item));
            }
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Mirror the forward iterator: prefer consuming buffered items from the high data block to
        // avoid touching the index once a block has been materialized.
        if let Some(block) = &mut self.hi_data_block {
            if let Some(item) = block.next_back().map(Ok) {
                return Some(item);
            }
        }

        if !self.index_initialized {
            // As in `next`, set up the index iterator lazily so that callers can configure range
            // bounds before we spend time seeking or loading blocks.
            let mut ok = true;

            if let Some(key) = &self.range.0 {
                ok = self.index_iter.seek_lower(key);
            }

            if ok {
                if let Some(key) = &self.range.1 {
                    ok = self.index_iter.seek_upper(key);
                }
            }

            self.index_initialized = true;

            if !ok {
                // No index span overlaps the requested window; clear both buffers and finish early.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            }
        }

        loop {
            let Some(handle) = self.index_iter.next_back() else {
                // Once we exhaust the index in reverse order, flush any items that were buffered on
                // the low side (set when iterating forward first) before signalling completion.
                if let Some(block) = &mut self.lo_data_block {
                    if let Some(item) = block.next_back().map(Ok) {
                        return Some(item);
                    }
                }

                // Nothing left to produce; reset both buffers to keep the iterator reusable.
                self.lo_data_block = None;
                self.hi_data_block = None;
                return None;
            };
            let handle = fail_iter!(handle);

            // Retrieve the next data block from the cache (or disk on miss) so the high-side reader
            // can serve entries in reverse order.
            #[allow(clippy::single_match_else)]
            let block = match self.cache.get_block(self.table_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block(
                        self.table_id,
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

            if let Some(key) = &self.range.1 {
                // Reverse iteration needs to clamp the upper bound first so that `next_back` only
                // sees entries ≤ the requested high key.
                reader.seek_upper(key, SeqNo::MAX);
            }
            if let Some(key) = &self.range.0 {
                // Apply the lower bound as well so that we never step past the low key when
                // iterating backwards through the block.
                reader.seek_lower(key, SeqNo::MAX);
            }

            let item = reader.next_back();

            self.hi_offset = handle.offset();
            self.hi_data_block = Some(reader);

            if let Some(item) = item {
                // Emit the first materialized entry immediately to match the forward path and avoid
                // storing it in a temporary buffer.
                return Some(Ok(item));
            }
        }
    }
}
