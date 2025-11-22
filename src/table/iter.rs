// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{data_block::Iter as DataBlockIter, BlockOffset, DataBlock, GlobalTableId};
use crate::{
    table::{
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

pub enum Bound {
    Included(UserKey),
    Excluded(UserKey),
}
type Bounds = (Option<Bound>, Option<Bound>);

self_cell!(
    pub struct OwnedDataBlockIter {
        owner: DataBlock,

        #[covariant]
        dependent: InnerIter,
    }
);

impl OwnedDataBlockIter {
    fn seek_lower_inclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle /* TODO: , seqno */))
    }

    fn seek_upper_inclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle /* TODO: , seqno */))
    }

    fn seek_lower_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_exclusive(needle /* TODO: , seqno */))
    }

    fn seek_upper_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper_exclusive(needle /* TODO: , seqno */))
    }

    pub fn seek_lower_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_lower_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_lower_exclusive(key, seqno),
        }
    }

    pub fn seek_upper_bound(&mut self, bound: &Bound, seqno: SeqNo) -> bool {
        match bound {
            Bound::Included(key) => self.seek_upper_inclusive(key, seqno),
            Bound::Excluded(key) => self.seek_upper_exclusive(key, seqno),
        }
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

    global_seqno: SeqNo,

    #[expect(clippy::struct_field_names)]
    index_iter: BlockIndexIterImpl,

    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    compression: CompressionType,

    index_initialized: bool,

    lo_offset: BlockOffset,
    lo_data_block: Option<OwnedDataBlockIter>,

    hi_offset: BlockOffset,
    hi_data_block: Option<OwnedDataBlockIter>,

    range: Bounds,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    pub fn new(
        table_id: GlobalTableId,
        global_seqno: SeqNo,
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

            global_seqno,

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

    pub fn set_lower_bound(&mut self, bound: Bound) {
        self.range.0 = Some(bound);
    }

    pub fn set_upper_bound(&mut self, bound: Bound) {
        self.range.1 = Some(bound);
    }
}

impl Iterator for Iter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        // Always try to keep iterating inside the already-materialized low data block first; this
        // lets callers consume multiple entries without touching the index or cache again.
        if let Some(block) = &mut self.lo_data_block {
            if let Some(item) = block
                .next()
                .map(|mut v| {
                    v.key.seqno += self.global_seqno;
                    v
                })
                .map(Ok)
            {
                return Some(item);
            }
        }

        if !self.index_initialized {
            // Lazily initialize the index iterator here (not in `new`) so callers can set bounds
            // before we incur any seek or I/O cost. Bounds exclusivity is enforced at the data-
            // block level; index seeks only narrow the span of blocks to touch.
            let mut ok = true;

            if let Some(bound) = &self.range.0 {
                // Seek to the first block whose end key is ≥ lower bound.
                // If this fails we can immediately conclude the range is empty.
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_lower(key);
            }

            if ok {
                // Apply an upper-bound seek to cap the block span, but keep exact high-key
                // handling inside the data block so exclusivity is respected precisely.
                if let Some(bound) = &self.range.1 {
                    let key = match bound {
                        Bound::Included(k) | Bound::Excluded(k) => k,
                    };
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
                    if let Some(item) = block
                        .next()
                        .map(|mut v| {
                            v.key.seqno += self.global_seqno;
                            v
                        })
                        .map(Ok)
                    {
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
            let block = match self.cache.get_block(self.table_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block(
                        self.table_id,
                        &self.path,
                        &self.descriptor_table,
                        &self.cache,
                        &BlockHandle::new(handle.offset(), handle.size()),
                        crate::table::block::BlockType::Data,
                        self.compression,
                        #[cfg(feature = "metrics")]
                        &self.metrics,
                    ))
                }
            };
            let block = DataBlock::new(block);

            let mut reader = create_data_block_reader(block);

            // Forward path: seek the low side first to avoid returning entries below the lower
            // bound, then clamp the iterator on the high side. This guarantees iteration stays in
            // [low, high] with exact control over inclusivity/exclusivity.
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }

            let item = reader.next();

            self.lo_offset = handle.offset();
            self.lo_data_block = Some(reader);

            if let Some(mut item) = item {
                item.key.seqno += self.global_seqno;

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
            if let Some(item) = block
                .next_back()
                .map(|mut v| {
                    v.key.seqno += self.global_seqno;
                    v
                })
                .map(Ok)
            {
                return Some(item);
            }
        }

        if !self.index_initialized {
            // Mirror forward iteration: initialize lazily so bounds can be applied up-front. The
            // index only restricts which blocks we consider; tight bound enforcement happens in
            // the data block readers below.
            let mut ok = true;

            if let Some(bound) = &self.range.0 {
                let key = match bound {
                    Bound::Included(k) | Bound::Excluded(k) => k,
                };
                ok = self.index_iter.seek_lower(key);
            }

            if ok {
                if let Some(bound) = &self.range.1 {
                    let key = match bound {
                        Bound::Included(k) | Bound::Excluded(k) => k,
                    };
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
                    if let Some(item) = block
                        .next_back()
                        .map(|mut v| {
                            v.key.seqno += self.global_seqno;
                            v
                        })
                        .map(Ok)
                    {
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
            let block = match self.cache.get_block(self.table_id, handle.offset()) {
                Some(block) => block,
                None => {
                    fail_iter!(load_block(
                        self.table_id,
                        &self.path,
                        &self.descriptor_table,
                        &self.cache,
                        &BlockHandle::new(handle.offset(), handle.size()),
                        crate::table::block::BlockType::Data,
                        self.compression,
                        #[cfg(feature = "metrics")]
                        &self.metrics,
                    ))
                }
            };
            let block = DataBlock::new(block);

            let mut reader = create_data_block_reader(block);

            // Reverse path: clamp the high side first so `next_back` never yields an entry above
            // the upper bound, then apply the low-side seek to avoid stepping below the lower
            // bound during reverse traversal.
            if let Some(bound) = &self.range.1 {
                reader.seek_upper_bound(bound, SeqNo::MAX);
            }
            if let Some(bound) = &self.range.0 {
                reader.seek_lower_bound(bound, SeqNo::MAX);
            }

            let item = reader.next_back();

            self.hi_offset = handle.offset();
            self.hi_data_block = Some(reader);

            if let Some(mut item) = item {
                item.key.seqno += self.global_seqno;

                // Emit the first materialized entry immediately to match the forward path and avoid
                // storing it in a temporary buffer.
                return Some(Ok(item));
            }
        }
    }
}
