// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{BlockIndexPureIter, KeyedBlockHandle, PureItem};
use crate::table::util::{load_block_pure, pure};
use crate::table::Block;
#[cfg(feature = "metrics")]
use crate::Metrics;
use crate::{
    table::{
        block::BlockType,
        block_index::{iter::OwnedIndexBlockIter, BlockIndexIter},
        util::load_block,
        BlockHandle, IndexBlock,
    },
    Cache, CompressionType, DescriptorTable, GlobalTableId, SeqNo, UserKey,
};
use std::fs::File;
use std::{path::PathBuf, sync::Arc};

/// Index that translates item keys to data block handles
///
/// The index is loaded on demand.
pub struct VolatileBlockIndex {
    pub(crate) table_id: GlobalTableId,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) descriptor_table: Arc<DescriptorTable>,
    pub(crate) cache: Arc<Cache>,
    pub(crate) handle: BlockHandle,
    pub(crate) compression: CompressionType,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl VolatileBlockIndex {
    pub fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Iter {
        let mut iter = Iter::new(self);
        iter.seek_lower(needle, seqno);
        iter
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self)
    }
}

pub struct Iter {
    inner: Option<OwnedIndexBlockIter>,
    table_id: GlobalTableId,
    path: Arc<PathBuf>,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    handle: BlockHandle,
    compression: CompressionType,

    lo: Option<(UserKey, SeqNo)>,
    hi: Option<(UserKey, SeqNo)>,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl Iter {
    fn new(index: &VolatileBlockIndex) -> Self {
        Self {
            inner: None,
            table_id: index.table_id,
            path: index.path.clone(),
            descriptor_table: index.descriptor_table.clone(),
            cache: index.cache.clone(),
            handle: index.handle,
            compression: index.compression,

            lo: None,
            hi: None,

            #[cfg(feature = "metrics")]
            metrics: index.metrics.clone(),
        }
    }
}

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.lo = Some((key.into(), seqno));
        true
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.hi = Some((key.into(), seqno));
        true
    }
}

impl Iterator for Iter {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner {
            inner.next().map(Ok)
        } else {
            let block = fail_iter!(load_block(
                self.table_id,
                &self.path,
                &self.descriptor_table,
                &self.cache,
                &self.handle,
                BlockType::Index,
                self.compression,
                #[cfg(feature = "metrics")]
                &self.metrics,
            ));
            let index_block = IndexBlock::new(block);

            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

            if let Some((lo_key, lo_seqno)) = &self.lo {
                if !iter.seek_lower(lo_key, *lo_seqno) {
                    return None;
                }
            }
            if let Some((hi_key, hi_seqno)) = &self.hi {
                if !iter.seek_upper(hi_key, *hi_seqno) {
                    return None;
                }
            }

            let next_item = iter.next().map(Ok);

            self.inner = Some(iter);

            next_item
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner {
            inner.next_back().map(Ok)
        } else {
            let block = fail_iter!(load_block(
                self.table_id,
                &self.path,
                &self.descriptor_table,
                &self.cache,
                &self.handle,
                BlockType::Index,
                self.compression,
                #[cfg(feature = "metrics")]
                &self.metrics,
            ));
            let index_block = IndexBlock::new(block);

            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

            if let Some((lo_key, lo_seqno)) = &self.lo {
                if !iter.seek_lower(lo_key, *lo_seqno) {
                    return None;
                }
            }
            if let Some((hi_key, hi_seqno)) = &self.hi {
                if !iter.seek_upper(hi_key, *hi_seqno) {
                    return None;
                }
            }

            let next_item = iter.next_back().map(Ok);

            self.inner = Some(iter);

            next_item
        }
    }
}

enum StateKind {
    Ready,
    ExpectingFile,
    ExpectingBlock,
    NeedsToCheckBlock(Block),
}

// Add PureIter struct
pub struct PureIter {
    state: StateKind,
    iter: Iter,
    handle: BlockHandle,
}

impl PureIter {
    pub fn new(iter: Iter, handle: BlockHandle) -> Self {
        Self {
            state: StateKind::Ready,
            iter,
            handle,
        }
    }

    pub fn supply_file(&mut self, file: Arc<File>) {
        if !matches!(self.state, StateKind::ExpectingFile) {
            panic!("unexpected call to supply_file while not expecting file to be open");
        }
        self.iter
            .descriptor_table
            .insert_for_table(self.iter.table_id, file);
        self.state = StateKind::ExpectingBlock;
    }

    pub fn supply_block(&mut self, handle: BlockHandle, block: Block) {
        if !matches!(self.state, StateKind::ExpectingBlock) {
            panic!("unexpected call to supply_block while not expecting block to be read");
        }
        self.iter
            .cache
            .insert_block(self.iter.table_id, handle.offset(), block.clone());
        self.state = StateKind::NeedsToCheckBlock(block);
    }
}

impl Iterator for PureIter {
    type Item = crate::Result<PureItem>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut compute = || -> (StateKind, Option<Self::Item>) {
            match std::mem::replace(&mut self.state, StateKind::Ready) {
                StateKind::Ready => {
                    if let Some(inner) = &mut self.iter.inner {
                        if let Some(item) = inner.next() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
                        return (StateKind::Ready, None);
                    }

                    match load_block_pure(
                        self.iter.table_id,
                        &self.iter.descriptor_table,
                        &self.iter.cache,
                        &self.handle,
                        BlockType::Index,
                        #[cfg(feature = "metrics")]
                        &self.iter.metrics,
                    ) {
                        pure::Output::Block(block) => {
                            let index_block = IndexBlock::new(block);
                            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

                            if let Some((lo_key, lo_seqno)) = &self.iter.lo {
                                if !iter.seek_lower(lo_key, *lo_seqno) {
                                    return (StateKind::Ready, None);
                                }
                            }
                            if let Some((hi_key, hi_seqno)) = &self.iter.hi {
                                if !iter.seek_upper(hi_key, *hi_seqno) {
                                    return (StateKind::Ready, None);
                                }
                            }

                            let next_item = iter.next();
                            self.iter.inner = Some(iter);

                            if let Some(item) = next_item {
                                return (
                                    StateKind::Ready,
                                    Some(Ok(PureItem::KeyedBlockHandle(item))),
                                );
                            }
                            (StateKind::Ready, None)
                        }
                        pure::Output::OpenFd => {
                            (StateKind::ExpectingFile, Some(Ok(PureItem::ExpectFileOpen)))
                        }
                        pure::Output::ReadBlock(file) => (
                            StateKind::ExpectingBlock,
                            Some(Ok(PureItem::ExpectBlockRead {
                                block_handle: self.handle,
                                file,
                            })),
                        ),
                    }
                }
                StateKind::ExpectingFile => {
                    panic!("unexpected call to next while expecting file to be open")
                }
                StateKind::ExpectingBlock => {
                    panic!("unexpected call to next while expecting block to be read")
                }
                StateKind::NeedsToCheckBlock(block) => {
                    let index_block = IndexBlock::new(block);
                    let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

                    if let Some((lo_key, lo_seqno)) = &self.iter.lo {
                        if !iter.seek_lower(lo_key, *lo_seqno) {
                            return (StateKind::Ready, None);
                        }
                    }
                    if let Some((hi_key, hi_seqno)) = &self.iter.hi {
                        if !iter.seek_upper(hi_key, *hi_seqno) {
                            return (StateKind::Ready, None);
                        }
                    }

                    let next_item = iter.next();
                    self.iter.inner = Some(iter);

                    if let Some(item) = next_item {
                        return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                    }
                    (StateKind::Ready, None)
                }
            }
        };

        let (new_state, item) = compute();
        self.state = new_state;
        item
    }
}

impl DoubleEndedIterator for PureIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut compute = || -> (StateKind, Option<Self::Item>) {
            match std::mem::replace(&mut self.state, StateKind::Ready) {
                StateKind::Ready => {
                    if let Some(inner) = &mut self.iter.inner {
                        if let Some(item) = inner.next_back() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
                        return (StateKind::Ready, None);
                    }

                    match load_block_pure(
                        self.iter.table_id,
                        &self.iter.descriptor_table,
                        &self.iter.cache,
                        &self.handle,
                        BlockType::Index,
                        #[cfg(feature = "metrics")]
                        &self.iter.metrics,
                    ) {
                        pure::Output::Block(block) => {
                            let index_block = IndexBlock::new(block);
                            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

                            if let Some((lo_key, lo_seqno)) = &self.iter.lo {
                                if !iter.seek_lower(lo_key, *lo_seqno) {
                                    return (StateKind::Ready, None);
                                }
                            }
                            if let Some((hi_key, hi_seqno)) = &self.iter.hi {
                                if !iter.seek_upper(hi_key, *hi_seqno) {
                                    return (StateKind::Ready, None);
                                }
                            }

                            let next_item = iter.next_back();
                            self.iter.inner = Some(iter);

                            if let Some(item) = next_item {
                                return (
                                    StateKind::Ready,
                                    Some(Ok(PureItem::KeyedBlockHandle(item))),
                                );
                            }
                            (StateKind::Ready, None)
                        }
                        pure::Output::OpenFd => {
                            (StateKind::ExpectingFile, Some(Ok(PureItem::ExpectFileOpen)))
                        }
                        pure::Output::ReadBlock(file) => (
                            StateKind::ExpectingBlock,
                            Some(Ok(PureItem::ExpectBlockRead {
                                block_handle: self.handle,
                                file,
                            })),
                        ),
                    }
                }
                StateKind::ExpectingFile => {
                    panic!("unexpected call to next_back while expecting file to be open")
                }
                StateKind::ExpectingBlock => {
                    panic!("unexpected call to next_back while expecting block to be read")
                }
                StateKind::NeedsToCheckBlock(block) => {
                    let index_block = IndexBlock::new(block);
                    let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

                    if let Some((lo_key, lo_seqno)) = &self.iter.lo {
                        if !iter.seek_lower(lo_key, *lo_seqno) {
                            return (StateKind::Ready, None);
                        }
                    }
                    if let Some((hi_key, hi_seqno)) = &self.iter.hi {
                        if !iter.seek_upper(hi_key, *hi_seqno) {
                            return (StateKind::Ready, None);
                        }
                    }

                    let next_item = iter.next_back();
                    self.iter.inner = Some(iter);

                    if let Some(item) = next_item {
                        return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                    }
                    (StateKind::Ready, None)
                }
            }
        };

        let (new_state, item) = compute();
        self.state = new_state;
        item
    }
}

impl BlockIndexPureIter for PureIter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.iter.lo = Some((key.into(), seqno));
        true
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.iter.hi = Some((key.into(), seqno));
        true
    }

    fn supply_file(&mut self, file: Arc<File>) {
        self.supply_file(file)
    }

    fn supply_block(&mut self, handle: BlockHandle, block: Block) {
        self.supply_block(handle, block)
    }
}
