// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::table::block_index::{BlockIndexPureIter, PureItem};
use crate::table::util::{load_block_pure, pure};
use crate::table::{Block, BlockHandle, IndexBlock, KeyedBlockHandle};
#[cfg(feature = "metrics")]
use crate::Metrics;
use crate::SeqNo;
use crate::{
    table::{
        block::BlockType,
        block_index::{iter::OwnedIndexBlockIter, BlockIndexIter},
        util::load_block,
    },
    Cache, CompressionType, DescriptorTable, GlobalTableId, UserKey,
};
use std::fs::File;
use std::path::Path;
use std::{path::PathBuf, sync::Arc};

/// Index that translates item keys to data block handles
///
/// Only the top-level index is loaded into memory.
pub struct TwoLevelBlockIndex {
    pub(crate) top_level_index: IndexBlock,
    pub(crate) table_id: GlobalTableId,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) descriptor_table: Arc<DescriptorTable>,
    pub(crate) cache: Arc<Cache>,
    pub(crate) compression: CompressionType,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl TwoLevelBlockIndex {
    pub fn iter(&self) -> Iter {
        Iter {
            tli_block: self.top_level_index.clone(),
            tli: None,
            lo_consumer: None,
            hi_consumer: None,
            lo: None,
            hi: None,
            table_id: self.table_id,
            path: self.path.clone(),
            descriptor_table: self.descriptor_table.clone(),
            cache: self.cache.clone(),
            compression: self.compression,

            #[cfg(feature = "metrics")]
            metrics: self.metrics.clone(),
        }
    }

    pub fn iter_pure(&self) -> PureIter {
        PureIter::new(self.iter())
    }
}

pub struct Iter {
    tli_block: IndexBlock,
    tli: Option<OwnedIndexBlockIter>,

    lo_consumer: Option<OwnedIndexBlockIter>,
    hi_consumer: Option<OwnedIndexBlockIter>,

    lo: Option<(UserKey, SeqNo)>,
    hi: Option<(UserKey, SeqNo)>,

    table_id: GlobalTableId,
    path: Arc<PathBuf>,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    compression: CompressionType,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    fn init_tli(&mut self) -> bool {
        let mut iter = OwnedIndexBlockIter::new(self.tli_block.clone(), IndexBlock::iter);

        if let Some((lo_key, lo_seqno)) = &self.lo {
            if !iter.seek_lower(lo_key, *lo_seqno) {
                return false;
            }
        }
        if let Some((hi_key, hi_seqno)) = &self.hi {
            if !iter.seek_upper(hi_key, *hi_seqno) {
                return false;
            }
        }

        self.tli = Some(iter);

        true
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
        if let Some(lo_block) = &mut self.lo_consumer {
            if let Some(item) = lo_block.next() {
                return Some(Ok(item));
            }
        }

        if self.tli.is_none() && !self.init_tli() {
            return None;
        }

        if let Some(tli) = &mut self.tli {
            let next_lowest_block = tli.next();

            if let Some(handle) = next_lowest_block {
                let block = fail_iter!(load_block(
                    self.table_id,
                    &self.path,
                    &self.descriptor_table,
                    &self.cache,
                    &handle.into_inner(),
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

                self.lo_consumer = Some(iter);

                if let Some(item) = next_item {
                    return Some(item);
                }
            }
        }

        // Nothing more found, consume from hi consumer
        if let Some(hi_block) = &mut self.hi_consumer {
            if let Some(item) = hi_block.next() {
                return Some(Ok(item));
            }
        }

        None
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(hi_block) = &mut self.hi_consumer {
            if let Some(item) = hi_block.next_back() {
                return Some(Ok(item));
            }
        }

        if self.tli.is_none() && !self.init_tli() {
            return None;
        }

        if let Some(tli) = &mut self.tli {
            let next_highest_block = tli.next_back();

            if let Some(handle) = next_highest_block {
                let block = fail_iter!(load_block(
                    self.table_id,
                    &self.path,
                    &self.descriptor_table,
                    &self.cache,
                    &handle.into_inner(),
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

                self.hi_consumer = Some(iter);

                if let Some(item) = next_item {
                    return Some(item);
                }
            }
        }

        // Nothing more found, consume from lo consumer
        if let Some(lo_block) = &mut self.lo_consumer {
            if let Some(item) = lo_block.next_back() {
                return Some(Ok(item));
            }
        }

        None
    }
}

enum StateKind {
    Ready,
    ExpectingFile,
    ExpectingBlock,
    NeedsToCheckBlock(Block),
}

pub struct PureIter {
    state: StateKind,
    iter: Iter,
}

impl PureIter {
    pub fn new(iter: Iter) -> Self {
        Self {
            state: StateKind::Ready,
            iter,
        }
    }
    pub fn supply_file(&mut self, file: Arc<File>) {
        if matches!(self.state, StateKind::ExpectingFile) {
            panic!("unexpected call to supply_file while expecting file to be open");
        }
        self.iter
            .descriptor_table
            .insert_for_table(self.iter.table_id, file);
        self.state = StateKind::ExpectingBlock;
    }

    pub fn supply_block(&mut self, handle: BlockHandle, block: Block) {
        if matches!(self.state, StateKind::ExpectingBlock) {
            panic!("unexpected call to supply_block while expecting block to be read");
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
                    if let Some(lo_block) = &mut self.iter.lo_consumer {
                        if let Some(item) = lo_block.next() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
                    }

                    if self.iter.tli.is_none() && !self.iter.init_tli() {
                        return (StateKind::Ready, None);
                    }

                    if let Some(tli) = &mut self.iter.tli {
                        let next_highest_block = tli.next_back();

                        if let Some(handle) = next_highest_block {
                            let handle = handle.into_inner();
                            match load_block_pure(
                                self.iter.table_id,
                                &self.iter.descriptor_table,
                                &self.iter.cache,
                                &handle,
                                BlockType::Index,
                                #[cfg(feature = "metrics")]
                                &self.metrics,
                            ) {
                                pure::Output::Block(block) => {
                                    let index_block = IndexBlock::new(block);

                                    let mut iter =
                                        OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

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

                                    let next_item = iter.next_back().map(Ok);

                                    self.iter.hi_consumer = Some(iter);

                                    if let Some(item) = next_item {
                                        return (
                                            StateKind::Ready,
                                            Some(item.map(PureItem::KeyedBlockHandle)),
                                        );
                                    }
                                }
                                pure::Output::OpenFd => {
                                    return (
                                        StateKind::ExpectingFile,
                                        Some(Ok(PureItem::ExpectFileOpen)),
                                    );
                                }
                                pure::Output::ReadBlock(file) => {
                                    return (
                                        StateKind::ExpectingBlock,
                                        Some(Ok(PureItem::ExpectBlockRead {
                                            block_handle: handle,
                                            file,
                                        })),
                                    );
                                }
                            };
                        }
                    }

                    // Nothing more found, consume from lo consumer
                    if let Some(lo_block) = &mut self.iter.lo_consumer {
                        if let Some(item) = lo_block.next_back() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
                    }

                    (StateKind::Ready, None)
                }
                StateKind::ExpectingFile => {
                    panic!("unexpected call to next while expecting file to be open")
                }
                StateKind::ExpectingBlock => {
                    panic!("unexpected call to next while expecting block to be read");
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

                    let next_item = iter.next_back().map(Ok);

                    self.iter.hi_consumer = Some(iter);

                    if let Some(item) = next_item {
                        return (StateKind::Ready, Some(item.map(PureItem::KeyedBlockHandle)));
                    }

                    // Nothing more found, consume from lo consumer
                    if let Some(lo_block) = &mut self.iter.lo_consumer {
                        if let Some(item) = lo_block.next_back() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
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
                    if let Some(hi_block) = &mut self.iter.hi_consumer {
                        if let Some(item) = hi_block.next_back() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
                    }

                    if self.iter.tli.is_none() && !self.iter.init_tli() {
                        return (StateKind::Ready, None);
                    }

                    if let Some(tli) = &mut self.iter.tli {
                        let next_highest_block = tli.next_back();

                        if let Some(handle) = next_highest_block {
                            let handle = handle.into_inner();
                            match load_block_pure(
                                self.iter.table_id,
                                &self.iter.descriptor_table,
                                &self.iter.cache,
                                &handle,
                                BlockType::Index,
                                #[cfg(feature = "metrics")]
                                &self.iter.metrics,
                            ) {
                                pure::Output::Block(block) => {
                                    let index_block = IndexBlock::new(block);

                                    let mut iter =
                                        OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

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

                                    let next_item = iter.next_back().map(Ok);

                                    self.iter.hi_consumer = Some(iter);

                                    if let Some(item) = next_item {
                                        return (
                                            StateKind::Ready,
                                            Some(item.map(PureItem::KeyedBlockHandle)),
                                        );
                                    }
                                }
                                pure::Output::OpenFd => {
                                    return (
                                        StateKind::ExpectingFile,
                                        Some(Ok(PureItem::ExpectFileOpen)),
                                    );
                                }
                                pure::Output::ReadBlock(file) => {
                                    return (
                                        StateKind::ExpectingBlock,
                                        Some(Ok(PureItem::ExpectBlockRead {
                                            block_handle: handle,
                                            file,
                                        })),
                                    );
                                }
                            };
                        }
                    }

                    // Nothing more found, consume from lo consumer
                    if let Some(lo_block) = &mut self.iter.lo_consumer {
                        if let Some(item) = lo_block.next_back() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
                    }

                    (StateKind::Ready, None)
                }
                StateKind::ExpectingFile => {
                    panic!("unexpected call to next_back while expecting file to be open")
                }
                StateKind::ExpectingBlock => {
                    panic!("unexpected call to next_back while expecting block to be read");
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

                    let next_item = iter.next_back().map(Ok);

                    self.iter.hi_consumer = Some(iter);

                    if let Some(item) = next_item {
                        return (StateKind::Ready, Some(item.map(PureItem::KeyedBlockHandle)));
                    }

                    // Nothing more found, consume from lo consumer
                    if let Some(lo_block) = &mut self.iter.lo_consumer {
                        if let Some(item) = lo_block.next_back() {
                            return (StateKind::Ready, Some(Ok(PureItem::KeyedBlockHandle(item))));
                        }
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

    fn file_path(&self) -> Option<&Path> {
        Some(self.iter.path.as_ref())
    }
}
