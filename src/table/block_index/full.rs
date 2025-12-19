// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::table::block_index::{
    iter::OwnedIndexBlockIter, BlockIndexIter, BlockIndexPureIter, PureItem,
};
use crate::table::{Block, BlockHandle, IndexBlock, KeyedBlockHandle};
use crate::SeqNo;
use std::fs::File;
use std::sync::Arc;

/// Index that translates item keys to data block handles
///
/// The index is fully loaded into memory.
pub struct FullBlockIndex(IndexBlock);

impl FullBlockIndex {
    pub fn new(block: IndexBlock) -> Self {
        Self(block)
    }

    pub fn inner(&self) -> &IndexBlock {
        &self.0
    }

    pub fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Option<Iter> {
        let mut it = self.iter();
        if it.seek_lower(needle, seqno) {
            Some(it)
        } else {
            None
        }
    }

    pub fn iter(&self) -> Iter {
        Iter(OwnedIndexBlockIter::new(self.0.clone(), IndexBlock::iter))
    }

    pub fn forward_reader_pure(&self, needle: &[u8], seqno: SeqNo) -> Option<PureIter> {
        let mut it = self.iter_pure();
        if it.seek_lower(needle, seqno) {
            Some(it)
        } else {
            None
        }
    }

    pub fn iter_pure(&self) -> PureIter {
        PureIter(Iter(OwnedIndexBlockIter::new(
            self.0.clone(),
            IndexBlock::iter,
        )))
    }
}

pub struct Iter(OwnedIndexBlockIter);

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.0.seek_lower(key, seqno)
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.0.seek_upper(key, seqno)
    }
}

impl Iterator for Iter {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(Ok)
    }
}

pub struct PureIter(Iter);

impl Iterator for PureIter {
    type Item = crate::Result<PureItem>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|r| r.map(PureItem::KeyedBlockHandle))
    }
}

impl DoubleEndedIterator for PureIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|r| r.map(PureItem::KeyedBlockHandle))
    }
}

impl BlockIndexPureIter for PureIter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.0.seek_lower(key, seqno)
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.0.seek_upper(key, seqno)
    }

    fn supply_file(&mut self, _file: Arc<File>) {
        panic!("unexpected call `supply_file` on full block index");
    }

    fn supply_block(&mut self, _handle: BlockHandle, _block: Block) {
        panic!("unexpected call `supply_block` on full block index");
    }
}
