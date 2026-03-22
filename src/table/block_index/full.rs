// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::comparator::SharedComparator;
use crate::table::block_index::{iter::OwnedIndexBlockIter, BlockIndexIter};
use crate::table::{IndexBlock, KeyedBlockHandle};
use crate::SeqNo;

/// Index that translates item keys to data block handles
///
/// The index is fully loaded into memory.
pub struct FullBlockIndex {
    block: IndexBlock,
    comparator: SharedComparator,
}

impl FullBlockIndex {
    pub fn new(block: IndexBlock, comparator: SharedComparator) -> Self {
        Self { block, comparator }
    }

    pub fn inner(&self) -> &IndexBlock {
        &self.block
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
        let cmp = self.comparator.clone();
        Iter(OwnedIndexBlockIter::new(self.block.clone(), |b| {
            b.iter(cmp)
        }))
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
