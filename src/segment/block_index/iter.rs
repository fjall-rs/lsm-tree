// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::segment::{
    block::ParsedItem, index_block::Iter as IndexBlockIter, IndexBlock, KeyedBlockHandle,
};
use self_cell::self_cell;

self_cell!(
    pub struct OwnedIndexBlockIter {
        owner: IndexBlock,

        #[covariant]
        dependent: IndexBlockIter,
    }
);

impl OwnedIndexBlockIter {
    pub fn seek_lower(&mut self, needle: &[u8]) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle /* TODO: , seqno */))
    }

    pub fn seek_upper(&mut self, needle: &[u8]) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle /* TODO: , seqno */))
    }
}

impl Iterator for OwnedIndexBlockIter {
    type Item = KeyedBlockHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedIndexBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

pub fn create_index_block_reader(block: IndexBlock) -> OwnedIndexBlockIter {
    OwnedIndexBlockIter::new(block, IndexBlock::iter)
}
