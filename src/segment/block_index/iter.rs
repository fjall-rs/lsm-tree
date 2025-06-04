// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::segment::{IndexBlock, KeyedBlockHandle};
use self_cell::self_cell;

type BoxedIter<'a> = Box<dyn DoubleEndedIterator<Item = KeyedBlockHandle> + 'a>;

self_cell!(
    pub struct IndexBlockConsumer {
        owner: IndexBlock,

        #[covariant]
        dependent: BoxedIter,
    }
);

pub fn create_index_block_reader(block: IndexBlock) -> IndexBlockConsumer {
    IndexBlockConsumer::new(block, |block| Box::new(block.iter()))
}

impl Iterator for IndexBlockConsumer {
    type Item = KeyedBlockHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for IndexBlockConsumer {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}
