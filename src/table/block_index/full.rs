use crate::table::block_index::{iter::OwnedIndexBlockIter, BlockIndexIter};
use crate::table::{IndexBlock, KeyedBlockHandle};

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

    pub fn forward_reader(&self, needle: &[u8]) -> Option<Iter> {
        let mut it = self.iter();
        if it.seek_lower(needle) {
            Some(it)
        } else {
            None
        }
    }

    pub fn iter(&self) -> Iter {
        Iter(OwnedIndexBlockIter::new(self.0.clone(), IndexBlock::iter))
    }
}

pub struct Iter(OwnedIndexBlockIter);

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8]) -> bool {
        self.0.seek_lower(key)
    }

    fn seek_upper(&mut self, key: &[u8]) -> bool {
        self.0.seek_upper(key)
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
