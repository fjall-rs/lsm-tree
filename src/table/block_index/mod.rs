// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
pub mod iter;
mod two_level;
mod volatile;

pub use full::FullBlockIndex;
use std::fs::File;
use std::sync::Arc;
pub use two_level::TwoLevelBlockIndex;
pub use volatile::VolatileBlockIndex;

use super::{Block, BlockHandle, KeyedBlockHandle};
use crate::SeqNo;

pub trait BlockIndex {
    fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Option<BlockIndexIterImpl>;
    fn iter(&self) -> BlockIndexIterImpl;
}

pub trait BlockIndexIter: DoubleEndedIterator<Item = crate::Result<KeyedBlockHandle>> {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool;
    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool;
}

pub enum BlockIndexIterImpl {
    Full(self::full::Iter),
    Volatile(self::volatile::Iter),
    TwoLevel(self::two_level::Iter),
}

impl BlockIndexIter for BlockIndexIterImpl {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        match self {
            Self::Full(i) => i.seek_lower(key, seqno),
            Self::Volatile(i) => i.seek_lower(key, seqno),
            Self::TwoLevel(i) => i.seek_lower(key, seqno),
        }
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        match self {
            Self::Full(i) => i.seek_upper(key, seqno),
            Self::Volatile(i) => i.seek_upper(key, seqno),
            Self::TwoLevel(i) => i.seek_upper(key, seqno),
        }
    }
}

impl Iterator for BlockIndexIterImpl {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Full(i) => i.next(),
            Self::Volatile(i) => i.next(),
            Self::TwoLevel(i) => i.next(),
        }
    }
}

impl DoubleEndedIterator for BlockIndexIterImpl {
    fn next_back(&mut self) -> Option<<Self as Iterator>::Item> {
        match self {
            Self::Full(i) => i.next_back(),
            Self::Volatile(i) => i.next_back(),
            Self::TwoLevel(i) => i.next_back(),
        }
    }
}

/// The block index stores references to the positions of blocks on a file and their size
///
/// __________________
/// |                |
/// |     BLOCK0     |
/// |________________| <- 'G': 0x0
/// |                |
/// |     BLOCK1     |
/// |________________| <- 'M': 0x...
/// |                |
/// |     BLOCK2     |
/// |________________| <- 'Z': 0x...
///
/// The block information can be accessed by key.
/// Because the blocks are sorted, any entries not covered by the index (it is sparse) can be
/// found by finding the highest block that has a lower or equal end key than the searched key (by performing in-memory binary search).
/// In the diagram above, searching for 'J' yields the block starting with 'G'.
/// 'J' must be in that block, because the next block starts with 'M').
pub enum BlockIndexImpl {
    Full(FullBlockIndex),
    VolatileFull(VolatileBlockIndex),
    TwoLevel(TwoLevelBlockIndex),
}

impl BlockIndex for BlockIndexImpl {
    fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Option<BlockIndexIterImpl> {
        match self {
            Self::Full(index) => index
                .forward_reader(needle, seqno)
                .map(BlockIndexIterImpl::Full),
            Self::VolatileFull(index) => {
                let mut it = index.iter();

                if it.seek_lower(needle, seqno) {
                    Some(BlockIndexIterImpl::Volatile(it))
                } else {
                    None
                }
            }
            Self::TwoLevel(index) => {
                let mut it = index.iter();

                if it.seek_lower(needle, seqno) {
                    Some(BlockIndexIterImpl::TwoLevel(it))
                } else {
                    None
                }
            }
        }
    }

    fn iter(&self) -> BlockIndexIterImpl {
        match self {
            Self::Full(index) => BlockIndexIterImpl::Full(index.iter()),
            Self::VolatileFull(index) => BlockIndexIterImpl::Volatile(index.iter()),
            Self::TwoLevel(index) => BlockIndexIterImpl::TwoLevel(index.iter()),
        }
    }
}

pub enum PureItem {
    KeyedBlockHandle(KeyedBlockHandle),
    ExpectFileOpen,
    ExpectBlockRead {
        block_handle: BlockHandle,
        file: Arc<File>,
    },
}

pub trait BlockIndexPureIter: DoubleEndedIterator<Item = crate::Result<PureItem>> {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool;
    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool;

    fn supply_file(&mut self, file: Arc<File>);
    fn supply_block(&mut self, handle: BlockHandle, block: Block);
}
