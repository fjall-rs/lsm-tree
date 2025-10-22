// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
pub mod iter;
mod two_level;
mod volatile;

pub use full::FullBlockIndex;
pub use two_level::TwoLevelBlockIndex;
pub use volatile::VolatileBlockIndex;

use super::{IndexBlock, KeyedBlockHandle};

#[enum_dispatch::enum_dispatch]
pub trait BlockIndex {
    fn forward_reader(
        &self,
        needle: &[u8],
    ) -> Option<Box<dyn Iterator<Item = crate::Result<KeyedBlockHandle>> + '_>>;

    fn iter(&self) -> Box<dyn BlockIndexIter>;
}

pub trait BlockIndexIter:
    DoubleEndedIterator<Item = crate::Result<KeyedBlockHandle>> + Send
{
    fn seek_lower(&mut self, key: &[u8]) -> bool;
    fn seek_upper(&mut self, key: &[u8]) -> bool;
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
#[allow(clippy::module_name_repetitions)]
pub enum BlockIndexImpl {
    Full(FullBlockIndex),
    VolatileFull(VolatileBlockIndex),
    TwoLevel(TwoLevelBlockIndex),
}

impl BlockIndex for BlockIndexImpl {
    fn forward_reader(
        &self,
        needle: &[u8],
    ) -> Option<Box<dyn Iterator<Item = crate::Result<KeyedBlockHandle>> + '_>> {
        // TODO: 3.0.0 convert to enum_dispatch
        match self {
            Self::Full(index) => index
                .forward_reader(needle)
                .map(|x| Box::new(x.map(Ok)) as Box<_>),

            Self::VolatileFull(index) => Some(Box::new(index.forward_reader(needle)) as Box<_>),

            BlockIndexImpl::TwoLevel(index) => {
                Some(Box::new(index.forward_reader(needle)) as Box<_>)
            }
        }
    }

    fn iter(&self) -> Box<dyn BlockIndexIter> {
        // TODO: convert to enum_dispatch?
        match self {
            Self::Full(index) => Box::new(index.iter()) as Box<_>,
            Self::VolatileFull(index) => Box::new(index.iter()) as Box<_>,
            BlockIndexImpl::TwoLevel(index) => Box::new(index.iter()) as Box<_>,
        }
    }
}
