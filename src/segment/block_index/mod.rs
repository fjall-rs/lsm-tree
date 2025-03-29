// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block_handle;
pub mod full_index;
pub mod top_level;
pub mod two_level_index;
pub mod writer;

use super::{
    block::{offset::BlockOffset, Block},
    value_block::CachePolicy,
};
use crate::binary_search::partition_point;
use block_handle::KeyedBlockHandle;
use full_index::FullBlockIndex;
use two_level_index::TwoLevelBlockIndex;

pub type IndexBlock = Block<KeyedBlockHandle>;

#[allow(clippy::module_name_repetitions)]
pub trait KeyedBlockIndex {
    /// Gets the lowest block handle that may contain the given item
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>>;

    /// Gets the last block handle that may contain the given item
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>>;

    /// Returns a handle to the last block
    fn get_last_block_handle(&self, cache_policy: CachePolicy) -> crate::Result<&KeyedBlockHandle>;
}

impl KeyedBlockIndex for [KeyedBlockHandle] {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        let idx = partition_point(self, |item| item.end_key < key);
        Ok(self.get(idx))
    }

    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        let idx = partition_point(self, |x| &*x.end_key <= key);

        if idx == 0 {
            return Ok(self.first());
        }
        if idx == self.len() {
            let Some(last_block) = self.last() else {
                return Ok(None);
            };

            if last_block.end_key < key {
                return Ok(None);
            }

            return Ok(Some(last_block));
        }

        Ok(self.get(idx))
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<&KeyedBlockHandle> {
        // NOTE: Index is never empty
        #[allow(clippy::expect_used)]
        Ok(self.last().expect("index should not be empty"))
    }
}

#[enum_dispatch::enum_dispatch]
pub trait BlockIndex {
    /// Gets the lowest block handle that may contain the given item
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>>;

    /// Gets the last block handle that may contain the given item
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>>;

    /// Returns a handle to the last block
    fn get_last_block_handle(&self, cache_policy: CachePolicy) -> crate::Result<BlockOffset>;
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
#[enum_dispatch::enum_dispatch(BlockIndex)]
#[allow(clippy::module_name_repetitions)]
pub enum BlockIndexImpl {
    Full(FullBlockIndex),
    TwoLevel(TwoLevelBlockIndex),
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{segment::block::offset::BlockOffset, UserKey};
    use test_log::test;

    fn bh<K: Into<UserKey>>(end_key: K, offset: BlockOffset) -> KeyedBlockHandle {
        KeyedBlockHandle {
            end_key: end_key.into(),
            offset,
        }
    }

    #[test]
    fn block_handle_array_lowest() {
        let index = [
            bh(*b"c", BlockOffset(0)),
            bh(*b"g", BlockOffset(10)),
            bh(*b"g", BlockOffset(20)),
            bh(*b"l", BlockOffset(30)),
            bh(*b"t", BlockOffset(40)),
        ];

        {
            let handle = index
                .get_lowest_block_containing_key(b"a", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(0));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"b", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(0));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"c", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(0));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"d", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"g");
            assert_eq!(handle.offset, BlockOffset(10));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"j", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"l");
            assert_eq!(handle.offset, BlockOffset(30));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"m", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"t");
            assert_eq!(handle.offset, BlockOffset(40));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"t", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"t");
            assert_eq!(handle.offset, BlockOffset(40));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"z", CachePolicy::Read)
                .expect("cannot fail");

            assert!(handle.is_none());
        }
    }

    #[test]
    fn block_handle_array_spanning_lowest() {
        let index = [
            bh(*b"a", BlockOffset(0)),
            bh(*b"a", BlockOffset(10)),
            bh(*b"a", BlockOffset(20)),
            bh(*b"a", BlockOffset(30)),
            bh(*b"b", BlockOffset(40)),
            bh(*b"b", BlockOffset(50)),
            bh(*b"c", BlockOffset(60)),
        ];

        {
            let handle = index
                .get_lowest_block_containing_key(b"0", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"a");
            assert_eq!(handle.offset, BlockOffset(0));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"a", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"a");
            assert_eq!(handle.offset, BlockOffset(0));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"ab", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, BlockOffset(40));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"b", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, BlockOffset(40));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"c", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(60));
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"d", CachePolicy::Read)
                .expect("cannot fail");

            assert!(handle.is_none());
        }
    }

    #[test]
    fn block_handle_array_last_of_key() {
        let index = [
            bh(*b"a", BlockOffset(0)),
            bh(*b"a", BlockOffset(10)),
            bh(*b"a", BlockOffset(20)),
            bh(*b"a", BlockOffset(30)),
            bh(*b"b", BlockOffset(40)),
            bh(*b"b", BlockOffset(50)),
            bh(*b"c", BlockOffset(60)),
        ];

        {
            let handle = index
                .get_last_block_containing_key(b"0", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"a");
            assert_eq!(handle.offset, BlockOffset(0));
        }

        {
            let handle = index
                .get_last_block_containing_key(b"a", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, BlockOffset(40));
        }

        {
            let handle = index
                .get_last_block_containing_key(b"ab", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, BlockOffset(40));
        }

        {
            let handle = index
                .get_last_block_containing_key(b"b", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(60));
        }

        {
            let handle = index
                .get_last_block_containing_key(b"c", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(60));
        }

        {
            let handle = index
                .get_last_block_containing_key(b"d", CachePolicy::Read)
                .expect("cannot fail");

            assert!(handle.is_none());
        }
    }

    #[test]
    fn block_handle_array_last() {
        let index = [
            bh(*b"a", BlockOffset(0)),
            bh(*b"a", BlockOffset(10)),
            bh(*b"a", BlockOffset(20)),
            bh(*b"a", BlockOffset(30)),
            bh(*b"b", BlockOffset(40)),
            bh(*b"b", BlockOffset(50)),
            bh(*b"c", BlockOffset(60)),
        ];

        {
            let handle = index
                .get_last_block_handle(CachePolicy::Read)
                .expect("cannot fail");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, BlockOffset(60));
        }
    }
}
