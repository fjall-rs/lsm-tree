// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block_handle;
pub mod full_index;
pub mod top_level;
pub mod two_level_index;
pub mod writer;

use super::{block::Block, value_block::CachePolicy};
use block_handle::KeyedBlockHandle;
use full_index::FullBlockIndex;
use two_level_index::TwoLevelBlockIndex;

pub type IndexBlock = Block<KeyedBlockHandle>;

impl RawBlockIndex for [KeyedBlockHandle] {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        let idx = self.partition_point(|x| &*x.end_key < key);
        Ok(self.get(idx))
    }

    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        let idx = self.partition_point(|x| &*x.end_key <= key);

        if idx == 0 {
            return Ok(self.first());
        }
        if idx == self.len() {
            let Some(last_block) = self.last() else {
                return Ok(None);
            };

            if &last_block.end_key < key {
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
    ) -> crate::Result<Option<u64>>;

    /// Gets the last block handle that may contain the given item
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<u64>>;

    /// Returns a handle to the last block
    fn get_last_block_handle(&self, cache_policy: CachePolicy) -> crate::Result<u64>;
}

#[allow(clippy::module_name_repetitions)]
pub trait RawBlockIndex {
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
    use crate::Slice;
    use test_log::test;

    fn bh<K: Into<Slice>>(end_key: K, offset: u64) -> KeyedBlockHandle {
        KeyedBlockHandle {
            end_key: end_key.into(),
            offset,
        }
    }

    #[test]
    fn block_handle_array_lowest() {
        let index = [
            bh(*b"c", 0),
            bh(*b"g", 10),
            bh(*b"g", 20),
            bh(*b"l", 30),
            bh(*b"t", 40),
        ];

        {
            let handle = index
                .get_lowest_block_containing_key(b"a", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 0);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"b", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 0);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"c", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 0);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"d", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"g");
            assert_eq!(handle.offset, 10);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"j", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"l");
            assert_eq!(handle.offset, 30);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"m", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"t");
            assert_eq!(handle.offset, 40);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"t", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"t");
            assert_eq!(handle.offset, 40);
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
            bh(*b"a", 0),
            bh(*b"a", 10),
            bh(*b"a", 20),
            bh(*b"a", 30),
            bh(*b"b", 40),
            bh(*b"b", 50),
            bh(*b"c", 60),
        ];

        {
            let handle = index
                .get_lowest_block_containing_key(b"0", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"a");
            assert_eq!(handle.offset, 0);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"a", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"a");
            assert_eq!(handle.offset, 0);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"ab", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, 40);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"b", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, 40);
        }

        {
            let handle = index
                .get_lowest_block_containing_key(b"c", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 60);
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
            bh(*b"a", 0),
            bh(*b"a", 10),
            bh(*b"a", 20),
            bh(*b"a", 30),
            bh(*b"b", 40),
            bh(*b"b", 50),
            bh(*b"c", 60),
        ];

        {
            let handle = index
                .get_last_block_containing_key(b"0", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"a");
            assert_eq!(handle.offset, 0);
        }

        {
            let handle = index
                .get_last_block_containing_key(b"a", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, 40);
        }

        {
            let handle = index
                .get_last_block_containing_key(b"ab", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"b");
            assert_eq!(handle.offset, 40);
        }

        {
            let handle = index
                .get_last_block_containing_key(b"b", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 60);
        }

        {
            let handle = index
                .get_last_block_containing_key(b"c", CachePolicy::Read)
                .expect("cannot fail")
                .expect("should exist");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 60);
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
            bh(*b"a", 0),
            bh(*b"a", 10),
            bh(*b"a", 20),
            bh(*b"a", 30),
            bh(*b"b", 40),
            bh(*b"b", 50),
            bh(*b"c", 60),
        ];

        {
            let handle = index
                .get_last_block_handle(CachePolicy::Read)
                .expect("cannot fail");

            assert_eq!(&*handle.end_key, *b"c");
            assert_eq!(handle.offset, 60);
        }
    }
}
