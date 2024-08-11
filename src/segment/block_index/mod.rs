// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block_handle;
pub mod top_level;
pub mod two_level_index;
pub mod writer;

use super::{block::Block, value_block::CachePolicy};
use block_handle::KeyedBlockHandle;

pub type IndexBlock = Block<KeyedBlockHandle>;

impl BlockIndex for [KeyedBlockHandle] {
    fn get_lowest_block_not_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        let idx = self.partition_point(|x| &*x.end_key <= key);
        Ok(self.get(idx + 1))
    }

    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        let idx = self.partition_point(|x| &*x.end_key < key);
        Ok(self.get(idx))
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<&KeyedBlockHandle> {
        // NOTE: Index is never empty
        #[allow(clippy::expect_used)]
        Ok(self.last().expect("index should not be empty"))
    }
}

pub trait BlockIndex {
    /// Gets the lowest block handle that may contain the given item
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>>;

    /// Returns a handle to the lowest block which definitely does not contain the given key
    fn get_lowest_block_not_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>>;

    /// Returns a handle to the last block
    fn get_last_block_handle(&self, cache_policy: CachePolicy) -> crate::Result<&KeyedBlockHandle>;
}
