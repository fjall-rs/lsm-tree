// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod iter;

use super::{CachePolicy, IndexBlock, KeyedBlockHandle};
use crate::segment::block::ParsedItem;

#[enum_dispatch::enum_dispatch]
pub trait BlockIndex {
    /// Gets the lowest block handle that can possibly contain the given item.
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>>; // TODO: return BlockHandle (::into_non_keyed)

    /// Gets the last block handle that can possibly contain the given item.
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>>;

    /// Returns a handle to the last block.
    fn get_last_block_handle(&self, cache_policy: CachePolicy) -> crate::Result<KeyedBlockHandle>;
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
// #[enum_dispatch::enum_dispatch(BlockIndex)]
#[allow(clippy::module_name_repetitions)]
pub enum BlockIndexImpl {
    Full(FullBlockIndex),
    VolatileFull,
    // TwoLevel(TwoLevelBlockIndex),
}

/// Index that translates item keys to data block handles
///
/// The index is fully loaded into memory.
pub struct FullBlockIndex(IndexBlock);

impl FullBlockIndex {
    pub fn new(block: IndexBlock) -> Self {
        Self(block)
    }

    pub fn forward_reader(
        &self,
        needle: &[u8],
    ) -> Option<impl Iterator<Item = KeyedBlockHandle> + '_> {
        let mut iter = self.0.iter();

        if iter.seek(needle) {
            Some(iter.map(|x| x.materialize(&self.0.inner.data)))
        } else {
            None
        }
    }

    pub fn inner(&self) -> &IndexBlock {
        &self.0
    }
}

/* impl BlockIndex for FullBlockIndex {
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        Ok(self.0.get_highest_possible_block(key))
    }

    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<KeyedBlockHandle>> {
        Ok(self.0.get_lowest_possible_block(key))
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<KeyedBlockHandle> {
        todo!()
    }
} */

/* impl std::ops::Deref for FullBlockIndex {
    type Target = Box<[KeyedBlockHandle]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
} */

/* impl FullBlockIndex {
  /*   pub fn from_file(
        path: &Path,
        metadata: &crate::segment::meta::Metadata,
        offsets: &crate::segment::file_offsets::FileOffsets,
    ) -> crate::Result<Self> {
        todo!()
        /* let cnt = metadata.index_block_count as usize;

        log::trace!(
            "reading full block index from {path:?} at idx_ptr={} ({cnt} index blocks)",
            offsets.index_block_ptr,
        );

        let mut file = File::open(path)?;
        file.seek(std::io::SeekFrom::Start(*offsets.index_block_ptr))?;

        let mut block_handles = Vec::with_capacity(cnt);

        for _ in 0..cnt {
            let idx_block = IndexBlock::from_reader(&mut file)?.items;
            // TODO: 1.80? IntoIter impl for Box<[T]>
            block_handles.extend(idx_block.into_vec());
        }

        debug_assert!(!block_handles.is_empty());

        Ok(Self(block_handles.into_boxed_slice())) */
    } */
} */

/* impl BlockIndex for FullBlockIndex {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        use super::KeyedBlockIndex;

        self.0
            .get_lowest_block_containing_key(key, CachePolicy::Read)
            .map(|x| x.map(|x| x.offset))
    }

    /// Gets the last block handle that may contain the given item
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        use super::KeyedBlockIndex;

        self.0
            .get_last_block_containing_key(key, cache_policy)
            .map(|x| x.map(|x| x.offset))
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<BlockOffset> {
        use super::KeyedBlockIndex;

        self.0
            .get_last_block_handle(CachePolicy::Read)
            .map(|x| x.offset)
    }
} */
