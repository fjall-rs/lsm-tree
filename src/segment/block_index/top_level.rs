// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{block_handle::KeyedBlockHandle, RawBlockIndex};
use crate::segment::{block_index::IndexBlock, value_block::CachePolicy};
use std::{fs::File, path::Path};

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
#[derive(Debug)]
pub struct TopLevelIndex(Box<[KeyedBlockHandle]>);

impl TopLevelIndex {
    pub fn from_file<P: AsRef<Path>>(
        path: P,
        _: &crate::segment::meta::Metadata,
        offsets: &crate::segment::file_offsets::FileOffsets,
    ) -> crate::Result<Self> {
        let path = path.as_ref();

        log::trace!("reading TLI from {path:?} at tli_ptr={}", offsets.tli_ptr);

        let mut file = File::open(path)?;
        let items = IndexBlock::from_file(&mut file, offsets.tli_ptr)?.items;

        log::trace!("loaded TLI ({path:?}): {items:?}");
        debug_assert!(!items.is_empty());

        Ok(Self::from_boxed_slice(items))
    }

    /// Creates a top-level block index
    #[must_use]
    pub fn from_boxed_slice(handles: Box<[KeyedBlockHandle]>) -> Self {
        Self(handles)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = &KeyedBlockHandle> {
        self.0.iter()
    }

    pub fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        self.0
            .get_lowest_block_containing_key(key, CachePolicy::Read)
    }

    /// Gets the last block handle that may contain the given item
    pub fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        self.0.get_last_block_containing_key(key, cache_policy)
    }

    pub fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<&KeyedBlockHandle> {
        self.0.get_last_block_handle(CachePolicy::Read)
    }
}
