// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{block_handle::KeyedBlockHandle, KeyedBlockIndex};
use crate::segment::{
    block_index::IndexBlock,
    value_block::{BlockOffset, CachePolicy},
};
use std::{fs::File, path::Path};

/// The top-level index (TLI) is the level-0 index in a partitioned (two-level) block index
///
/// See `top_level_index.rs` for more info.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct TopLevelIndex(Box<[KeyedBlockHandle]>);

impl TopLevelIndex {
    pub fn from_file<P: AsRef<Path>>(
        path: P,
        _: &crate::segment::meta::Metadata,
        tli_ptr: BlockOffset,
    ) -> crate::Result<Self> {
        let path = path.as_ref();

        log::trace!("reading TLI from {path:?} at tli_ptr={tli_ptr}");

        let mut file = File::open(path)?;
        let items = IndexBlock::from_file(&mut file, tli_ptr)?.items;

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
}

impl KeyedBlockIndex for TopLevelIndex {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        self.0
            .get_lowest_block_containing_key(key, CachePolicy::Read)
    }

    /// Gets the last block handle that may contain the given item
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        self.0.get_last_block_containing_key(key, cache_policy)
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<&KeyedBlockHandle> {
        self.0.get_last_block_handle(CachePolicy::Read)
    }
}
