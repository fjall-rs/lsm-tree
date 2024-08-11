// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{block_handle::KeyedBlockHandle, BlockIndex};
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
    /// Creates a top-level block index
    #[must_use]
    pub fn from_boxed_slice(handles: Box<[KeyedBlockHandle]>) -> Self {
        Self(handles)
    }

    /// Loads a top-level index from disk
    pub fn from_file<P: AsRef<Path>>(path: P, offset: u64) -> crate::Result<Self> {
        let path = path.as_ref();
        log::trace!("reading TLI from {path:?}, offset={offset}");

        let mut file = File::open(path)?;

        let items = IndexBlock::from_file(&mut file, offset)?.items;
        log::trace!("loaded TLI ({path:?}): {items:?}");

        debug_assert!(!items.is_empty());

        Ok(Self::from_boxed_slice(items))
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

impl BlockIndex for TopLevelIndex {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        self.0
            .get_lowest_block_containing_key(key, CachePolicy::Read)
    }

    fn get_lowest_block_not_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<&KeyedBlockHandle>> {
        self.0
            .get_lowest_block_not_containing_key(key, CachePolicy::Read)
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<&KeyedBlockHandle> {
        self.0.get_last_block_handle(CachePolicy::Read)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::string_lit_as_bytes)]
mod tests {
    use super::super::BlockIndex;
    use super::*;
    use crate::Slice;
    use test_log::test;

    fn bh(start_key: Slice, offset: u64) -> KeyedBlockHandle {
        KeyedBlockHandle {
            end_key: start_key,
            offset,
        }
    }

    #[test]
    fn tli_get_last_block_handle() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("l".as_bytes().into(), 20),
            bh("t".as_bytes().into(), 30),
        ]));

        let handle = index
            .get_last_block_handle(CachePolicy::Read)
            .expect("cannot fail");
        assert_eq!(&*handle.end_key, "t".as_bytes());
    }

    #[test]

    fn tli_get_block_containing_key() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("c".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("g".as_bytes().into(), 20),
            bh("l".as_bytes().into(), 30),
            bh("t".as_bytes().into(), 40),
        ]));

        let handle = index
            .get_lowest_block_containing_key(b"a", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "c".as_bytes());

        let handle = index
            .get_lowest_block_containing_key(b"c", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "c".as_bytes());

        let handle = index
            .get_lowest_block_containing_key(b"f", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "g".as_bytes());
        assert_eq!(handle.offset, 10);

        let handle = index
            .get_lowest_block_containing_key(b"g", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "g".as_bytes());
        assert_eq!(handle.offset, 10);

        let handle = index
            .get_lowest_block_containing_key(b"h", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());
        assert_eq!(handle.offset, 30);

        let handle = index
            .get_lowest_block_containing_key(b"k", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());
        assert_eq!(handle.offset, 30);

        let handle = index
            .get_lowest_block_containing_key(b"p", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "t".as_bytes());

        let handle = index
            .get_lowest_block_containing_key(b"z", CachePolicy::Read)
            .expect("cannot fail");
        assert!(handle.is_none());
    }

    #[test]

    fn tli_get_block_not_containing_key() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("l".as_bytes().into(), 20),
            bh("t".as_bytes().into(), 30),
        ]));

        // NOTE: "t" is in the last block, so there can be no block after that
        assert!(index
            .get_lowest_block_not_containing_key(b"t", CachePolicy::Read)
            .expect("cannot fail")
            .is_none());

        let handle = index
            .get_lowest_block_not_containing_key(b"f", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());

        let handle = index
            .get_lowest_block_not_containing_key(b"k", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "t".as_bytes());

        // NOTE: "p" is in the last block, so there can be no block after that
        let handle = index
            .get_lowest_block_not_containing_key(b"p", CachePolicy::Read)
            .expect("cannot fail");
        assert!(handle.is_none());

        // NOTE: "z" is in the last block, so there can be no block after that
        assert!(index
            .get_lowest_block_not_containing_key(b"z", CachePolicy::Read)
            .expect("cannot fail")
            .is_none());
    }

    #[test]
    fn tli_spanning_multi() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("a".as_bytes().into(), 10),
            bh("a".as_bytes().into(), 20),
            bh("a".as_bytes().into(), 30),
            bh("b".as_bytes().into(), 40),
            bh("b".as_bytes().into(), 50),
            bh("c".as_bytes().into(), 60),
        ]));

        let handle = index
            .get_last_block_handle(CachePolicy::Read)
            .expect("cannot fail");
        assert_eq!(&*handle.end_key, "c".as_bytes());
        assert_eq!(handle.offset, 60);

        let handle = index
            .get_lowest_block_containing_key(b"a", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "a".as_bytes());
        assert_eq!(handle.offset, 0);

        let handle = index
            .get_lowest_block_containing_key(b"b", CachePolicy::Read)
            .expect("cannot fail")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "b".as_bytes());
        assert_eq!(handle.offset, 40);
    }
}
