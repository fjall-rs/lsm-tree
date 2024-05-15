use super::block_handle::KeyedBlockHandle;
use crate::segment::block_index::IndexBlock;
use std::{fs::File, io::BufReader, path::Path};

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
pub struct TopLevelIndex {
    pub data: Box<[KeyedBlockHandle]>,
}

impl TopLevelIndex {
    /// Creates a top-level block index
    #[must_use]
    pub fn from_boxed_slice(data: Box<[KeyedBlockHandle]>) -> Self {
        Self { data }
    }

    /// Loads a top-level index from disk
    pub fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        let items =
            IndexBlock::from_file_compressed(&mut BufReader::new(File::open(path)?), 0)?.items;

        log::trace!("loaded TLI ({path:?}): {items:#?}");

        debug_assert!(!items.is_empty());

        Ok(Self::from_boxed_slice(items))
    }

    /// Returns a handle to the first index block that is not covered by the given prefix anymore
    pub(crate) fn get_prefix_upper_bound(&self, prefix: &[u8]) -> Option<&KeyedBlockHandle> {
        let start_idx = self.data.partition_point(|x| &*x.end_key <= prefix);

        for idx in start_idx.. {
            let handle = self.data.get(idx)?;

            if !handle.end_key.starts_with(prefix) {
                return self.data.get(idx + 1);
            }
        }

        None
    }

    // TODO: these methods work using a slice of KeyedBlockHandles
    // IndexBlocks are also a slice of KeyedBlockHandles
    // ... see where I'm getting at...?

    /// Returns a handle to the lowest index block which definitely does not contain the given key
    #[must_use]
    pub fn get_lowest_block_not_containing_key(&self, key: &[u8]) -> Option<&KeyedBlockHandle> {
        let idx = self.data.partition_point(|x| &*x.end_key <= key);
        self.data.get(idx + 1)
    }

    /// Returns a handle to the index block which should contain an item with a given key
    #[must_use]
    pub fn get_lowest_block_containing_key(&self, key: &[u8]) -> Option<&KeyedBlockHandle> {
        let idx = self.data.partition_point(|x| &*x.end_key < key);

        let block = self.data.get(idx)?;

        if key > &*block.end_key {
            None
        } else {
            Some(block)
        }
    }

    /// Returns a handle to the first index block
    #[must_use]
    pub fn get_first_block_handle(&self) -> &KeyedBlockHandle {
        // NOTE: Index is never empty
        #[allow(clippy::expect_used)]
        self.data.iter().next().expect("index should not be empty")
    }

    /// Returns a handle to the last index block
    #[must_use]
    pub fn get_last_block_handle(&self) -> &KeyedBlockHandle {
        // NOTE: Index is never empty
        #[allow(clippy::expect_used)]
        self.data
            .iter()
            .next_back()
            .expect("index should not be empty")
    }

    /// Returns a handle to the index block before the input block, if it exists, or None
    #[must_use]
    pub fn get_prev_block_handle(&self, offset: u64) -> Option<&KeyedBlockHandle> {
        let idx = self.data.partition_point(|x| x.offset < offset);

        if idx == 0 {
            None
        } else {
            self.data.get(idx - 1)
        }
    }

    /// Returns a handle to the index block after the input block, if it exists, or None
    #[must_use]
    pub fn get_next_block_handle(&self, offset: u64) -> Option<&KeyedBlockHandle> {
        let idx = self.data.partition_point(|x| x.offset <= offset);
        self.data.get(idx)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::string_lit_as_bytes)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use test_log::test;

    fn bh(start_key: Arc<[u8]>, offset: u64) -> KeyedBlockHandle {
        KeyedBlockHandle {
            end_key: start_key,
            offset,
        }
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn tli_get_next_block_handle() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("l".as_bytes().into(), 20),
            bh("t".as_bytes().into(), 30),
        ]));

        let handle = index
            .get_next_block_handle(/* "g" */ 10)
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());

        let result_without_next = index.get_next_block_handle(/* "t" */ 30);
        assert!(result_without_next.is_none());
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn tli_get_prev_block_handle() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("l".as_bytes().into(), 20),
            bh("t".as_bytes().into(), 30),
        ]));

        let handle = index
            .get_prev_block_handle(/* "l" */ 20)
            .expect("should exist");
        assert_eq!(&*handle.end_key, "g".as_bytes());

        let prev_result = index.get_prev_block_handle(/* "a" */ 0);
        assert!(prev_result.is_none());
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    fn tli_get_prev_block_handle_2() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("g".as_bytes().into(), 20),
            bh("l".as_bytes().into(), 30),
            bh("t".as_bytes().into(), 40),
        ]));

        let handle = index
            .get_prev_block_handle(/* "l" */ 30)
            .expect("should exist");
        assert_eq!(&*handle.end_key, "g".as_bytes());
        assert_eq!(handle.offset, 20);

        let prev_result = index.get_prev_block_handle(/* "a" */ 0);
        assert!(prev_result.is_none());
    }

    #[test]
    fn tli_get_first_block_handle() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("l".as_bytes().into(), 20),
            bh("t".as_bytes().into(), 30),
        ]));

        let handle = index.get_first_block_handle();
        assert_eq!(&*handle.end_key, "a".as_bytes());
    }

    #[test]
    fn tli_get_last_block_handle() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("g".as_bytes().into(), 10),
            bh("l".as_bytes().into(), 20),
            bh("t".as_bytes().into(), 30),
        ]));

        let handle = index.get_last_block_handle();
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
            .get_lowest_block_containing_key(b"a")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "c".as_bytes());

        let handle = index
            .get_lowest_block_containing_key(b"c")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "c".as_bytes());

        let handle = index
            .get_lowest_block_containing_key(b"f")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "g".as_bytes());
        assert_eq!(handle.offset, 10);

        let handle = index
            .get_lowest_block_containing_key(b"g")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "g".as_bytes());
        assert_eq!(handle.offset, 10);

        let handle = index
            .get_lowest_block_containing_key(b"h")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());
        assert_eq!(handle.offset, 30);

        let handle = index
            .get_lowest_block_containing_key(b"k")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());
        assert_eq!(handle.offset, 30);

        let handle = index
            .get_lowest_block_containing_key(b"p")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "t".as_bytes());

        let handle = index.get_lowest_block_containing_key(b"z");
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
        assert!(index.get_lowest_block_not_containing_key(b"t").is_none());

        let handle = index
            .get_lowest_block_not_containing_key(b"f")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "l".as_bytes());

        let handle = index
            .get_lowest_block_not_containing_key(b"k")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "t".as_bytes());

        // NOTE: "p" is in the last block, so there can be no block after that
        let handle = index.get_lowest_block_not_containing_key(b"p");
        assert!(handle.is_none());

        // NOTE: "z" is in the last block, so there can be no block after that
        assert!(index.get_lowest_block_not_containing_key(b"z").is_none());
    }

    #[test]

    fn tli_get_prefix_upper_bound() {
        let index = TopLevelIndex::from_boxed_slice(Box::new([
            bh("a".as_bytes().into(), 0),
            bh("abc".as_bytes().into(), 10),
            bh("abcabc".as_bytes().into(), 20),
            bh("abcabcabc".as_bytes().into(), 30),
            bh("abcysw".as_bytes().into(), 40),
            bh("basd".as_bytes().into(), 50),
            bh("cxy".as_bytes().into(), 70),
            bh("ewqeqw".as_bytes().into(), 60),
        ]));

        let handle = index.get_prefix_upper_bound(b"a").expect("should exist");
        assert_eq!(&*handle.end_key, "cxy".as_bytes());

        let handle = index.get_prefix_upper_bound(b"abc").expect("should exist");
        assert_eq!(&*handle.end_key, "cxy".as_bytes());

        let handle = index.get_prefix_upper_bound(b"basd").expect("should exist");
        assert_eq!(&*handle.end_key, "ewqeqw".as_bytes());

        let result = index.get_prefix_upper_bound(b"cxy");
        assert!(result.is_none());

        let result = index.get_prefix_upper_bound(b"ewqeqw");
        assert!(result.is_none());
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

        {
            let handle = index.get_prefix_upper_bound(b"a").expect("should exist");
            assert_eq!(&*handle.end_key, "b".as_bytes());
            assert_eq!(handle.offset, 50);
        }

        {
            let handle = index.get_first_block_handle();
            assert_eq!(&*handle.end_key, "a".as_bytes());
            assert_eq!(handle.offset, 0);

            let handle = index
                .get_next_block_handle(handle.offset)
                .expect("should exist");
            assert_eq!(&*handle.end_key, "a".as_bytes());
            assert_eq!(handle.offset, 10);

            let handle = index
                .get_next_block_handle(handle.offset)
                .expect("should exist");
            assert_eq!(&*handle.end_key, "a".as_bytes());
            assert_eq!(handle.offset, 20);

            let handle = index
                .get_next_block_handle(handle.offset)
                .expect("should exist");
            assert_eq!(&*handle.end_key, "a".as_bytes());
            assert_eq!(handle.offset, 30);

            let handle = index
                .get_next_block_handle(handle.offset)
                .expect("should exist");
            assert_eq!(&*handle.end_key, "b".as_bytes());
            assert_eq!(handle.offset, 40);

            let handle = index
                .get_next_block_handle(handle.offset)
                .expect("should exist");
            assert_eq!(&*handle.end_key, "b".as_bytes());
            assert_eq!(handle.offset, 50);

            let handle = index
                .get_next_block_handle(handle.offset)
                .expect("should exist");
            assert_eq!(&*handle.end_key, "c".as_bytes());
            assert_eq!(handle.offset, 60);

            let handle = index.get_next_block_handle(handle.offset);
            assert!(handle.is_none());
        }

        {
            let handle = index.get_last_block_handle();
            assert_eq!(&*handle.end_key, "c".as_bytes());
            assert_eq!(handle.offset, 60);
        }

        let handle = index
            .get_lowest_block_containing_key(b"a")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "a".as_bytes());
        assert_eq!(handle.offset, 0);

        let handle = index
            .get_lowest_block_containing_key(b"b")
            .expect("should exist");
        assert_eq!(&*handle.end_key, "b".as_bytes());
        assert_eq!(handle.offset, 40);
    }
}
