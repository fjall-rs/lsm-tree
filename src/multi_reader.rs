// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::InternalValue;
use std::collections::VecDeque;

// TODO: remove?

/// Reads through a disjoint, sorted set of readers
pub struct MultiReader<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
    readers: VecDeque<I>,
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> MultiReader<I> {
    #[must_use]
    pub fn new(readers: VecDeque<I>) -> Self {
        Self { readers }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> Iterator for MultiReader<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.readers.front_mut()?.next() {
                return Some(item);
            }

            // NOTE: Current reader has no more items, load next reader if it exists and try again
            self.readers.pop_front();
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for MultiReader<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.readers.back_mut()?.next_back() {
                return Some(item);
            }

            // NOTE: Current reader has no more items, load next reader if it exists and try again
            self.readers.pop_back();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AbstractTree, SequenceNumberCounter, Slice};
    use test_log::test;

    // TODO: same test for prefix & ranges

    #[test]
    fn table_multi_reader_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(&tempdir, SequenceNumberCounter::default()).open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();

        #[expect(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<_> = VecDeque::new();

            for table in &tables {
                readers.push_back(table.iter());
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        }

        #[expect(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<_> = VecDeque::new();

            for table in &tables {
                readers.push_back(table.iter());
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.rev().flatten();

            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
        }

        #[expect(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<_> = VecDeque::new();

            for table in &tables {
                readers.push_back(table.iter());
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next_back().unwrap().key.user_key);
        }

        Ok(())
    }
}
