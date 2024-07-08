use crate::Value;
use std::collections::VecDeque;

/// Reads through a disjoint, sorted set of segment readers
pub struct MultiReader<I: DoubleEndedIterator<Item = crate::Result<Value>>> {
    readers: VecDeque<I>,
}

impl<I: DoubleEndedIterator<Item = crate::Result<Value>>> MultiReader<I> {
    #[must_use]
    pub fn new(readers: VecDeque<I>) -> Self {
        Self { readers }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<Value>>> Iterator for MultiReader<I> {
    type Item = crate::Result<Value>;

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

impl<I: DoubleEndedIterator<Item = crate::Result<Value>>> DoubleEndedIterator for MultiReader<I> {
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
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use test_log::test;

    // TODO: same test for prefix & ranges

    #[test]
    fn segment_multi_reader_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(&tempdir).open()?;

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
            tree.flush_active_memtable()?;
        }

        let segments = tree
            .levels
            .read()
            .expect("lock is poisoned")
            .iter()
            .collect::<Vec<_>>();

        #[allow(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<_> = VecDeque::new();

            for segment in &segments {
                readers.push_back(segment.iter());
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.flatten();

            assert_eq!(Arc::from(*b"a"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"b"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"c"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"d"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"e"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"f"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"g"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"h"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"i"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"j"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"k"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"l"), iter.next().unwrap().key);
        }

        #[allow(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<_> = VecDeque::new();

            for segment in &segments {
                readers.push_back(segment.iter());
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.rev().flatten();

            assert_eq!(Arc::from(*b"l"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"k"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"j"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"i"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"h"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"g"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"f"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"e"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"d"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"c"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"b"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"a"), iter.next().unwrap().key);
        }

        #[allow(clippy::unwrap_used)]
        {
            let mut readers: VecDeque<_> = VecDeque::new();

            for segment in &segments {
                readers.push_back(segment.iter());
            }

            let multi_reader = MultiReader::new(readers);

            let mut iter = multi_reader.flatten();

            assert_eq!(Arc::from(*b"a"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"l"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"b"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"k"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"c"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"j"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"d"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"i"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"e"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"h"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"f"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"g"), iter.next_back().unwrap().key);
        }

        Ok(())
    }
}
