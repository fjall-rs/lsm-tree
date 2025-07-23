// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::InternalValue;
use interval_heap::IntervalHeap as Heap;

type IterItem = crate::Result<InternalValue>;

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = IterItem> + 'a>;

#[derive(Eq)]
struct HeapItem(usize, InternalValue);

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.1.key == other.1.key
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.key.cmp(&other.1.key)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.1.key.cmp(&other.1.key))
    }
}

/// Merges multiple KV iterators
pub struct Merger<I> {
    iterators: Vec<I>,
    heap: Heap<HeapItem>,
    initialized_lo: bool,
    initialized_hi: bool,
}

impl<I: Iterator<Item = IterItem>> Merger<I> {
    #[must_use]
    pub fn new(iterators: Vec<I>) -> Self {
        let heap = Heap::with_capacity(iterators.len());

        let iterators = iterators.into_iter().collect::<Vec<_>>();

        Self {
            iterators,
            heap,
            initialized_lo: false,
            initialized_hi: false,
        }
    }

    #[allow(clippy::indexing_slicing)]
    fn initialize_lo(&mut self) -> crate::Result<()> {
        for idx in 0..self.iterators.len() {
            if let Some(item) = self.iterators[idx].next() {
                let item = item?;
                self.heap.push(HeapItem(idx, item));
            }
        }
        self.initialized_lo = true;
        Ok(())
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> Merger<I> {
    #[allow(clippy::indexing_slicing)]
    fn initialize_hi(&mut self) -> crate::Result<()> {
        for idx in 0..self.iterators.len() {
            if let Some(item) = self.iterators[idx].next_back() {
                let item = item?;
                self.heap.push(HeapItem(idx, item));
            }
        }
        self.initialized_hi = true;
        Ok(())
    }
}

impl<I: Iterator<Item = IterItem>> Iterator for Merger<I> {
    type Item = IterItem;

    #[allow(clippy::indexing_slicing)]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized_lo {
            fail_iter!(self.initialize_lo());
        }

        let min_item = self.heap.pop_min()?;

        if let Some(next_item) = self.iterators[min_item.0].next() {
            let next_item = fail_iter!(next_item);
            self.heap.push(HeapItem(min_item.0, next_item));
        }

        Some(Ok(min_item.1))
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> DoubleEndedIterator for Merger<I> {
    #[allow(clippy::indexing_slicing)]
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.initialized_hi {
            fail_iter!(self.initialize_hi());
        }

        let max_item = self.heap.pop_max()?;

        if let Some(next_item) = self.iterators[max_item.0].next_back() {
            let next_item = fail_iter!(next_item);
            self.heap.push(HeapItem(max_item.0, next_item));
        }

        Some(Ok(max_item.1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueType::Value;
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn merge_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];
        #[rustfmt::skip]
        let b = vec![
            Ok(InternalValue::from_components("b", b"", 0, Value)),
        ];

        let mut iter = Merger::new(vec![a.into_iter(), b.into_iter()]);

        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("a", b"", 0, Value),
        );
        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("b", b"", 0, Value),
        );
        assert!(iter.next().is_none(), "iter should be closed");

        Ok(())
    }

    #[test]
    #[ignore]
    #[allow(clippy::unwrap_used)]
    fn merge_dup() -> crate::Result<()> {
        #[rustfmt::skip]
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];
        #[rustfmt::skip]
        let b = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];

        let mut iter = Merger::new(vec![a.into_iter(), b.into_iter()]);

        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("a", b"", 0, Value),
        );
        assert!(iter.next().is_none(), "iter should be closed");

        Ok(())
    }
}
