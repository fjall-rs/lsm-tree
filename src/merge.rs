// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::comparator::SharedComparator;
use crate::heap::{HeapEntry, MergeHeap};
use crate::InternalValue;

type IterItem = crate::Result<InternalValue>;

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = IterItem> + Send + 'a>;

/// Merges multiple KV iterators into a single sorted stream.
///
/// Uses a custom sorted-vector heap with `replace_min` / `replace_max`
/// to avoid the double O(log n) cost of `pop` + `push` in the hot
/// path.  The comparator is stored once in the heap, not per entry —
/// eliminating per-item `Arc` ref-count traffic.
///
/// When two entries have the same user key and sequence number, the
/// entry from the iterator with the **lower index** (earlier position
/// in the `iterators` vec) sorts first.  Callers that need "newer
/// wins" semantics (e.g. compaction) must pass sources in
/// newest-first order.
pub struct Merger<I> {
    iterators: Vec<I>,
    heap: MergeHeap,
    initialized_lo: bool,
    initialized_hi: bool,
}

impl<I: Iterator<Item = IterItem>> Merger<I> {
    #[must_use]
    pub fn new(iterators: Vec<I>, comparator: SharedComparator) -> Self {
        // 2× capacity: mixed forward+reverse can buffer up to 2 entries per source.
        let heap = MergeHeap::with_capacity(2 * iterators.len(), comparator);

        Self {
            iterators,
            heap,
            initialized_lo: false,
            initialized_hi: false,
        }
    }

    fn initialize_lo(&mut self) -> crate::Result<()> {
        for (idx, it) in self.iterators.iter_mut().enumerate() {
            if let Some(item) = it.next() {
                let item = item?;
                self.heap.push(HeapEntry::new(idx, item));
            }
        }
        self.initialized_lo = true;
        Ok(())
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> Merger<I> {
    fn initialize_hi(&mut self) -> crate::Result<()> {
        for (idx, it) in self.iterators.iter_mut().enumerate() {
            if let Some(item) = it.next_back() {
                let item = item?;
                self.heap.push(HeapEntry::new(idx, item));
            }
        }
        self.initialized_hi = true;
        Ok(())
    }
}

impl<I: Iterator<Item = IterItem>> Iterator for Merger<I> {
    type Item = IterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized_lo {
            fail_iter!(self.initialize_lo());
        }

        // Read the source index of the current minimum (borrow ends
        // at the semicolon, so we can mutably borrow iterators next).
        let top_index = self.heap.peek_min()?.index();

        #[expect(clippy::indexing_slicing, reason = "we trust the HeapEntry index")]
        if let Some(next_result) = self.iterators[top_index].next() {
            match next_result {
                Ok(next_value) => {
                    // Replace the min in-place and slide into position.
                    // Common case (same source still wins): 1 comparison.
                    let old = self.heap.replace_min(HeapEntry::new(top_index, next_value));
                    Some(Ok(old.into_value()))
                }
                Err(e) => {
                    // Pop the stale entry so the next call makes progress
                    // on a different source instead of retrying this one.
                    let _ = self.heap.pop_min();
                    Some(Err(e))
                }
            }
        } else {
            // Source iterator exhausted — just remove.
            let old = self.heap.pop_min()?;
            Some(Ok(old.into_value()))
        }
    }
}

impl<I: DoubleEndedIterator<Item = IterItem>> DoubleEndedIterator for Merger<I> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.initialized_hi {
            fail_iter!(self.initialize_hi());
        }

        let top_index = self.heap.peek_max()?.index();

        #[expect(clippy::indexing_slicing, reason = "we trust the HeapEntry index")]
        if let Some(next_result) = self.iterators[top_index].next_back() {
            match next_result {
                Ok(next_value) => {
                    let old = self.heap.replace_max(HeapEntry::new(top_index, next_value));
                    Some(Ok(old.into_value()))
                }
                Err(e) => {
                    let _ = self.heap.pop_max();
                    Some(Err(e))
                }
            }
        } else {
            let old = self.heap.pop_max()?;
            Some(Ok(old.into_value()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator;
    use crate::ValueType::Value;
    use test_log::test;

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertions")]
    fn merge_simple() -> crate::Result<()> {
        #[rustfmt::skip]
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];
        #[rustfmt::skip]
        let b = vec![
            Ok(InternalValue::from_components("b", b"", 0, Value)),
        ];

        let mut iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

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
    #[ignore = "maybe not needed"]
    #[expect(clippy::unwrap_used, reason = "test assertions")]
    fn merge_dup() -> crate::Result<()> {
        #[rustfmt::skip]
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];
        #[rustfmt::skip]
        let b = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
        ];

        let mut iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

        assert_eq!(
            iter.next().unwrap()?,
            InternalValue::from_components("a", b"", 0, Value),
        );
        assert!(iter.next().is_none(), "iter should be closed");

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertions")]
    fn merge_interleaved() -> crate::Result<()> {
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
            Ok(InternalValue::from_components("c", b"", 0, Value)),
            Ok(InternalValue::from_components("e", b"", 0, Value)),
        ];
        let b = vec![
            Ok(InternalValue::from_components("b", b"", 0, Value)),
            Ok(InternalValue::from_components("d", b"", 0, Value)),
        ];

        let iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

        let keys: Vec<String> = iter
            .map(|r| {
                let v = r.unwrap();
                String::from_utf8_lossy(&v.key.user_key).to_string()
            })
            .collect();
        assert_eq!(keys, ["a", "b", "c", "d", "e"]);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertions")]
    fn merge_many_sources() -> crate::Result<()> {
        let sources: Vec<Vec<IterItem>> = (0..8)
            .map(|i| {
                vec![Ok(InternalValue::from_components(
                    format!("{}", (b'a' + i) as char),
                    b"",
                    0,
                    Value,
                ))]
            })
            .collect();

        let iter = Merger::new(
            sources.into_iter().map(|s| s.into_iter()).collect(),
            comparator::default_comparator(),
        );

        let keys: Vec<String> = iter
            .map(|r| {
                let v = r.unwrap();
                String::from_utf8_lossy(&v.key.user_key).to_string()
            })
            .collect();
        assert_eq!(keys, ["a", "b", "c", "d", "e", "f", "g", "h"]);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertions")]
    fn merge_seqno_ordering() -> crate::Result<()> {
        // Same key, different seqnos — higher seqno must come first.
        let a = vec![Ok(InternalValue::from_components("k", b"v1", 3, Value))];
        let b = vec![Ok(InternalValue::from_components("k", b"v2", 7, Value))];
        let c = vec![Ok(InternalValue::from_components("k", b"v3", 1, Value))];

        let iter = Merger::new(
            vec![a.into_iter(), b.into_iter(), c.into_iter()],
            comparator::default_comparator(),
        );

        let seqnos: Vec<u64> = iter.map(|r| r.unwrap().key.seqno).collect();
        assert_eq!(seqnos, [7, 3, 1]);

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used, reason = "test assertions")]
    fn merge_mixed_direction() -> crate::Result<()> {
        // Two sources with non-overlapping keys: a,c,e and b,d,f.
        // Interleave next() and next_back() to exercise shared heap state.
        let a = vec![
            Ok(InternalValue::from_components("a", b"", 0, Value)),
            Ok(InternalValue::from_components("c", b"", 0, Value)),
            Ok(InternalValue::from_components("e", b"", 0, Value)),
        ];
        let b = vec![
            Ok(InternalValue::from_components("b", b"", 0, Value)),
            Ok(InternalValue::from_components("d", b"", 0, Value)),
            Ok(InternalValue::from_components("f", b"", 0, Value)),
        ];

        let mut iter = Merger::new(
            vec![a.into_iter(), b.into_iter()],
            comparator::default_comparator(),
        );

        // Consume from both ends, meeting in the middle.
        let k = |v: InternalValue| String::from_utf8_lossy(&v.key.user_key).to_string();

        assert_eq!(k(iter.next().unwrap()?), "a");
        assert_eq!(k(iter.next_back().unwrap()?), "f");
        assert_eq!(k(iter.next().unwrap()?), "b");
        assert_eq!(k(iter.next_back().unwrap()?), "e");
        assert_eq!(k(iter.next().unwrap()?), "c");
        assert_eq!(k(iter.next_back().unwrap()?), "d");
        assert!(iter.next().is_none(), "should be exhausted");

        Ok(())
    }
}
