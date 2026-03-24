// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Custom merge heap backed by a sorted vector.
//!
//! Supports both min and max extraction (for forward and reverse
//! iteration) on the same data structure, unlike two separate heaps.
//!
//! The key optimisation is `replace_min` / `replace_max`: replacing the
//! extremum and sliding the replacement into its sorted position.  In
//! the common case — a sequential scan where the same source keeps
//! winning — the replacement is still the extremum and the operation
//! completes in **one comparison** (O(1)).
//!
//! For the typical merge fan-in (n = 2–30 source iterators), a sorted
//! vector is competitive with a binary heap because:
//! - Cache-friendly sequential layout
//! - No tree-pointer overhead
//! - `memmove` of ≤30 entries is negligible

use crate::comparator::SharedComparator;
use crate::InternalValue;
use std::cmp::Ordering;

/// A single entry in the merge heap.
///
/// Comparator is stored once in the heap, not per entry — eliminating
/// the per-item `Arc` clone that the old `HeapItem` required.
pub struct HeapEntry {
    index: usize,
    value: InternalValue,
}

impl HeapEntry {
    pub fn new(index: usize, value: InternalValue) -> Self {
        Self { index, value }
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn into_value(self) -> InternalValue {
        self.value
    }

    /// Compares two heap entries using the given comparator.
    ///
    /// Ties (same user key + same seqno) are broken by source index,
    /// with lower indices sorting first.  This ensures deterministic
    /// merge order; callers that need "newer wins" semantics must pass
    /// sources in newest-first precedence order.
    #[inline]
    fn cmp_with(&self, other: &Self, cmp: &dyn crate::comparator::UserComparator) -> Ordering {
        self.value
            .key
            .compare_with(&other.value.key, cmp)
            .then_with(|| self.index.cmp(&other.index))
    }
}

// ---------------------------------------------------------------------------
// MergeHeap
// ---------------------------------------------------------------------------

/// Merge heap backed by a sorted vector.
///
/// Entries are stored in ascending order: `data[0]` is the minimum,
/// `data[last]` is the maximum.  This makes both `pop_min` / `pop_max`
/// and `replace_min` / `replace_max` straightforward.
pub struct MergeHeap {
    data: Vec<HeapEntry>,
    comparator: SharedComparator,
}

impl MergeHeap {
    /// Creates an empty heap pre-allocated for `cap` entries.
    pub fn with_capacity(cap: usize, comparator: SharedComparator) -> Self {
        Self {
            data: Vec::with_capacity(cap),
            comparator,
        }
    }

    #[inline]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a reference to the minimum (first) entry.
    #[inline]
    pub fn peek_min(&self) -> Option<&HeapEntry> {
        self.data.first()
    }

    /// Returns a reference to the maximum (last) entry.
    #[inline]
    pub fn peek_max(&self) -> Option<&HeapEntry> {
        self.data.last()
    }

    /// Inserts a new entry, maintaining sorted order.
    pub fn push(&mut self, entry: HeapEntry) {
        let cmp = self.comparator.as_ref();
        let pos = self
            .data
            .partition_point(|e| e.cmp_with(&entry, cmp) != Ordering::Greater);
        self.data.insert(pos, entry);
    }

    /// Removes and returns the minimum entry.
    pub fn pop_min(&mut self) -> Option<HeapEntry> {
        if self.data.is_empty() {
            return None;
        }
        Some(self.data.remove(0))
    }

    /// Removes and returns the maximum entry.
    pub fn pop_max(&mut self) -> Option<HeapEntry> {
        self.data.pop()
    }

    /// Replaces the minimum entry and slides the replacement into its
    /// sorted position.
    ///
    /// Returns the old minimum.  In the common case (replacement is
    /// still the minimum), this completes in **one comparison**.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked by debug_assert and loop guard"
    )]
    pub fn replace_min(&mut self, entry: HeapEntry) -> HeapEntry {
        debug_assert!(!self.data.is_empty());

        let old = std::mem::replace(&mut self.data[0], entry);

        // Slide right until in sorted position.
        let cmp = self.comparator.as_ref();
        let mut i = 0;
        while i + 1 < self.data.len()
            && self.data[i].cmp_with(&self.data[i + 1], cmp) == Ordering::Greater
        {
            self.data.swap(i, i + 1);
            i += 1;
        }

        old
    }

    /// Replaces the maximum entry and slides the replacement into its
    /// sorted position.
    ///
    /// Returns the old maximum.  In the common case (replacement is
    /// still the maximum), this completes in **one comparison**.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    #[expect(
        clippy::indexing_slicing,
        reason = "bounds checked by debug_assert and loop guard"
    )]
    pub fn replace_max(&mut self, entry: HeapEntry) -> HeapEntry {
        debug_assert!(!self.data.is_empty());

        let last = self.data.len() - 1;
        let old = std::mem::replace(&mut self.data[last], entry);

        // Slide left until in sorted position.
        let cmp = self.comparator.as_ref();
        let mut i = last;
        while i > 0 && self.data[i].cmp_with(&self.data[i - 1], cmp) == Ordering::Less {
            self.data.swap(i, i - 1);
            i -= 1;
        }

        old
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions use unwrap for brevity")]
mod tests {
    use super::*;
    use crate::comparator;
    use crate::ValueType::Value;
    use test_log::test;

    fn entry(key: &str, seqno: u64) -> HeapEntry {
        HeapEntry::new(0, InternalValue::from_components(key, b"", seqno, Value))
    }

    fn entry_src(key: &str, seqno: u64, src: usize) -> HeapEntry {
        HeapEntry::new(src, InternalValue::from_components(key, b"", seqno, Value))
    }

    #[test]
    fn min_ordering() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("c", 0));
        heap.push(entry("a", 0));
        heap.push(entry("d", 0));
        heap.push(entry("b", 0));

        let keys: Vec<_> = std::iter::from_fn(|| heap.pop_min())
            .map(|e| String::from_utf8_lossy(&e.value.key.user_key).to_string())
            .collect();
        assert_eq!(keys, ["a", "b", "c", "d"]);
    }

    #[test]
    fn max_ordering() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("c", 0));
        heap.push(entry("a", 0));
        heap.push(entry("d", 0));
        heap.push(entry("b", 0));

        let keys: Vec<_> = std::iter::from_fn(|| heap.pop_max())
            .map(|e| String::from_utf8_lossy(&e.value.key.user_key).to_string())
            .collect();
        assert_eq!(keys, ["d", "c", "b", "a"]);
    }

    #[test]
    fn replace_min_stays() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("a", 0));
        heap.push(entry("c", 0));
        heap.push(entry("d", 0));

        // Replace "a" with "b" — still the minimum.
        let old = heap.replace_min(entry("b", 0));
        assert_eq!(&*old.value.key.user_key, b"a");
        assert_eq!(&*heap.peek_min().unwrap().value.key.user_key, b"b");
    }

    #[test]
    fn replace_min_slides() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("a", 0));
        heap.push(entry("b", 0));
        heap.push(entry("c", 0));

        // Replace "a" with "z" — slides to end, "b" becomes min.
        let old = heap.replace_min(entry("z", 0));
        assert_eq!(&*old.value.key.user_key, b"a");
        assert_eq!(&*heap.peek_min().unwrap().value.key.user_key, b"b");
        assert_eq!(&*heap.peek_max().unwrap().value.key.user_key, b"z");
    }

    #[test]
    fn replace_max_stays() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("a", 0));
        heap.push(entry("b", 0));
        heap.push(entry("d", 0));

        // Replace "d" with "c" — still the maximum.
        let old = heap.replace_max(entry("c", 0));
        assert_eq!(&*old.value.key.user_key, b"d");
        assert_eq!(&*heap.peek_max().unwrap().value.key.user_key, b"c");
    }

    #[test]
    fn replace_max_slides() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("b", 0));
        heap.push(entry("c", 0));
        heap.push(entry("d", 0));

        // Replace "d" with "a" — slides to front.
        let old = heap.replace_max(entry("a", 0));
        assert_eq!(&*old.value.key.user_key, b"d");
        assert_eq!(&*heap.peek_min().unwrap().value.key.user_key, b"a");
    }

    #[test]
    fn seqno_tiebreak() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        // Same user key, different seqnos — higher seqno = "smaller".
        heap.push(entry("a", 1));
        heap.push(entry("a", 5));
        heap.push(entry("a", 3));

        let seqnos: Vec<_> = std::iter::from_fn(|| heap.pop_min())
            .map(|e| e.value.key.seqno)
            .collect();
        assert_eq!(seqnos, [5, 3, 1]);
    }

    #[test]
    fn source_index_tiebreak() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        // Same key + seqno: lower source index sorts first.
        heap.push(entry_src("k", 0, 2));
        heap.push(entry_src("k", 0, 0));
        heap.push(entry_src("k", 0, 1));

        let indices: Vec<_> = std::iter::from_fn(|| heap.pop_min())
            .map(|e| e.index)
            .collect();
        assert_eq!(indices, [0, 1, 2]);
    }

    #[test]
    fn mixed_min_max() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        heap.push(entry("a", 0));
        heap.push(entry("b", 0));
        heap.push(entry("c", 0));
        heap.push(entry("d", 0));

        // Interleave min and max pops.
        assert_eq!(&*heap.pop_min().unwrap().value.key.user_key, b"a");
        assert_eq!(&*heap.pop_max().unwrap().value.key.user_key, b"d");
        assert_eq!(&*heap.pop_min().unwrap().value.key.user_key, b"b");
        assert_eq!(&*heap.pop_max().unwrap().value.key.user_key, b"c");
        assert!(heap.is_empty());
    }

    #[test]
    fn replace_min_into_tie() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        // Source 0 has key "a", source 1 has key "b".
        heap.push(entry_src("a", 0, 0));
        heap.push(entry_src("b", 0, 1));

        // Replace source 0's "a" with "b" — now ties with source 1.
        // Source 0 (lower index) must still sort before source 1.
        let old = heap.replace_min(entry_src("b", 0, 0));
        assert_eq!(&*old.value.key.user_key, b"a");

        let first = heap.pop_min().unwrap();
        let second = heap.pop_min().unwrap();
        assert_eq!(first.index, 0, "lower source index wins on tie");
        assert_eq!(second.index, 1);
    }

    #[test]
    fn replace_max_into_tie() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(4, cmp);

        // Source 0 has key "a", source 1 has key "b".
        heap.push(entry_src("a", 0, 0));
        heap.push(entry_src("b", 0, 1));

        // Replace source 1's "b" with "a" — now ties with source 0.
        // Source 0 (lower index) must still sort first.
        let old = heap.replace_max(entry_src("a", 0, 1));
        assert_eq!(&*old.value.key.user_key, b"b");

        let first = heap.pop_min().unwrap();
        let second = heap.pop_min().unwrap();
        assert_eq!(first.index, 0, "lower source index wins on tie");
        assert_eq!(second.index, 1);
    }

    #[test]
    fn empty_heap() {
        let cmp = comparator::default_comparator();
        let heap = MergeHeap::with_capacity(0, cmp);
        assert!(heap.is_empty());
        assert!(heap.peek_min().is_none());
        assert!(heap.peek_max().is_none());
    }

    #[test]
    fn single_element() {
        let cmp = comparator::default_comparator();
        let mut heap = MergeHeap::with_capacity(1, cmp);
        heap.push(entry("x", 0));
        assert!(!heap.is_empty());

        let e = heap.pop_min().unwrap();
        assert_eq!(&*e.value.key.user_key, b"x");
        assert!(heap.is_empty());
    }
}
