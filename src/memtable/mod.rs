// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod interval_tree;

use crate::key::InternalKey;
use crate::range_tombstone::{CoveringRt, RangeTombstone};
use crate::{
    value::{InternalValue, SeqNo, UserValue},
    UserKey, ValueType,
};
use crossbeam_skiplist::SkipMap;
use interval_tree::IntervalTree;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::RwLock;

pub use crate::tree::inner::MemtableId;

/// The memtable serves as an intermediary, ephemeral, sorted storage for new items
///
/// When the Memtable exceeds some size, it should be flushed to a table.
pub struct Memtable {
    #[doc(hidden)]
    pub id: MemtableId,

    /// The actual content, stored in a lock-free skiplist.
    #[doc(hidden)]
    pub items: SkipMap<InternalKey, UserValue>,

    /// Approximate active memtable size.
    ///
    /// If this grows too large, a flush is triggered.
    pub(crate) approximate_size: AtomicU64,

    /// Highest encountered sequence number.
    ///
    /// This is used so that `get_highest_seqno` has O(1) complexity.
    pub(crate) highest_seqno: AtomicU64,

    pub(crate) requested_rotation: AtomicBool,

    /// Range tombstones indexed by start for efficient point queries
    /// and overlap collection (used for forward scans and seek init).
    #[doc(hidden)]
    pub range_tombstones: RwLock<IntervalTree>,

    /// Range tombstones indexed by `(Reverse(end), Reverse(seqno))` for
    /// reverse iteration. Yields tombstones in end-desc order.
    pub(crate) tombstones_by_end: RwLock<BTreeMap<(Reverse<UserKey>, Reverse<SeqNo>), RangeTombstone>>,
}

impl Memtable {
    /// Returns the memtable ID.
    pub fn id(&self) -> MemtableId {
        self.id
    }

    /// Returns `true` if the memtable was already flagged for rotation.
    pub fn is_flagged_for_rotation(&self) -> bool {
        self.requested_rotation
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Flags the memtable as requested for rotation.
    pub fn flag_rotated(&self) {
        self.requested_rotation
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new(id: MemtableId) -> Self {
        Self {
            id,
            items: SkipMap::default(),
            approximate_size: AtomicU64::default(),
            highest_seqno: AtomicU64::default(),
            requested_rotation: AtomicBool::default(),
            range_tombstones: RwLock::new(IntervalTree::new()),
            tombstones_by_end: RwLock::new(BTreeMap::new()),
        }
    }

    /// Creates an iterator over all items.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        self.items.iter().map(|entry| InternalValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Creates an iterator over a range of items.
    pub(crate) fn range<'a, R: RangeBounds<InternalKey> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + 'a {
        self.items.range(range).map(|entry| InternalValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Returns the item by key if it exists.
    ///
    /// The item with the highest seqno will be returned, if `seqno` is None.
    #[doc(hidden)]
    pub fn get(&self, key: &[u8], seqno: SeqNo) -> Option<InternalValue> {
        if seqno == 0 {
            return None;
        }

        // NOTE: This range start deserves some explanation...
        // InternalKeys are multi-sorted by 2 categories: user_key and Reverse(seqno). (tombstone doesn't really matter)
        // We search for the lowest entry that is greater or equal the user's prefix key
        // and has the seqno (or lower) we want  (because the seqno is stored in reverse order)
        //
        // Example: We search for "abc"
        //
        // key -> seqno
        //
        // a   -> 7
        // abc -> 5 <<< This is the lowest key (highest seqno) that matches the key with seqno=None
        // abc -> 4
        // abc -> 3 <<< If searching for abc and seqno=4, we would get this
        // abcdef -> 6
        // abcdef -> 5
        //
        let lower_bound = InternalKey::new(key, seqno - 1, ValueType::Value);

        let mut iter = self
            .items
            .range(lower_bound..)
            .take_while(|entry| &*entry.key().user_key == key);

        iter.next().map(|entry| InternalValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Gets approximate size of memtable in bytes.
    pub fn size(&self) -> u64 {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Counts the number of items in the memtable.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` if the memtable is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Inserts an item into the memtable
    #[doc(hidden)]
    pub fn insert(&self, item: InternalValue) -> (u64, u64) {
        #[expect(
            clippy::expect_used,
            reason = "keys are limited to 16-bit length + values are limited to 32-bit length"
        )]
        let item_size =
            (item.key.user_key.len() + item.value.len() + std::mem::size_of::<InternalValue>())
                .try_into()
                .expect("should fit into u64");

        let size_before = self
            .approximate_size
            .fetch_add(item_size, std::sync::atomic::Ordering::AcqRel);

        let key = InternalKey::new(item.key.user_key, item.key.seqno, item.key.value_type);
        self.items.insert(key, item.value);

        self.highest_seqno
            .fetch_max(item.key.seqno, std::sync::atomic::Ordering::AcqRel);

        (item_size, size_before + item_size)
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_highest_seqno(&self) -> Option<SeqNo> {
        if self.is_empty() && self.range_tombstone_count() == 0 {
            None
        } else {
            Some(
                self.highest_seqno
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        }
    }

    /// Inserts a range tombstone into the memtable.
    ///
    /// Updates both the interval tree (for point queries / seek init) and the
    /// end-desc index (for reverse iteration).
    #[doc(hidden)]
    pub fn insert_range_tombstone(&self, rt: RangeTombstone) {
        let size_contribution = (rt.start.len() + rt.end.len() + 8 + 64) as u64;

        self.approximate_size
            .fetch_add(size_contribution, std::sync::atomic::Ordering::AcqRel);

        self.highest_seqno
            .fetch_max(rt.seqno, std::sync::atomic::Ordering::AcqRel);

        {
            #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
            let mut tree = self.range_tombstones.write().expect("lock poisoned");
            tree.insert(rt.clone());
        }

        {
            #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
            let mut by_end = self.tombstones_by_end.write().expect("lock poisoned");
            by_end.insert(
                (Reverse(rt.end.clone()), Reverse(rt.seqno)),
                rt,
            );
        }
    }

    /// Returns `true` if the given key at the given seqno is suppressed
    /// by a range tombstone visible at `read_seqno`.
    pub fn is_suppressed_by_range_tombstone(
        &self,
        key: &[u8],
        key_seqno: SeqNo,
        read_seqno: SeqNo,
    ) -> bool {
        #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
        let tree = self.range_tombstones.read().expect("lock poisoned");
        tree.query_suppression(key, key_seqno, read_seqno)
    }

    /// Returns all range tombstones overlapping with `key` and visible at `read_seqno`.
    ///
    /// Used for seek initialization.
    pub fn overlapping_tombstones(
        &self,
        key: &[u8],
        read_seqno: SeqNo,
    ) -> Vec<RangeTombstone> {
        #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
        let tree = self.range_tombstones.read().expect("lock poisoned");
        tree.overlapping_tombstones(key, read_seqno)
    }

    /// Returns the highest-seqno covering tombstone for `[min, max]`, if any.
    ///
    /// Used for table-skip decisions.
    pub fn query_covering_rt_for_range(
        &self,
        min: &[u8],
        max: &[u8],
        read_seqno: SeqNo,
    ) -> Option<CoveringRt> {
        #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
        let tree = self.range_tombstones.read().expect("lock poisoned");
        tree.query_covering_rt_for_range(min, max, read_seqno)
    }

    /// Returns all range tombstones sorted by `(start asc, seqno desc, end asc)`.
    ///
    /// Used for flush / encoding the ByStart block.
    pub fn range_tombstones_by_start(&self) -> Vec<RangeTombstone> {
        #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
        let tree = self.range_tombstones.read().expect("lock poisoned");
        tree.iter_sorted()
    }

    /// Returns all range tombstones sorted by `(end desc, seqno desc)`.
    ///
    /// Used for flush / encoding the ByEndDesc block.
    pub fn range_tombstones_by_end_desc(&self) -> Vec<RangeTombstone> {
        #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
        let by_end = self.tombstones_by_end.read().expect("lock poisoned");
        by_end.values().cloned().collect()
    }

    /// Returns the number of range tombstones in the memtable.
    pub fn range_tombstone_count(&self) -> usize {
        #[expect(clippy::expect_used, reason = "lock poisoning is unrecoverable")]
        let tree = self.range_tombstones.read().expect("lock poisoned");
        tree.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueType;
    use test_log::test;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn memtable_mvcc_point_read() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991",
            0,
            ValueType::Value,
        ));

        let item = memtable.get(b"hello-key-99999", SeqNo::MAX);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", SeqNo::MAX);
        assert_eq!(*b"hello-value-999991", &*item.unwrap().value);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991-2",
            1,
            ValueType::Value,
        ));

        let item = memtable.get(b"hello-key-99999", SeqNo::MAX);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", SeqNo::MAX);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);

        let item = memtable.get(b"hello-key-99999", 1);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", 1);
        assert_eq!((*b"hello-value-999991"), &*item.unwrap().value);

        let item = memtable.get(b"hello-key-99999", 2);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", 2);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);
    }

    #[test]
    fn memtable_get() {
        let memtable = Memtable::new(0);

        let value =
            InternalValue::from_components(b"abc".to_vec(), b"abc".to_vec(), 0, ValueType::Value);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get(b"abc", SeqNo::MAX));
    }

    #[test]
    fn memtable_get_highest_seqno() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            1,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            2,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            3,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            4,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                4,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );
    }

    #[test]
    fn memtable_get_prefix() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            b"abc0".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc0".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get(b"abc0", SeqNo::MAX)
        );
    }

    #[test]
    fn memtable_get_old_version() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            99,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                99,
                ValueType::Value,
            )),
            memtable.get(b"abc", 100)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get(b"abc", 50)
        );
    }
}
