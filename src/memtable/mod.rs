// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod interval_tree;

use crate::key::InternalKey;
use crate::range_tombstone::RangeTombstone;
use crate::{
    value::{InternalValue, SeqNo, UserValue},
    UserKey, ValueType,
};
use crossbeam_skiplist::SkipMap;
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

    /// Range tombstones stored in an interval tree.
    ///
    /// Protected by `RwLock` — read-heavy suppression queries (`query_suppression`,
    /// `range_tombstones_sorted`) take a shared read lock, while `insert_range_tombstone`
    /// takes an exclusive write lock. After a rotation has been requested via
    /// `requested_rotation`, the interval tree is treated as read-only by convention,
    /// and only readers are expected to access this field (the `RwLock` is still used
    /// for synchronization, but there should be no further writes).
    ///
    /// `std::sync::RwLock` may be reader-biased on some platforms, but writer
    /// starvation is not a concern here: range deletes are rare, the write-side
    /// critical section is O(log n) with n typically small, and the memtable
    /// rotates (becoming read-only) well before contention could accumulate.
    pub(crate) range_tombstones: RwLock<interval_tree::IntervalTree>,

    /// Approximate active memtable size.
    ///
    /// If this grows too large, a flush is triggered.
    pub(crate) approximate_size: AtomicU64,

    /// Highest encountered sequence number.
    ///
    /// This is used so that `get_highest_seqno` has O(1) complexity.
    pub(crate) highest_seqno: AtomicU64,

    pub(crate) requested_rotation: AtomicBool,
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
            range_tombstones: RwLock::new(interval_tree::IntervalTree::new()),
            approximate_size: AtomicU64::default(),
            highest_seqno: AtomicU64::default(),
            requested_rotation: AtomicBool::default(),
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
        // and has the seqno (or lower) we want (because the seqno is stored in reverse order)
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

    /// Returns `true` if the memtable has no KV items and no range tombstones.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && self.range_tombstone_count() == 0
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

    /// Inserts a range tombstone covering `[start, end)` at the given seqno.
    ///
    /// Returns the approximate size added to the memtable.
    ///
    /// Returns 0 if `start >= end` or if either bound exceeds `u16::MAX` bytes.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    #[must_use]
    pub fn insert_range_tombstone(&self, start: UserKey, end: UserKey, seqno: SeqNo) -> u64 {
        // flag_rotated() (which sets requested_rotation) is called by the host
        // crate (fjall) before rotation; this crate never sets it directly.
        // The assert catches misuse by callers
        // in debug builds — intentionally debug-only because post-rotation writes
        // are structurally prevented by the host (sealed memtables are behind Arc
        // with no write path exposed), and an atomic load here would add overhead
        // on the hot insert path in release builds for no practical benefit.
        debug_assert!(
            !self.is_flagged_for_rotation(),
            "insert_range_tombstone called after memtable was flagged for rotation"
        );

        // Reject invalid intervals in release builds (debug_assert is not enough)
        if start >= end {
            return 0;
        }

        // On-disk RT format writes key lengths as u16, enforce at insertion time.
        // Emit a warning when rejecting an oversized bound so this failure is diagnosable.
        if u16::try_from(start.len()).is_err() || u16::try_from(end.len()).is_err() {
            log::warn!(
                "insert_range_tombstone: rejecting oversized range tombstone \
                 bounds (start_len = {}, end_len = {}, max = {})",
                start.len(),
                end.len(),
                u16::MAX,
            );
            return 0;
        }

        let size = (start.len() + end.len() + std::mem::size_of::<RangeTombstone>()) as u64;

        // Panic on poison is intentional — a poisoned lock indicates a prior panic
        // during a write, leaving the tree in an unknown state. Recovery would
        // require validating AVL invariants which is not worth the complexity.
        // This pattern is consistent with the original Mutex implementation.
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.range_tombstones
            .write()
            .expect("lock is poisoned")
            .insert(RangeTombstone::new(start, end, seqno));

        self.approximate_size
            .fetch_add(size, std::sync::atomic::Ordering::AcqRel);

        self.highest_seqno
            .fetch_max(seqno, std::sync::atomic::Ordering::AcqRel);

        size
    }

    /// Returns `true` if the key at `key_seqno` is suppressed by a range tombstone
    /// visible at `read_seqno`.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub(crate) fn is_key_suppressed_by_range_tombstone(
        &self,
        key: &[u8],
        key_seqno: SeqNo,
        read_seqno: SeqNo,
    ) -> bool {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.range_tombstones
            .read()
            .expect("lock is poisoned")
            .query_suppression(key, key_seqno, read_seqno)
    }

    /// Returns all range tombstones in sorted order (for flush).
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    pub(crate) fn range_tombstones_sorted(&self) -> Vec<RangeTombstone> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.range_tombstones
            .read()
            .expect("lock is poisoned")
            .iter_sorted()
    }

    /// Returns the number of range tombstones.
    ///
    /// # Panics
    ///
    /// Panics if the internal `RwLock` is poisoned.
    #[must_use]
    pub fn range_tombstone_count(&self) -> usize {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.range_tombstones
            .read()
            .expect("lock is poisoned")
            .len()
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_highest_seqno(&self) -> Option<SeqNo> {
        if self.is_empty() {
            None
        } else {
            Some(
                self.highest_seqno
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueType;
    use std::sync::{Arc, Barrier};
    use test_log::test;

    #[test]
    #[expect(
        clippy::expect_used,
        reason = "tests use expect for lock and thread join"
    )]
    fn rwlock_read_while_read_held_succeeds() {
        let mt = Memtable::new(0);
        let _ = mt.insert_range_tombstone(b"a".to_vec().into(), b"z".to_vec().into(), 10);

        // Two one-way channels avoid Barrier entirely — if either side
        // panics, the sender drops and recv() returns Err, unblocking the
        // peer so thread::scope can join without hanging.
        let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let rt_ref = &mt.range_tombstones;
        std::thread::scope(|s| {
            s.spawn(move || {
                let _guard = rt_ref.read().expect("lock is poisoned");
                let _ = held_tx.send(()); // signal: guard held
                let _ = release_rx.recv(); // wait: main thread done
            });

            held_rx
                .recv()
                .expect("spawned thread panicked before acquiring guard");
            let guard2 = mt.range_tombstones.try_read();
            assert!(
                guard2.is_ok(),
                "second read lock must succeed while first is held"
            );
            drop(guard2);
            drop(release_tx); // signal: done
        });
    }

    #[test]
    #[expect(clippy::expect_used, reason = "tests use expect for thread join")]
    fn suppression_queries_concurrent_readers_no_panic() {
        let mt = Arc::new(Memtable::new(0));

        let _ = mt.insert_range_tombstone(b"a".to_vec().into(), b"z".to_vec().into(), 10);
        for i in 0u8..100 {
            let key = vec![b'a' + (i % 25)];
            mt.insert(InternalValue::from_components(
                key,
                b"v".to_vec(),
                u64::from(i),
                ValueType::Value,
            ));
        }

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let mt = Arc::clone(&mt);
                std::thread::spawn(move || {
                    for i in 0u8..200 {
                        let key = vec![b'a' + ((t + i) % 25)];
                        let _ = mt.is_key_suppressed_by_range_tombstone(&key, 5, SeqNo::MAX);
                        let _ = mt.range_tombstone_count();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    #[test]
    #[expect(clippy::expect_used, reason = "tests use expect for thread join")]
    fn range_tombstones_concurrent_read_write_writers_observable() {
        let mt = Arc::new(Memtable::new(0));
        // Barrier ensures all 6 threads start simultaneously.
        let start = Arc::new(Barrier::new(6));

        let _ = mt.insert_range_tombstone(b"a".to_vec().into(), b"m".to_vec().into(), 10);

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let mt = Arc::clone(&mt);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..500 {
                        let suppressed =
                            mt.is_key_suppressed_by_range_tombstone(b"f", 5, SeqNo::MAX);
                        assert!(
                            suppressed,
                            "key 'f' at seqno=5 must be suppressed by RT [a,m)@10"
                        );
                    }
                })
            })
            .collect();

        let writers: Vec<_> = (0..2)
            .map(|t| {
                let mt = Arc::clone(&mt);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    let start_key: UserKey = b"n".to_vec().into();
                    let end_key: UserKey = b"z".to_vec().into();
                    for i in 0u64..100 {
                        let seqno = 100 + t * 1000 + i;
                        let _ =
                            mt.insert_range_tombstone(start_key.clone(), end_key.clone(), seqno);
                    }
                })
            })
            .collect();

        for h in readers {
            h.join().expect("reader panicked");
        }
        for h in writers {
            h.join().expect("writer panicked");
        }

        // We intentionally do not assert that any reader observed a
        // writer-inserted tombstone mid-loop. `std::sync::RwLock` may be
        // reader-biased, so writers are allowed to be blocked until all
        // readers have finished, which would make such an assertion flaky.
        // Instead, validate post-join visibility: writers insert [n,z) at
        // seqnos starting from 100, so keys in this range must be suppressed.
        assert!(mt.is_key_suppressed_by_range_tombstone(b"n", 50, SeqNo::MAX));
        assert!(mt.is_key_suppressed_by_range_tombstone(b"y", 150, SeqNo::MAX));
    }

    #[test]
    #[expect(clippy::expect_used, reason = "tests use expect for thread join")]
    fn range_tombstones_populated_tree_concurrent_reads_succeed() {
        let mt = Arc::new(Memtable::new(0));

        for i in 0u8..50 {
            let start = vec![b'a' + (i % 25)];
            let end = vec![b'a' + (i % 25) + 1];
            let _ = mt.insert_range_tombstone(start.into(), end.into(), u64::from(i));
        }

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let mt = Arc::clone(&mt);
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let _ = mt.is_key_suppressed_by_range_tombstone(b"c", 5, SeqNo::MAX);
                        let sorted = mt.range_tombstones_sorted();
                        assert!(!sorted.is_empty());
                        let count = mt.range_tombstone_count();
                        assert!(count > 0);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

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
