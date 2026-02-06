// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Active tombstone sets for tracking range tombstones during iteration.
//!
//! During forward or reverse scans, range tombstones must be activated when
//! the scan enters their range and expired when it leaves. These sets use
//! a seqno multiset (`BTreeMap<SeqNo, u32>`) for O(log t) max-seqno queries,
//! and a heap for efficient expiry tracking.
//!
//! A unique monotonic `id` on each heap entry ensures total ordering in the
//! heap (no equality on the tuple), which makes expiry deterministic.

use crate::{range_tombstone::RangeTombstone, SeqNo, UserKey};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};

/// Tracks active range tombstones during forward iteration.
///
/// Tombstones are activated when the scan reaches their `start` key, and
/// expired when the scan reaches or passes their `end` key.
///
/// Uses a min-heap (via `Reverse`) keyed by `(end, id, seqno)` so the
/// tombstone expiring soonest (smallest `end`) is at the top.
pub struct ActiveTombstoneSet {
    seqno_counts: BTreeMap<SeqNo, u32>,
    pending_expiry: BinaryHeap<Reverse<(UserKey, u64, SeqNo)>>,
    cutoff_seqno: SeqNo,
    next_id: u64,
}

impl ActiveTombstoneSet {
    /// Creates a new forward active tombstone set.
    ///
    /// Only tombstones with `seqno <= cutoff_seqno` will be activated.
    #[must_use]
    pub fn new(cutoff_seqno: SeqNo) -> Self {
        Self {
            seqno_counts: BTreeMap::new(),
            pending_expiry: BinaryHeap::new(),
            cutoff_seqno,
            next_id: 0,
        }
    }

    /// Activates a range tombstone, adding it to the active set.
    ///
    /// The tombstone is only activated if it is visible at the cutoff seqno
    /// (i.e., `rt.seqno <= cutoff_seqno`). Duplicate activations (same seqno
    /// from different sources) are handled correctly via multiset accounting.
    pub fn activate(&mut self, rt: &RangeTombstone) {
        if !rt.visible_at(self.cutoff_seqno) {
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        *self.seqno_counts.entry(rt.seqno).or_insert(0) += 1;
        self.pending_expiry
            .push(Reverse((rt.end.clone(), id, rt.seqno)));
    }

    /// Expires tombstones whose `end <= current_key`.
    ///
    /// In the half-open convention `[start, end)`, a tombstone stops covering
    /// keys at `end`. So when `current_key >= end`, the tombstone no longer
    /// applies and is removed from the active set.
    ///
    /// # Panics
    ///
    /// Panics if an expiry pop has no matching activation in the seqno multiset.
    pub fn expire_until(&mut self, current_key: &[u8]) {
        while let Some(Reverse((ref end, _, seqno))) = self.pending_expiry.peek() {
            if current_key >= end.as_ref() {
                let seqno = *seqno;
                self.pending_expiry.pop();
                #[expect(
                    clippy::expect_used,
                    reason = "expiry pop must have matching activation"
                )]
                let count = self
                    .seqno_counts
                    .get_mut(&seqno)
                    .expect("expiry pop must have matching activation");
                *count -= 1;
                if *count == 0 {
                    self.seqno_counts.remove(&seqno);
                }
            } else {
                break;
            }
        }
    }

    /// Returns the highest seqno among all active tombstones, or `None` if
    /// no tombstones are active.
    #[must_use]
    pub fn max_active_seqno(&self) -> Option<SeqNo> {
        self.seqno_counts.keys().next_back().copied()
    }

    /// Returns `true` if a KV with the given seqno is suppressed by any
    /// active tombstone (i.e., there exists an active tombstone with a
    /// higher seqno).
    #[must_use]
    pub fn is_suppressed(&self, key_seqno: SeqNo) -> bool {
        self.max_active_seqno().is_some_and(|max| key_seqno < max)
    }

    /// Bulk-activates tombstones at a seek position.
    ///
    /// # Invariant
    ///
    /// At any iterator position, the active set contains only tombstones
    /// where `start <= current_key < end` (and visible at `cutoff_seqno`).
    /// Seek prefill must collect truly overlapping tombstones
    /// (`start <= key < end`); `expire_until` immediately enforces the
    /// `end` bound.
    pub fn initialize_from(&mut self, tombstones: impl IntoIterator<Item = RangeTombstone>) {
        for rt in tombstones {
            self.activate(&rt);
        }
    }

    /// Returns `true` if there are no active tombstones.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seqno_counts.is_empty()
    }
}

/// Tracks active range tombstones during reverse iteration.
///
/// During reverse scans, tombstones are activated when the scan reaches
/// a key < `end` (strict `>`: `rt.end > current_key`), and expired when
/// `current_key < rt.start`.
///
/// Uses a max-heap keyed by `(start, id, seqno)` so the tombstone
/// expiring soonest (largest `start`) is at the top.
///
/// # Future extension: cache invalidation
///
/// If a per-table `tombstone_global_max_end` cache is ever added for
/// reverse init bounding, it must be invalidated on block reload
/// (rare, but necessary for correctness if blocks can be evicted
/// and reloaded with different contents during the lifetime of a
/// `SuperVersion`).
pub struct ActiveTombstoneSetReverse {
    seqno_counts: BTreeMap<SeqNo, u32>,
    pending_expiry: BinaryHeap<(UserKey, u64, SeqNo)>,
    cutoff_seqno: SeqNo,
    next_id: u64,
}

impl ActiveTombstoneSetReverse {
    /// Creates a new reverse active tombstone set.
    ///
    /// Only tombstones with `seqno <= cutoff_seqno` will be activated.
    #[must_use]
    pub fn new(cutoff_seqno: SeqNo) -> Self {
        Self {
            seqno_counts: BTreeMap::new(),
            pending_expiry: BinaryHeap::new(),
            cutoff_seqno,
            next_id: 0,
        }
    }

    /// Activates a range tombstone, adding it to the active set.
    ///
    /// The tombstone is only activated if it is visible at the cutoff seqno
    /// (i.e., `rt.seqno <= cutoff_seqno`). Duplicate activations (same seqno
    /// from different sources) are handled correctly via multiset accounting.
    ///
    /// For reverse iteration, activation uses strict `>`: tombstones with
    /// `rt.end > current_key` are activated. `key == end` is NOT covered
    /// (half-open).
    pub fn activate(&mut self, rt: &RangeTombstone) {
        if !rt.visible_at(self.cutoff_seqno) {
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        *self.seqno_counts.entry(rt.seqno).or_insert(0) += 1;
        self.pending_expiry.push((rt.start.clone(), id, rt.seqno));
    }

    /// Expires tombstones whose `start > current_key`.
    ///
    /// During reverse iteration, when the scan moves to a key that is
    /// before a tombstone's start, that tombstone no longer applies.
    ///
    /// # Panics
    ///
    /// Panics if an expiry pop has no matching activation in the seqno multiset.
    pub fn expire_until(&mut self, current_key: &[u8]) {
        while let Some((ref start, _, seqno)) = self.pending_expiry.peek() {
            if current_key < start.as_ref() {
                let seqno = *seqno;
                self.pending_expiry.pop();
                #[expect(
                    clippy::expect_used,
                    reason = "expiry pop must have matching activation"
                )]
                let count = self
                    .seqno_counts
                    .get_mut(&seqno)
                    .expect("expiry pop must have matching activation");
                *count -= 1;
                if *count == 0 {
                    self.seqno_counts.remove(&seqno);
                }
            } else {
                break;
            }
        }
    }

    /// Returns the highest seqno among all active tombstones, or `None` if
    /// no tombstones are active.
    #[must_use]
    pub fn max_active_seqno(&self) -> Option<SeqNo> {
        self.seqno_counts.keys().next_back().copied()
    }

    /// Returns `true` if a KV with the given seqno is suppressed by any
    /// active tombstone (i.e., there exists an active tombstone with a
    /// higher seqno).
    #[must_use]
    pub fn is_suppressed(&self, key_seqno: SeqNo) -> bool {
        self.max_active_seqno().is_some_and(|max| key_seqno < max)
    }

    /// Bulk-activates tombstones at a seek position (for reverse).
    pub fn initialize_from(&mut self, tombstones: impl IntoIterator<Item = RangeTombstone>) {
        for rt in tombstones {
            self.activate(&rt);
        }
    }

    /// Returns `true` if there are no active tombstones.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seqno_counts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UserKey;

    fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    // ──── Forward tests ────

    #[test]
    fn forward_activate_and_suppress() {
        let mut set = ActiveTombstoneSet::new(100);
        set.activate(&rt(b"a", b"m", 10));
        assert!(set.is_suppressed(5));
        assert!(!set.is_suppressed(10));
        assert!(!set.is_suppressed(15));
    }

    #[test]
    fn forward_expire_at_end() {
        let mut set = ActiveTombstoneSet::new(100);
        set.activate(&rt(b"a", b"m", 10));
        assert!(set.is_suppressed(5));
        set.expire_until(b"m"); // key == end, tombstone expires
        assert!(!set.is_suppressed(5));
    }

    #[test]
    fn forward_expire_past_end() {
        let mut set = ActiveTombstoneSet::new(100);
        set.activate(&rt(b"a", b"m", 10));
        set.expire_until(b"z");
        assert!(set.is_empty());
    }

    #[test]
    fn forward_not_expired_before_end() {
        let mut set = ActiveTombstoneSet::new(100);
        set.activate(&rt(b"a", b"m", 10));
        set.expire_until(b"l");
        assert!(set.is_suppressed(5)); // still active
    }

    #[test]
    fn forward_invisible_tombstone_not_activated() {
        let mut set = ActiveTombstoneSet::new(5);
        set.activate(&rt(b"a", b"m", 10)); // seqno 10 > cutoff 5
        assert!(!set.is_suppressed(1));
        assert!(set.is_empty());
    }

    #[test]
    fn forward_multiple_tombstones_max_seqno() {
        let mut set = ActiveTombstoneSet::new(100);
        set.activate(&rt(b"a", b"m", 10));
        set.activate(&rt(b"b", b"n", 20));
        assert_eq!(set.max_active_seqno(), Some(20));
        assert!(set.is_suppressed(15)); // 15 < 20
    }

    #[test]
    fn forward_duplicate_end_seqno_accounting() {
        // Test H: Two tombstones with same end + seqno
        let mut set = ActiveTombstoneSet::new(100);
        set.activate(&rt(b"a", b"m", 10));
        set.activate(&rt(b"b", b"m", 10));
        assert_eq!(set.max_active_seqno(), Some(10));

        // Expire at "m" — both should be removed
        set.expire_until(b"m");
        assert_eq!(set.max_active_seqno(), None);
        assert!(set.is_empty());
    }

    #[test]
    fn forward_initialize_from() {
        let mut set = ActiveTombstoneSet::new(100);
        set.initialize_from(vec![rt(b"a", b"m", 10), rt(b"b", b"z", 20)]);
        assert_eq!(set.max_active_seqno(), Some(20));
    }

    #[test]
    fn forward_initialize_and_expire() {
        let mut set = ActiveTombstoneSet::new(100);
        set.initialize_from(vec![rt(b"a", b"d", 10), rt(b"b", b"f", 20)]);
        set.expire_until(b"e"); // expires [a,d) but not [b,f)
        assert_eq!(set.max_active_seqno(), Some(20));
        set.expire_until(b"f"); // expires [b,f)
        assert!(set.is_empty());
    }

    // ──── Reverse tests ────

    #[test]
    fn reverse_activate_and_suppress() {
        let mut set = ActiveTombstoneSetReverse::new(100);
        set.activate(&rt(b"a", b"m", 10));
        assert!(set.is_suppressed(5));
        assert!(!set.is_suppressed(10));
    }

    #[test]
    fn reverse_expire_before_start() {
        let mut set = ActiveTombstoneSetReverse::new(100);
        set.activate(&rt(b"d", b"m", 10));

        // Key before start — tombstone expires
        set.expire_until(b"c");
        assert!(set.is_empty());
    }

    #[test]
    fn reverse_not_expired_at_start() {
        let mut set = ActiveTombstoneSetReverse::new(100);
        set.activate(&rt(b"d", b"m", 10));

        // Key == start — tombstone still active (key is covered)
        set.expire_until(b"d");
        assert!(set.is_suppressed(5));
    }

    #[test]
    fn reverse_invisible_tombstone_not_activated() {
        let mut set = ActiveTombstoneSetReverse::new(5);
        set.activate(&rt(b"a", b"m", 10));
        assert!(set.is_empty());
    }

    #[test]
    fn reverse_duplicate_end_seqno_accounting() {
        // Symmetric to forward Test H
        let mut set = ActiveTombstoneSetReverse::new(100);
        set.activate(&rt(b"d", b"m", 10));
        set.activate(&rt(b"d", b"n", 10)); // same start + seqno
        assert_eq!(set.max_active_seqno(), Some(10));

        // Expire before start
        set.expire_until(b"c");
        assert_eq!(set.max_active_seqno(), None);
        assert!(set.is_empty());
    }

    #[test]
    fn reverse_multiple_tombstones() {
        let mut set = ActiveTombstoneSetReverse::new(100);
        set.activate(&rt(b"a", b"m", 10));
        set.activate(&rt(b"f", b"z", 20));
        assert_eq!(set.max_active_seqno(), Some(20));

        // Moving to 'e' expires [f,z) but keeps [a,m)
        set.expire_until(b"e");
        assert_eq!(set.max_active_seqno(), Some(10));
    }
}
