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
    next_id: u64,
}

impl ActiveTombstoneSet {
    /// Creates a new forward active tombstone set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seqno_counts: BTreeMap::new(),
            pending_expiry: BinaryHeap::new(),
            next_id: 0,
        }
    }

    /// Activates a range tombstone, adding it to the active set.
    ///
    /// The tombstone is only activated if it is visible at `cutoff_seqno`
    /// (i.e., `rt.seqno < cutoff_seqno`). Each source may supply a different
    /// cutoff (e.g., ephemeral memtable uses its own `index_seqno`).
    /// Duplicate activations (same seqno from different sources) are handled
    /// correctly via multiset accounting.
    pub fn activate(&mut self, rt: &RangeTombstone, cutoff_seqno: SeqNo) {
        if !rt.visible_at(cutoff_seqno) {
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
    /// where `start <= current_key < end` (and visible at their respective
    /// `cutoff_seqno`). Seek prefill must collect truly overlapping tombstones
    /// (`start <= key < end`); `expire_until` immediately enforces the
    /// `end` bound.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by iterator initialization logic")
    )]
    pub fn initialize_from(
        &mut self,
        tombstones: impl IntoIterator<Item = (RangeTombstone, SeqNo)>,
    ) {
        for (rt, cutoff) in tombstones {
            self.activate(&rt, cutoff);
        }
    }

    /// Returns `true` if there are no active tombstones.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "helper for callers to inspect active tombstones")
    )]
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
pub struct ActiveTombstoneSetReverse {
    seqno_counts: BTreeMap<SeqNo, u32>,
    pending_expiry: BinaryHeap<(UserKey, u64, SeqNo)>,
    next_id: u64,
}

impl ActiveTombstoneSetReverse {
    /// Creates a new reverse active tombstone set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seqno_counts: BTreeMap::new(),
            pending_expiry: BinaryHeap::new(),
            next_id: 0,
        }
    }

    /// Activates a range tombstone, adding it to the active set.
    ///
    /// The tombstone is only activated if it is visible at `cutoff_seqno`
    /// (i.e., `rt.seqno < cutoff_seqno`). Each source may supply a different
    /// cutoff (e.g., ephemeral memtable uses its own `index_seqno`).
    /// Duplicate activations (same seqno from different sources) are handled
    /// correctly via multiset accounting.
    ///
    /// For reverse iteration, activation uses strict `>`: tombstones with
    /// `rt.end > current_key` are activated. `key == end` is NOT covered
    /// (half-open).
    pub fn activate(&mut self, rt: &RangeTombstone, cutoff_seqno: SeqNo) {
        if !rt.visible_at(cutoff_seqno) {
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
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by iterator initialization logic")
    )]
    pub fn initialize_from(
        &mut self,
        tombstones: impl IntoIterator<Item = (RangeTombstone, SeqNo)>,
    ) {
        for (rt, cutoff) in tombstones {
            self.activate(&rt, cutoff);
        }
    }

    /// Returns `true` if there are no active tombstones.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "helper for callers to inspect active tombstones")
    )]
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
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        assert!(set.is_suppressed(5));
        assert!(!set.is_suppressed(10));
        assert!(!set.is_suppressed(15));
    }

    #[test]
    fn forward_expire_at_end() {
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        assert!(set.is_suppressed(5));
        set.expire_until(b"m"); // key == end, tombstone expires
        assert!(!set.is_suppressed(5));
    }

    #[test]
    fn forward_expire_past_end() {
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        set.expire_until(b"z");
        assert!(set.is_empty());
    }

    #[test]
    fn forward_not_expired_before_end() {
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        set.expire_until(b"l");
        assert!(set.is_suppressed(5)); // still active
    }

    #[test]
    fn forward_invisible_tombstone_not_activated() {
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 5); // seqno 10 > cutoff 5
        assert!(!set.is_suppressed(1));
        assert!(set.is_empty());
    }

    #[test]
    fn forward_multiple_tombstones_max_seqno() {
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        set.activate(&rt(b"b", b"n", 20), 100);
        assert_eq!(set.max_active_seqno(), Some(20));
        assert!(set.is_suppressed(15)); // 15 < 20
    }

    #[test]
    fn forward_duplicate_end_seqno_accounting() {
        let mut set = ActiveTombstoneSet::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        set.activate(&rt(b"b", b"m", 10), 100);
        assert_eq!(set.max_active_seqno(), Some(10));

        set.expire_until(b"m");
        assert_eq!(set.max_active_seqno(), None);
        assert!(set.is_empty());
    }

    #[test]
    fn forward_initialize_from() {
        let mut set = ActiveTombstoneSet::new();
        set.initialize_from(vec![(rt(b"a", b"m", 10), 100), (rt(b"b", b"z", 20), 100)]);
        assert_eq!(set.max_active_seqno(), Some(20));
    }

    #[test]
    fn forward_initialize_and_expire() {
        let mut set = ActiveTombstoneSet::new();
        set.initialize_from(vec![(rt(b"a", b"d", 10), 100), (rt(b"b", b"f", 20), 100)]);
        set.expire_until(b"e"); // expires [a,d) but not [b,f)
        assert_eq!(set.max_active_seqno(), Some(20));
        set.expire_until(b"f"); // expires [b,f)
        assert!(set.is_empty());
    }

    #[test]
    fn forward_mixed_cutoffs_activates_only_visible_rt() {
        let mut set = ActiveTombstoneSet::new();
        // RT from source with cutoff 15 — visible (10 < 15)
        set.activate(&rt(b"a", b"m", 10), 15);
        // RT from source with cutoff 5 — NOT visible (10 >= 5)
        set.activate(&rt(b"a", b"z", 10), 5);
        assert_eq!(set.max_active_seqno(), Some(10));
        assert!(!set.is_empty());

        // Expire past the first RT's end; the set should now be empty if the
        // second RT was never incorrectly activated.
        set.expire_until(b"m");
        assert!(set.is_empty());
    }

    // ──── Reverse tests ────

    #[test]
    fn reverse_activate_and_suppress() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        assert!(set.is_suppressed(5));
        assert!(!set.is_suppressed(10));
    }

    #[test]
    fn reverse_expire_before_start() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.activate(&rt(b"d", b"m", 10), 100);

        set.expire_until(b"c");
        assert!(set.is_empty());
    }

    #[test]
    fn reverse_initialize_from() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.initialize_from(vec![(rt(b"a", b"m", 10), 100), (rt(b"b", b"z", 20), 100)]);
        assert_eq!(set.max_active_seqno(), Some(20));
    }

    #[test]
    fn reverse_not_expired_at_start() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.activate(&rt(b"d", b"m", 10), 100);

        set.expire_until(b"d");
        assert!(set.is_suppressed(5));
    }

    #[test]
    fn reverse_invisible_tombstone_not_activated() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.activate(&rt(b"a", b"m", 10), 5);
        assert!(set.is_empty());
    }

    #[test]
    fn reverse_duplicate_end_seqno_accounting() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.activate(&rt(b"d", b"m", 10), 100);
        set.activate(&rt(b"d", b"n", 10), 100);
        assert_eq!(set.max_active_seqno(), Some(10));

        set.expire_until(b"c");
        assert_eq!(set.max_active_seqno(), None);
        assert!(set.is_empty());
    }

    #[test]
    fn reverse_multiple_tombstones() {
        let mut set = ActiveTombstoneSetReverse::new();
        set.activate(&rt(b"a", b"m", 10), 100);
        set.activate(&rt(b"f", b"z", 20), 100);
        assert_eq!(set.max_active_seqno(), Some(20));

        set.expire_until(b"e");
        assert_eq!(set.max_active_seqno(), Some(10));
    }

    #[test]
    fn reverse_mixed_cutoffs_activates_only_visible_rt() {
        let mut set = ActiveTombstoneSetReverse::new();
        // RT from source with cutoff 15 — visible (10 < 15)
        set.activate(&rt(b"n", b"z", 10), 15);
        // RT from source with cutoff 5 — NOT visible (10 >= 5)
        set.activate(&rt(b"a", b"m", 10), 5);
        assert_eq!(set.max_active_seqno(), Some(10));

        // Advance expiry past the visible tombstone's start but not the
        // invisible one's.  If only the visible RT was activated, the set
        // should become empty.
        set.expire_until(b"l");
        assert_eq!(set.max_active_seqno(), None);
        assert!(set.is_empty());
    }
}
