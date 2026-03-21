// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Bidirectional range tombstone filter for iteration.
//!
//! Wraps a sorted KV stream and suppresses entries covered by range tombstones.
//! Forward: tombstones sorted by `(start asc, seqno desc)`, activated when
//! `start <= key`, expired when `end <= key`.
//! Reverse: tombstones sorted by `(end desc, seqno desc)`, activated when
//! `end > key`, expired when `key < start`.

use crate::active_tombstone_set::{ActiveTombstoneSet, ActiveTombstoneSetReverse};
use crate::range_tombstone::RangeTombstone;
use crate::{InternalValue, SeqNo};

/// Wraps a bidirectional KV stream and suppresses entries covered by range tombstones.
///
/// Each tombstone is paired with a per-source visibility cutoff (`SeqNo`).
/// Different sources may use different cutoffs — e.g., an ephemeral memtable
/// uses its own `index_seqno` while disk segments use the outer scan seqno.
pub struct RangeTombstoneFilter<I> {
    inner: I,

    // Forward state: (tombstone, per-source cutoff)
    fwd_tombstones: Vec<(RangeTombstone, SeqNo)>,
    fwd_idx: usize,
    fwd_active: ActiveTombstoneSet,

    // Reverse state: (tombstone, per-source cutoff)
    rev_tombstones: Vec<(RangeTombstone, SeqNo)>,
    rev_idx: usize,
    rev_active: ActiveTombstoneSetReverse,
}

impl<I> RangeTombstoneFilter<I> {
    /// Creates a new bidirectional filter.
    ///
    /// Each tombstone is paired with its per-source visibility cutoff.
    /// Forward tombstones need not be pre-sorted — the constructor sorts
    /// internally. A second copy sorted by `(end desc, seqno desc)` is
    /// created for reverse iteration.
    #[must_use]
    pub fn new(inner: I, mut fwd_tombstones: Vec<(RangeTombstone, SeqNo)>) -> Self {
        // Sort by RT natural order (start asc, seqno desc, end asc).
        // Callers may pre-sort for dedup; re-sorting is O(n) on sorted input.
        fwd_tombstones.sort_by(|a, b| a.0.cmp(&b.0));

        // Build reverse-sorted copy: (end desc, seqno desc)
        let mut rev_tombstones = fwd_tombstones.clone();
        rev_tombstones.sort_by(|a, b| (&b.0.end, &b.0.seqno).cmp(&(&a.0.end, &a.0.seqno)));

        Self {
            inner,
            fwd_tombstones,
            fwd_idx: 0,
            fwd_active: ActiveTombstoneSet::new(),
            rev_tombstones,
            rev_idx: 0,
            rev_active: ActiveTombstoneSetReverse::new(),
        }
    }

    /// Activates forward tombstones whose start <= `current_key`.
    fn fwd_activate_up_to(&mut self, key: &[u8]) {
        while let Some((rt, cutoff)) = self.fwd_tombstones.get(self.fwd_idx) {
            if rt.start.as_ref() <= key {
                self.fwd_active.activate(rt, *cutoff);
                self.fwd_idx += 1;
            } else {
                break;
            }
        }
    }

    /// Activates reverse tombstones whose end > `current_key`.
    fn rev_activate_up_to(&mut self, key: &[u8]) {
        while let Some((rt, cutoff)) = self.rev_tombstones.get(self.rev_idx) {
            if rt.end.as_ref() > key {
                self.rev_active.activate(rt, *cutoff);
                self.rev_idx += 1;
            } else {
                break;
            }
        }
    }
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> Iterator for RangeTombstoneFilter<I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;

            let Ok(kv) = &item else { return Some(item) };

            let key = kv.key.user_key.as_ref();
            let kv_seqno = kv.key.seqno;

            // Activate tombstones whose start <= this key
            self.fwd_activate_up_to(key);

            // Expire tombstones whose end <= this key
            self.fwd_active.expire_until(key);

            // Check suppression
            if self.fwd_active.is_suppressed(kv_seqno) {
                continue;
            }

            return Some(item);
        }
    }
}

impl<I: DoubleEndedIterator<Item = crate::Result<InternalValue>>> DoubleEndedIterator
    for RangeTombstoneFilter<I>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next_back()?;

            let Ok(kv) = &item else { return Some(item) };

            let key = kv.key.user_key.as_ref();
            let kv_seqno = kv.key.seqno;

            // Activate tombstones whose end > this key (strict >)
            self.rev_activate_up_to(key);

            // Expire tombstones whose start > this key (key < start)
            self.rev_active.expire_until(key);

            // Check suppression
            if self.rev_active.is_suppressed(kv_seqno) {
                continue;
            }

            return Some(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{UserKey, ValueType};

    fn kv(key: &[u8], seqno: SeqNo) -> InternalValue {
        InternalValue::from_components(key, b"v", seqno, ValueType::Value)
    }

    fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    /// Helper: tag all tombstones with the same cutoff seqno.
    fn tagged(tombstones: Vec<RangeTombstone>, cutoff: SeqNo) -> Vec<(RangeTombstone, SeqNo)> {
        tombstones.into_iter().map(|rt| (rt, cutoff)).collect()
    }

    #[test]
    fn items_no_tombstones_return_all() {
        let items: Vec<crate::Result<InternalValue>> =
            vec![Ok(kv(b"a", 1)), Ok(kv(b"b", 2)), Ok(kv(b"c", 3))];

        let filter = RangeTombstoneFilter::new(items.into_iter(), vec![]);
        let results: Vec<_> = filter.flatten().collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn items_with_range_tombstone_suppress_covered_keys() {
        let items: Vec<crate::Result<InternalValue>> = vec![
            Ok(kv(b"a", 5)),
            Ok(kv(b"b", 5)),
            Ok(kv(b"c", 5)),
            Ok(kv(b"d", 5)),
            Ok(kv(b"e", 5)),
        ];

        let tombstones = tagged(vec![rt(b"b", b"d", 10)], SeqNo::MAX);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"a".as_ref(), b"d", b"e"]);
    }

    #[test]
    fn items_newer_than_tombstone_survive() {
        let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 10)), Ok(kv(b"c", 3))];

        let tombstones = tagged(vec![rt(b"a", b"z", 5)], SeqNo::MAX);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"b".as_ref()]);
    }

    #[test]
    fn range_end_exclusive_preserves_boundary_key() {
        let items: Vec<crate::Result<InternalValue>> =
            vec![Ok(kv(b"b", 5)), Ok(kv(b"c", 5)), Ok(kv(b"d", 5))];

        let tombstones = tagged(vec![rt(b"b", b"d", 10)], SeqNo::MAX);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"d".as_ref()]);
    }

    #[test]
    fn overlapping_tombstones_suppress_union_of_ranges() {
        let items: Vec<crate::Result<InternalValue>> = vec![
            Ok(kv(b"a", 1)),
            Ok(kv(b"b", 3)),
            Ok(kv(b"c", 6)),
            Ok(kv(b"d", 1)),
        ];

        let tombstones = tagged(vec![rt(b"a", b"c", 5), rt(b"b", b"e", 4)], SeqNo::MAX);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"c".as_ref()]);
    }

    #[test]
    fn tombstone_newer_than_read_seqno_not_visible() {
        let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 3))];

        // RT at seqno 10 with cutoff 5 — not visible
        let tombstones = tagged(vec![rt(b"a", b"z", 10)], 5);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.flatten().collect();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn mixed_cutoffs_suppress_only_visible_source() {
        // Two RTs with same seqno but different per-source cutoffs:
        // RT from source A (cutoff 15) — visible (10 < 15), suppresses kv at seqno 5
        // RT from source B (cutoff 5) — NOT visible (10 >= 5), does not suppress
        let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 5)), Ok(kv(b"x", 5))];

        let tombstones = vec![
            (rt(b"a", b"d", 10), 15), // source A: visible
            (rt(b"w", b"z", 10), 5),  // source B: not visible
        ];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        // "b" suppressed by source-A RT, "x" survives (source-B RT invisible)
        assert_eq!(keys, vec![b"x".as_ref()]);
    }

    #[test]
    fn rev_items_with_range_tombstone_suppress_covered_keys() {
        let items: Vec<crate::Result<InternalValue>> = vec![
            Ok(kv(b"a", 5)),
            Ok(kv(b"b", 5)),
            Ok(kv(b"c", 5)),
            Ok(kv(b"d", 5)),
            Ok(kv(b"e", 5)),
        ];

        let tombstones = tagged(vec![rt(b"b", b"d", 10)], SeqNo::MAX);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.rev().flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"e".as_ref(), b"d", b"a"]);
    }

    #[test]
    fn rev_range_end_exclusive_preserves_boundary_key() {
        let items: Vec<crate::Result<InternalValue>> =
            vec![Ok(kv(b"a", 5)), Ok(kv(b"l", 5)), Ok(kv(b"m", 5))];

        let tombstones = tagged(vec![rt(b"a", b"m", 10)], SeqNo::MAX);
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones);
        let results: Vec<_> = filter.rev().flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"m".as_ref()]);
    }
}
