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
use std::cmp::Reverse;

/// Wraps a bidirectional KV stream and suppresses entries covered by range tombstones.
pub struct RangeTombstoneFilter<I> {
    inner: I,

    // Forward state
    fwd_tombstones: Vec<RangeTombstone>,
    fwd_idx: usize,
    fwd_active: ActiveTombstoneSet,

    // Reverse state
    rev_tombstones: Vec<RangeTombstone>,
    rev_idx: usize,
    rev_active: ActiveTombstoneSetReverse,
}

impl<I> RangeTombstoneFilter<I> {
    /// Creates a new bidirectional filter.
    ///
    /// `tombstones` is sorted by `(start asc, seqno desc, end asc)` (the natural Ord).
    /// Internally, a second copy sorted by `(end desc, seqno desc)` is created for reverse.
    #[must_use]
    pub fn new(inner: I, fwd_tombstones: Vec<RangeTombstone>, read_seqno: SeqNo) -> Self {
        // Build reverse-sorted copy: (end desc, seqno desc)
        let mut rev_tombstones = fwd_tombstones.clone();
        rev_tombstones.sort_by(|a, b| (&b.end, Reverse(b.seqno)).cmp(&(&a.end, Reverse(a.seqno))));

        Self {
            inner,
            fwd_tombstones,
            fwd_idx: 0,
            fwd_active: ActiveTombstoneSet::new(read_seqno),
            rev_tombstones,
            rev_idx: 0,
            rev_active: ActiveTombstoneSetReverse::new(read_seqno),
        }
    }

    /// Activates forward tombstones whose start <= current_key.
    fn fwd_activate_up_to(&mut self, key: &[u8]) {
        while let Some(rt) = self.fwd_tombstones.get(self.fwd_idx) {
            if rt.start.as_ref() <= key {
                self.fwd_active.activate(rt);
                self.fwd_idx += 1;
            } else {
                break;
            }
        }
    }

    /// Activates reverse tombstones whose end > current_key.
    fn rev_activate_up_to(&mut self, key: &[u8]) {
        while let Some(rt) = self.rev_tombstones.get(self.rev_idx) {
            if rt.end.as_ref() > key {
                self.rev_active.activate(rt);
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

            let kv = match &item {
                Ok(kv) => kv,
                Err(_) => return Some(item),
            };

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

            let kv = match &item {
                Ok(kv) => kv,
                Err(_) => return Some(item),
            };

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

    #[test]
    fn no_tombstones() {
        let items: Vec<crate::Result<InternalValue>> =
            vec![Ok(kv(b"a", 1)), Ok(kv(b"b", 2)), Ok(kv(b"c", 3))];

        let filter = RangeTombstoneFilter::new(items.into_iter(), vec![], SeqNo::MAX);
        let results: Vec<_> = filter.flatten().collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn basic_suppression() {
        // Tombstone [b, d) at seqno 10 suppresses KVs at b and c (seqno < 10)
        let items: Vec<crate::Result<InternalValue>> = vec![
            Ok(kv(b"a", 5)),
            Ok(kv(b"b", 5)),
            Ok(kv(b"c", 5)),
            Ok(kv(b"d", 5)),
            Ok(kv(b"e", 5)),
        ];

        let tombstones = vec![rt(b"b", b"d", 10)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, SeqNo::MAX);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"a".as_ref(), b"d", b"e"]);
    }

    #[test]
    fn tombstone_does_not_suppress_newer_kv() {
        // Tombstone [a, z) at seqno 5 does NOT suppress KV at seqno 10
        let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 10)), Ok(kv(b"c", 3))];

        let tombstones = vec![rt(b"a", b"z", 5)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, SeqNo::MAX);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        // b@10 survives (newer than tombstone), c@3 suppressed (older)
        assert_eq!(keys, vec![b"b".as_ref()]);
    }

    #[test]
    fn half_open_end_exclusive() {
        // Tombstone [b, d) at seqno 10. Key "d" is NOT covered.
        let items: Vec<crate::Result<InternalValue>> =
            vec![Ok(kv(b"b", 5)), Ok(kv(b"c", 5)), Ok(kv(b"d", 5))];

        let tombstones = vec![rt(b"b", b"d", 10)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, SeqNo::MAX);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        assert_eq!(keys, vec![b"d".as_ref()]); // only d survives
    }

    #[test]
    fn multiple_overlapping_tombstones() {
        let items: Vec<crate::Result<InternalValue>> = vec![
            Ok(kv(b"a", 1)),
            Ok(kv(b"b", 3)),
            Ok(kv(b"c", 6)),
            Ok(kv(b"d", 1)),
        ];

        // Two tombstones: [a,c)@5 and [b,e)@4
        let tombstones = vec![rt(b"a", b"c", 5), rt(b"b", b"e", 4)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, SeqNo::MAX);
        let results: Vec<_> = filter.flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        // a@1 suppressed by [a,c)@5
        // b@3 suppressed by [a,c)@5 (max active seqno = 5)
        // c@6 NOT suppressed (seqno 6 > max active 4, since [a,c) expired at c)
        // d@1 suppressed by [b,e)@4
        assert_eq!(keys, vec![b"c".as_ref()]);
    }

    #[test]
    fn tombstone_not_visible_at_read_seqno() {
        // Tombstone at seqno 10, but read_seqno is 5, so tombstone not visible
        let items: Vec<crate::Result<InternalValue>> = vec![Ok(kv(b"b", 3))];

        let tombstones = vec![rt(b"a", b"z", 10)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, 5);
        let results: Vec<_> = filter.flatten().collect();

        // b@3 survives because tombstone@10 is not visible at read_seqno=5
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn reverse_basic_suppression() {
        // Tombstone [b, d) at seqno 10
        let items: Vec<crate::Result<InternalValue>> = vec![
            Ok(kv(b"a", 5)),
            Ok(kv(b"b", 5)),
            Ok(kv(b"c", 5)),
            Ok(kv(b"d", 5)),
            Ok(kv(b"e", 5)),
        ];

        let tombstones = vec![rt(b"b", b"d", 10)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, SeqNo::MAX);
        let results: Vec<_> = filter.rev().flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        // Reverse order: e, d, a (b and c suppressed)
        assert_eq!(keys, vec![b"e".as_ref(), b"d", b"a"]);
    }

    #[test]
    fn reverse_half_open() {
        // Tombstone [a, m) at seqno 10. m is NOT covered.
        let items: Vec<crate::Result<InternalValue>> =
            vec![Ok(kv(b"a", 5)), Ok(kv(b"l", 5)), Ok(kv(b"m", 5))];

        let tombstones = vec![rt(b"a", b"m", 10)];
        let filter = RangeTombstoneFilter::new(items.into_iter(), tombstones, SeqNo::MAX);
        let results: Vec<_> = filter.rev().flatten().collect();

        let keys: Vec<&[u8]> = results.iter().map(|v| v.key.user_key.as_ref()).collect();
        // m survives (end exclusive), a and l are suppressed
        assert_eq!(keys, vec![b"m".as_ref()]);
    }
}
