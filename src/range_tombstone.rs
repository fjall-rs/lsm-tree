// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{SeqNo, UserKey};
use std::cmp::Reverse;

/// A range tombstone that deletes all keys in `[start, end)` at a given sequence number.
///
/// Half-open interval: `start` is inclusive, `end` is exclusive.
/// A key `k` is covered iff `start <= k < end`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeTombstone {
    /// Inclusive start bound
    pub start: UserKey,
    /// Exclusive end bound
    pub end: UserKey,
    /// Sequence number at which this tombstone was written
    pub seqno: SeqNo,
}

impl RangeTombstone {
    /// Creates a new range tombstone for `[start, end)` at the given seqno.
    ///
    /// # Panics (debug only)
    ///
    /// Debug-asserts that `start < end`. Callers must validate untrusted input
    /// before constructing a `RangeTombstone`.
    pub fn new(start: UserKey, end: UserKey, seqno: SeqNo) -> Self {
        debug_assert!(start < end, "range tombstone start must be < end");
        Self { start, end, seqno }
    }

    /// Returns `true` if `key` is within `[start, end)`.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.start.as_ref() <= key && key < self.end.as_ref()
    }

    /// Returns `true` if this tombstone is visible at the given read seqno.
    ///
    /// A tombstone is visible when `self.seqno <= read_seqno`.
    pub fn visible_at(&self, read_seqno: SeqNo) -> bool {
        self.seqno <= read_seqno
    }

    /// Returns `true` if this tombstone should suppress a KV with the given seqno
    /// at the given read snapshot.
    ///
    /// Suppress iff: `kv_seqno < self.seqno AND self.contains_key(key) AND self.visible_at(read_seqno)`
    pub fn should_suppress(&self, key: &[u8], kv_seqno: SeqNo, read_seqno: SeqNo) -> bool {
        self.visible_at(read_seqno) && self.contains_key(key) && kv_seqno < self.seqno
    }

    /// Returns the intersection of this tombstone with `[min, max)`, or `None`
    /// if the ranges do not overlap.
    ///
    /// The resulting tombstone has the same seqno as `self`.
    pub fn intersect_opt(&self, min: &[u8], max: &[u8]) -> Option<Self> {
        let new_start_ref = if self.start.as_ref() > min {
            self.start.as_ref()
        } else {
            min
        };
        let new_end_ref = if self.end.as_ref() < max {
            self.end.as_ref()
        } else {
            max
        };

        if new_start_ref < new_end_ref {
            Some(Self {
                start: UserKey::from(new_start_ref),
                end: UserKey::from(new_end_ref),
                seqno: self.seqno,
            })
        } else {
            None
        }
    }

    /// Returns `true` if this tombstone fully covers the key range `[min, max]`.
    ///
    /// "Fully covers" means `self.start <= min` AND `max < self.end`.
    /// This uses the half-open convention: the inclusive `max` must be
    /// strictly less than the exclusive `end`.
    pub fn fully_covers(&self, min: &[u8], max: &[u8]) -> bool {
        self.start.as_ref() <= min && max < self.end.as_ref()
    }
}

/// Ordered by `(start asc, seqno desc, end asc)`.
///
/// The `end` tiebreaker ensures deterministic ordering for debug output
/// and property tests.
impl Ord for RangeTombstone {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.start, Reverse(self.seqno), &self.end).cmp(&(
            &other.start,
            Reverse(other.seqno),
            &other.end,
        ))
    }
}

impl PartialOrd for RangeTombstone {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Information about a covering range tombstone, used for table-skip decisions.
///
/// A covering tombstone fully covers a table's key range and has a seqno
/// greater than the table's max seqno, meaning the entire table can be skipped.
#[derive(Clone, Debug)]
pub struct CoveringRt {
    /// The start key of the covering tombstone (inclusive)
    pub start: UserKey,
    /// The end key of the covering tombstone (exclusive)
    pub end: UserKey,
    /// The seqno of the covering tombstone
    pub seqno: SeqNo,
}

impl CoveringRt {
    /// Returns `true` if this covering tombstone fully covers the given
    /// key range `[min, max]` and has a higher seqno than the table's max.
    pub fn covers_table(&self, table_min: &[u8], table_max: &[u8], table_max_seqno: SeqNo) -> bool {
        self.start.as_ref() <= table_min
            && table_max < self.end.as_ref()
            && self.seqno > table_max_seqno
    }
}

impl From<&RangeTombstone> for CoveringRt {
    fn from(rt: &RangeTombstone) -> Self {
        Self {
            start: rt.start.clone(),
            end: rt.end.clone(),
            seqno: rt.seqno,
        }
    }
}

/// Computes the upper bound exclusive key for use in range queries.
///
/// Given a key, returns the next key in lexicographic order by appending `0x00`.
/// This is useful for converting inclusive upper bounds to exclusive ones
/// in range-cover queries.
pub fn upper_bound_exclusive(key: &[u8]) -> UserKey {
    let mut result = Vec::with_capacity(key.len() + 1);
    result.extend_from_slice(key);
    result.push(0x00);
    UserKey::from(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rt(start: &[u8], end: &[u8], seqno: SeqNo) -> RangeTombstone {
        RangeTombstone::new(UserKey::from(start), UserKey::from(end), seqno)
    }

    #[test]
    fn contains_key_inclusive_start() {
        let t = rt(b"b", b"d", 10);
        assert!(t.contains_key(b"b"));
    }

    #[test]
    fn contains_key_exclusive_end() {
        let t = rt(b"b", b"d", 10);
        assert!(!t.contains_key(b"d"));
    }

    #[test]
    fn contains_key_middle() {
        let t = rt(b"b", b"d", 10);
        assert!(t.contains_key(b"c"));
    }

    #[test]
    fn contains_key_before_start() {
        let t = rt(b"b", b"d", 10);
        assert!(!t.contains_key(b"a"));
    }

    #[test]
    fn visible_at_equal() {
        let t = rt(b"a", b"z", 10);
        assert!(t.visible_at(10));
    }

    #[test]
    fn visible_at_higher() {
        let t = rt(b"a", b"z", 10);
        assert!(t.visible_at(20));
    }

    #[test]
    fn not_visible_at_lower() {
        let t = rt(b"a", b"z", 10);
        assert!(!t.visible_at(9));
    }

    #[test]
    fn should_suppress_yes() {
        let t = rt(b"b", b"d", 10);
        assert!(t.should_suppress(b"c", 5, 10));
    }

    #[test]
    fn should_suppress_no_newer_kv() {
        let t = rt(b"b", b"d", 10);
        assert!(!t.should_suppress(b"c", 15, 20));
    }

    #[test]
    fn should_suppress_no_not_visible() {
        let t = rt(b"b", b"d", 10);
        assert!(!t.should_suppress(b"c", 5, 9));
    }

    #[test]
    fn should_suppress_no_outside_range() {
        let t = rt(b"b", b"d", 10);
        assert!(!t.should_suppress(b"e", 5, 10));
    }

    #[test]
    fn ordering_by_start_asc() {
        let a = rt(b"a", b"z", 10);
        let b = rt(b"b", b"z", 10);
        assert!(a < b);
    }

    #[test]
    fn ordering_by_seqno_desc() {
        let a = rt(b"a", b"z", 20);
        let b = rt(b"a", b"z", 10);
        assert!(a < b); // higher seqno comes first
    }

    #[test]
    fn ordering_by_end_asc_tiebreaker() {
        let a = rt(b"a", b"m", 10);
        let b = rt(b"a", b"z", 10);
        assert!(a < b);
    }

    #[test]
    fn intersect_overlap() {
        let t = rt(b"b", b"y", 10);
        let clipped = t.intersect_opt(b"d", b"g").unwrap();
        assert_eq!(clipped.start.as_ref(), b"d");
        assert_eq!(clipped.end.as_ref(), b"g");
        assert_eq!(clipped.seqno, 10);
    }

    #[test]
    fn intersect_no_overlap() {
        let t = rt(b"b", b"d", 10);
        assert!(t.intersect_opt(b"e", b"g").is_none());
    }

    #[test]
    fn intersect_partial_left() {
        let t = rt(b"b", b"f", 10);
        let clipped = t.intersect_opt(b"a", b"d").unwrap();
        assert_eq!(clipped.start.as_ref(), b"b");
        assert_eq!(clipped.end.as_ref(), b"d");
    }

    #[test]
    fn intersect_partial_right() {
        let t = rt(b"b", b"f", 10);
        let clipped = t.intersect_opt(b"d", b"z").unwrap();
        assert_eq!(clipped.start.as_ref(), b"d");
        assert_eq!(clipped.end.as_ref(), b"f");
    }

    #[test]
    fn fully_covers_yes() {
        let t = rt(b"a", b"z", 10);
        assert!(t.fully_covers(b"b", b"y"));
    }

    #[test]
    fn fully_covers_exact_start() {
        let t = rt(b"a", b"z", 10);
        assert!(t.fully_covers(b"a", b"y"));
    }

    #[test]
    fn fully_covers_no_end_equal() {
        let t = rt(b"a", b"z", 10);
        // max == end is not covered (half-open)
        assert!(!t.fully_covers(b"a", b"z"));
    }

    #[test]
    fn fully_covers_no_start_before() {
        let t = rt(b"b", b"z", 10);
        assert!(!t.fully_covers(b"a", b"y"));
    }

    #[test]
    fn covering_rt_covers_table() {
        let crt = CoveringRt {
            start: UserKey::from(b"a" as &[u8]),
            end: UserKey::from(b"z" as &[u8]),
            seqno: 100,
        };
        assert!(crt.covers_table(b"b", b"y", 50));
    }

    #[test]
    fn covering_rt_no_cover_seqno_too_low() {
        let crt = CoveringRt {
            start: UserKey::from(b"a" as &[u8]),
            end: UserKey::from(b"z" as &[u8]),
            seqno: 50,
        };
        assert!(!crt.covers_table(b"b", b"y", 100));
    }

    #[test]
    fn upper_bound_exclusive_appends_zero() {
        let key = b"hello";
        let result = upper_bound_exclusive(key);
        assert_eq!(result.as_ref(), b"hello\x00");
    }
}
