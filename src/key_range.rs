// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{Slice, UserKey};
use std::ops::Bound;

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange(UserKey, UserKey);

impl KeyRange {
    /// Creates a new key range.
    #[must_use]
    pub fn new((min, max): (UserKey, UserKey)) -> Self {
        Self(min, max)
    }

    /// Creates an empty key range.
    #[must_use]
    pub fn empty() -> Self {
        Self(Slice::empty(), Slice::empty())
    }

    /// Returns the lower bound.
    #[must_use]
    pub fn min(&self) -> &UserKey {
        &self.0
    }

    /// Returns the upper bound.
    #[must_use]
    pub fn max(&self) -> &UserKey {
        &self.1
    }

    fn as_tuple(&self) -> (&UserKey, &UserKey) {
        (self.min(), self.max())
    }

    /// Returns `true` if the list of key ranges is disjoint
    #[must_use]
    pub fn is_disjoint(ranges: &[&Self]) -> bool {
        for (idx, a) in ranges.iter().enumerate() {
            for b in ranges.iter().skip(idx + 1) {
                if a.overlaps_with_key_range(b) {
                    return false;
                }
            }
        }

        true
    }

    /// Returns `true` if the key falls within this key range.
    ///
    /// Uses lexicographic ordering. See [`overlaps_with_key_range_cmp`] and
    /// [`contains_range_cmp`] for custom comparator support; a `contains_key_cmp`
    /// variant can be added when needed (#116).
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let (start, end) = self.as_tuple();
        key >= *start && key <= *end
    }

    /// Returns `true` if the `other` is fully contained in this range.
    #[must_use]
    pub fn contains_range(&self, other: &Self) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        start1 <= start2 && end1 >= end2
    }

    /// Like [`contains_range`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn contains_range_cmp(
        &self,
        other: &Self,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        cmp.compare(start1, start2) != std::cmp::Ordering::Greater
            && cmp.compare(end1, end2) != std::cmp::Ordering::Less
    }

    /// Returns `true` if the `other` overlaps at least partially with this range.
    #[must_use]
    pub fn overlaps_with_key_range(&self, other: &Self) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        end1 >= start2 && start1 <= end2
    }

    /// Like [`overlaps_with_key_range`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn overlaps_with_key_range_cmp(
        &self,
        other: &Self,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        let (start1, end1) = self.as_tuple();
        let (start2, end2) = other.as_tuple();
        cmp.compare(end1, start2) != std::cmp::Ordering::Less
            && cmp.compare(start1, end2) != std::cmp::Ordering::Greater
    }

    /// Like [`Self::overlaps_with_bounds`], but uses a custom comparator for key ordering.
    #[must_use]
    pub fn overlaps_with_bounds_cmp(
        &self,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        use std::cmp::Ordering;

        let (lo, hi) = bounds;
        let (my_lo, my_hi) = self.as_tuple();

        if *lo == Bound::Unbounded && *hi == Bound::Unbounded {
            return true;
        }

        if *hi == Bound::Unbounded {
            return match lo {
                Bound::Included(key) => cmp.compare(key, my_hi) != Ordering::Greater,
                Bound::Excluded(key) => cmp.compare(key, my_hi) == Ordering::Less,
                Bound::Unbounded => unreachable!(),
            };
        }

        if *lo == Bound::Unbounded {
            return match hi {
                Bound::Included(key) => cmp.compare(key, my_lo) != Ordering::Less,
                Bound::Excluded(key) => cmp.compare(key, my_lo) == Ordering::Greater,
                Bound::Unbounded => unreachable!(),
            };
        }

        let lo_included = match lo {
            Bound::Included(key) => cmp.compare(key, my_hi) != Ordering::Greater,
            Bound::Excluded(key) => cmp.compare(key, my_hi) == Ordering::Less,
            Bound::Unbounded => unreachable!(),
        };

        let hi_included = match hi {
            Bound::Included(key) => cmp.compare(key, my_lo) != Ordering::Less,
            Bound::Excluded(key) => cmp.compare(key, my_lo) == Ordering::Greater,
            Bound::Unbounded => unreachable!(),
        };

        lo_included && hi_included
    }

    /// Returns `true` if the ranges overlap partially or fully.
    #[must_use]
    pub fn overlaps_with_bounds(&self, bounds: &(Bound<&[u8]>, Bound<&[u8]>)) -> bool {
        let (lo, hi) = bounds;
        let (my_lo, my_hi) = self.as_tuple();

        if *lo == Bound::Unbounded && *hi == Bound::Unbounded {
            return true;
        }

        if *hi == Bound::Unbounded {
            return match lo {
                Bound::Included(key) => key <= my_hi,
                Bound::Excluded(key) => key < my_hi,
                Bound::Unbounded => unreachable!(),
            };
        }

        if *lo == Bound::Unbounded {
            return match hi {
                Bound::Included(key) => key >= my_lo,
                Bound::Excluded(key) => key > my_lo,
                Bound::Unbounded => unreachable!(),
            };
        }

        let lo_included = match lo {
            Bound::Included(key) => key <= my_hi,
            Bound::Excluded(key) => key < my_hi,
            Bound::Unbounded => unreachable!(),
        };

        let hi_included = match hi {
            Bound::Included(key) => key >= my_lo,
            Bound::Excluded(key) => key > my_lo,
            Bound::Unbounded => unreachable!(),
        };

        lo_included && hi_included
    }

    /// Aggregates a key range.
    pub fn aggregate<'a>(mut iter: impl Iterator<Item = &'a Self>) -> Self {
        let Some(first) = iter.next() else {
            return Self::empty();
        };

        let mut min = first.min();
        let mut max = first.max();

        for other in iter {
            let x = other.min();
            if x < min {
                min = x;
            }

            let x = other.max();
            if x > max {
                max = x;
            }
        }

        Self(min.clone(), max.clone())
    }

    /// Like [`aggregate`], but uses a custom comparator for key ordering.
    pub fn aggregate_cmp<'a>(
        mut iter: impl Iterator<Item = &'a Self>,
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Self {
        let Some(first) = iter.next() else {
            return Self::empty();
        };

        let mut min = first.min();
        let mut max = first.max();

        for other in iter {
            let x = other.min();
            if cmp.compare(x, min) == std::cmp::Ordering::Less {
                min = x;
            }

            let x = other.max();
            if cmp.compare(x, max) == std::cmp::Ordering::Greater {
                max = x;
            }
        }

        Self(min.clone(), max.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    fn int_key_range(a: u64, b: u64) -> KeyRange {
        KeyRange::new((a.to_be_bytes().into(), b.to_be_bytes().into()))
    }

    fn string_key_range(a: &str, b: &str) -> KeyRange {
        KeyRange::new((a.as_bytes().into(), b.as_bytes().into()))
    }

    #[test]
    fn key_range_aggregate_1() {
        let ranges = [
            int_key_range(2, 4),
            int_key_range(0, 4),
            int_key_range(7, 10),
        ];
        let aggregated = KeyRange::aggregate(ranges.iter());
        let (min, max) = aggregated.as_tuple();
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], &**min);
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 10], &**max);
    }

    #[test]
    fn key_range_aggregate_2() {
        let ranges = [
            int_key_range(6, 7),
            int_key_range(0, 2),
            int_key_range(0, 10),
        ];
        let aggregated = KeyRange::aggregate(ranges.iter());
        let (min, max) = aggregated.as_tuple();
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], &**min);
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 10], &**max);
    }

    mod is_disjoint {
        use super::*;
        use test_log::test;

        #[test]
        fn key_range_number() {
            let ranges = [&int_key_range(0, 4), &int_key_range(0, 4)];
            assert!(!KeyRange::is_disjoint(&ranges));
        }

        #[test]
        fn key_range_string() {
            let ranges = [&string_key_range("a", "d"), &string_key_range("g", "z")];
            assert!(KeyRange::is_disjoint(&ranges));
        }

        #[test]
        fn key_range_not_disjoint() {
            let ranges = [&string_key_range("a", "f"), &string_key_range("b", "h")];
            assert!(!KeyRange::is_disjoint(&ranges));

            let ranges = [
                &string_key_range("a", "d"),
                &string_key_range("d", "e"),
                &string_key_range("f", "z"),
            ];
            assert!(!KeyRange::is_disjoint(&ranges));
        }
    }

    mod overflap_key_range {
        use super::*;
        use test_log::test;

        #[test]
        fn key_range_overlap() {
            let a = string_key_range("a", "f");
            let b = string_key_range("b", "h");
            assert!(a.overlaps_with_key_range(&b));
        }

        #[test]
        fn key_range_overlap_edge() {
            let a = string_key_range("a", "f");
            let b = string_key_range("f", "t");
            assert!(a.overlaps_with_key_range(&b));
        }

        #[test]
        fn key_range_no_overlap() {
            let a = string_key_range("a", "f");
            let b = string_key_range("g", "t");
            assert!(!a.overlaps_with_key_range(&b));
        }
    }

    mod overlaps_with_bounds {
        use super::*;
        use std::ops::Bound::{Excluded, Included, Unbounded};
        use test_log::test;

        #[test]
        fn inclusive() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Included(b"key1" as &[u8]), Included(b"key5" as &[u8]));
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn exclusive() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Excluded(b"key0" as &[u8]), Excluded(b"key6" as &[u8]));
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn no_overlap() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Excluded(b"key5" as &[u8]), Excluded(b"key6" as &[u8]));
            assert!(!key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn unbounded() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Unbounded, Unbounded);
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_0() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Unbounded, Excluded(b"key1" as &[u8]));
            assert!(!key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_1() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Excluded(b"key5" as &[u8]), Unbounded);
            assert!(!key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_2() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Unbounded, Included(b"key1" as &[u8]));
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_3() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Included(b"key5" as &[u8]), Unbounded);
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_4() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Unbounded, Included(b"key5" as &[u8]));
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_5() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Unbounded, Included(b"key6" as &[u8]));
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_6() {
            let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
            let bounds = (Included(b"key0" as &[u8]), Unbounded);
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn semi_open_7() {
            let key_range = KeyRange(UserKey::from("key5"), UserKey::from("key8"));
            let bounds = (Unbounded, Excluded(b"key6" as &[u8]));
            assert!(key_range.overlaps_with_bounds(&bounds));
        }
    }

    mod overlaps_with_bounds_cmp {
        use super::*;
        use crate::comparator::UserComparator;
        use std::ops::Bound::{Excluded, Included, Unbounded};
        use test_log::test;

        struct ReverseComparator;

        impl UserComparator for ReverseComparator {
            fn name(&self) -> &'static str {
                "reverse"
            }

            fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                b.cmp(a)
            }
        }

        #[test]
        fn both_unbounded() {
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Unbounded, Unbounded);
            assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn inclusive_reverse_overlap() {
            // Reverse: f < e < d < c < b < a. Key range min=f, max=a.
            // Bounds "e"..="b" in reverse → should overlap.
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Included(b"e" as &[u8]), Included(b"b" as &[u8]));
            assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn no_overlap_reverse() {
            // Key range f..a (reverse). Bounds "z"..="x" → z < x in reverse?
            // No: z and x are both below f in reverse order (reverse: a > b > ... > z).
            // Actually reverse: z < y < x < ... < a. So "z"..="x" is valid.
            // kr min=f, max=a. cmp("z", "a")=reverse of z.cmp(a)=reverse(Greater)=Less.
            // So z < a in reverse → bounds lo "z" is below kr min "f"? Let's check:
            // lo_included: cmp("z", "a"(max)) = reverse(z.cmp(a)) = reverse(Greater) = Less.
            // Less != Greater → true. hi_included: cmp("x", "f"(min)) = reverse(x.cmp(f)) = reverse(Greater) = Less.
            // Less is Less → false. So no overlap.
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Included(b"z" as &[u8]), Included(b"x" as &[u8]));
            assert!(!kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn semi_open_hi_unbounded() {
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Included(b"c" as &[u8]), Unbounded);
            assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn semi_open_lo_unbounded() {
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Unbounded, Included(b"c" as &[u8]));
            assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn exclusive_overlap() {
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            // Excluded "a" as hi → hi must be > min "f" in reverse.
            // cmp("a", "f") = reverse(a.cmp(f)) = reverse(Less) = Greater → true.
            // But excluded "f" as lo → lo must be < max "a" in reverse.
            // cmp("f", "a") = reverse(f.cmp(a)) = reverse(Greater) = Less → true.
            // Both true → overlaps.
            let bounds = (Excluded(b"f" as &[u8]), Excluded(b"a" as &[u8]));
            assert!(kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn semi_open_excluded_no_overlap() {
            // kr min=f, max=a. Excluded "f" as hi, lo unbounded.
            // cmp("f", "f"(min)) = reverse(f.cmp(f)) = Equal. Greater? No → false.
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Unbounded, Excluded(b"f" as &[u8]));
            assert!(!kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }

        #[test]
        fn semi_open_excluded_lo_no_overlap() {
            // kr min=f, max=a. lo=Excluded("a"), hi unbounded.
            // cmp("a", "a"(max)) = Equal. Less? No → false.
            let kr = KeyRange(UserKey::from("f"), UserKey::from("a"));
            let bounds = (Excluded(b"a" as &[u8]), Unbounded);
            assert!(!kr.overlaps_with_bounds_cmp(&bounds, &ReverseComparator));
        }
    }

    #[test]
    fn key_range_contains_key() {
        let key_range = KeyRange(UserKey::from("key1"), UserKey::from("key5"));
        assert!(!key_range.contains_key(b"key0"));
        assert!(!key_range.contains_key(b"key01"));
        assert!(key_range.contains_key(b"key1"));
        assert!(key_range.contains_key(b"key2"));
        assert!(key_range.contains_key(b"key3"));
        assert!(key_range.contains_key(b"key4"));
        assert!(key_range.contains_key(b"key4x"));
        assert!(key_range.contains_key(b"key5"));
        assert!(!key_range.contains_key(b"key5x"));
        assert!(!key_range.contains_key(b"key6"));
    }
}
