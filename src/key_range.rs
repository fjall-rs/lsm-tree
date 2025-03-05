// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    Slice, UserKey,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    ops::{Bound, Deref},
};

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange((UserKey, UserKey));

impl std::fmt::Display for KeyRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}<=>{}]",
            String::from_utf8_lossy(&self.0 .0),
            String::from_utf8_lossy(&self.0 .1)
        )
    }
}

impl std::ops::Deref for KeyRange {
    type Target = (UserKey, UserKey);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KeyRange {
    pub fn new(range: (UserKey, UserKey)) -> Self {
        Self(range)
    }

    pub fn empty() -> Self {
        Self((Slice::new(b""), Slice::new(b"")))
    }

    fn min(&self) -> &UserKey {
        &self.0 .0
    }

    fn max(&self) -> &UserKey {
        &self.0 .1
    }

    /// Returns `true` if the list of key ranges is disjoint
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
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let (start, end) = &self.0;
        key >= *start && key <= *end
    }

    /// Returns `true` if the `other` is fully contained in this range.
    pub fn contains_range(&self, other: &Self) -> bool {
        let (start1, end1) = &self.0;
        let (start2, end2) = &other.0;
        start1 <= start2 && end1 >= end2
    }

    /// Returns `true` if the `other` overlaps at least partially with this range.
    pub fn overlaps_with_key_range(&self, other: &Self) -> bool {
        let (start1, end1) = &self.0;
        let (start2, end2) = &other.0;
        end1 >= start2 && start1 <= end2
    }

    pub fn overlaps_with_bounds(&self, bounds: &(Bound<UserKey>, Bound<UserKey>)) -> bool {
        let (lo, hi) = bounds;
        let (my_lo, my_hi) = &self.0;

        if *lo == Bound::Unbounded && *hi == Bound::Unbounded {
            return true;
        }

        if *hi == Bound::Unbounded {
            return match lo {
                Bound::Included(key) => key <= my_hi,
                Bound::Excluded(key) => key < my_hi,
                Bound::Unbounded => panic!("Invalid key range check"),
            };
        }

        if *lo == Bound::Unbounded {
            return match hi {
                Bound::Included(key) => key >= my_lo,
                Bound::Excluded(key) => key > my_lo,
                Bound::Unbounded => panic!("Invalid key range check"),
            };
        }

        let lo_included = match lo {
            Bound::Included(key) => key <= my_hi,
            Bound::Excluded(key) => key < my_hi,
            Bound::Unbounded => panic!("Invalid key range check"),
        };

        let hi_included = match hi {
            Bound::Included(key) => key >= my_lo,
            Bound::Excluded(key) => key > my_lo,
            Bound::Unbounded => panic!("Invalid key range check"),
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

        Self((min.clone(), max.clone()))
    }
}

impl Encode for KeyRange {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // NOTE: Max key size = u16
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.deref().0.len() as u16)?;
        writer.write_all(&self.deref().0)?;

        // NOTE: Max key size = u16
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16::<BigEndian>(self.deref().1.len() as u16)?;
        writer.write_all(&self.deref().1)?;

        Ok(())
    }
}

impl Decode for KeyRange {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let key_min_len = reader.read_u16::<BigEndian>()?;
        let key_min: UserKey = Slice::from_reader(reader, key_min_len.into())?;

        let key_max_len = reader.read_u16::<BigEndian>()?;
        let key_max: UserKey = Slice::from_reader(reader, key_max_len.into())?;

        Ok(Self::new((key_min, key_max)))
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
        let (min, max) = aggregated.0;
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], &*min);
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 10], &*max);
    }

    #[test]
    fn key_range_aggregate_2() {
        let ranges = [
            int_key_range(6, 7),
            int_key_range(0, 2),
            int_key_range(0, 10),
        ];
        let aggregated = KeyRange::aggregate(ranges.iter());
        let (min, max) = aggregated.0;
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], &*min);
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 10], &*max);
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
            let key_range = KeyRange((UserKey::from("key1"), UserKey::from("key5")));
            let bounds = (
                Included(UserKey::from("key1")),
                Included(UserKey::from("key5")),
            );
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn exclusive() {
            let key_range = KeyRange((UserKey::from("key1"), UserKey::from("key5")));
            let bounds = (
                Excluded(UserKey::from("key0")),
                Excluded(UserKey::from("key6")),
            );
            assert!(key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn no_overlap() {
            let key_range = KeyRange((UserKey::from("key1"), UserKey::from("key5")));
            let bounds = (
                Excluded(UserKey::from("key5")),
                Excluded(UserKey::from("key6")),
            );
            assert!(!key_range.overlaps_with_bounds(&bounds));
        }

        #[test]
        fn unbounded() {
            let key_range = KeyRange((UserKey::from("key1"), UserKey::from("key5")));
            let bounds = (Unbounded, Unbounded);
            assert!(key_range.overlaps_with_bounds(&bounds));
        }
    }
}
