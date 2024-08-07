use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError, Slice, UserKey,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    ops::{Bound, Deref},
};

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange((UserKey, UserKey));

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

    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        let (start, end) = &self.0;
        key >= *start && key <= *end
    }

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
}

impl Serializable for KeyRange {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
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

impl Deserializable for KeyRange {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let key_min_len = reader.read_u16::<BigEndian>()?;
        let mut key_min = vec![0; key_min_len.into()];
        reader.read_exact(&mut key_min)?;
        let key_min: UserKey = Slice::from(key_min);

        let key_max_len = reader.read_u16::<BigEndian>()?;
        let mut key_max = vec![0; key_max_len.into()];
        reader.read_exact(&mut key_max)?;
        let key_max: UserKey = Slice::from(key_max);

        Ok(Self::new((key_min, key_max)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_key_range(a: u64, b: u64) -> KeyRange {
        KeyRange::new((a.to_be_bytes().into(), b.to_be_bytes().into()))
    }

    fn string_key_range(a: &str, b: &str) -> KeyRange {
        KeyRange::new((a.as_bytes().into(), b.as_bytes().into()))
    }

    mod is_disjoint {
        use super::*;

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
