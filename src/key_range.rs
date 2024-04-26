use crate::UserKey;
use serde::{Deserialize, Serialize};
use std::ops::Bound;

/// A key range in the format of [min, max] (inclusive on both sides)
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
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

    /// Returns `true` if the list of key ranges is disjunct
    pub fn is_disjunct(ranges: &[Self]) -> bool {
        for i in 0..ranges.len() {
            let a = ranges.get(i).expect("should exist");

            for j in (i + 1)..ranges.len() {
                let b = ranges.get(j).expect("should exist");

                if a.overlaps_with_key_range(b) {
                    return false;
                }
            }
        }

        true
    }

    pub(crate) fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        let key = key.as_ref();
        let (start, end) = &self.0;
        key >= start && key <= end
    }

    pub fn overlaps_with_key_range(&self, other: &Self) -> bool {
        let (start1, end1) = &self.0;
        let (start2, end2) = &other.0;
        end1 >= start2 && start1 <= end2
    }

    // TODO: unit tests
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

    pub fn contains_prefix(&self, prefix: &[u8]) -> bool {
        if prefix.is_empty() {
            return true;
        }

        let (start, end) = &self.0;
        (&**start <= prefix && prefix <= end)
            || start.starts_with(prefix)
            || end.starts_with(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_range_disjunct() {
        let ranges = [
            KeyRange::new(((*b"a").into(), (*b"d").into())),
            KeyRange::new(((*b"g").into(), (*b"z").into())),
        ];
        assert!(KeyRange::is_disjunct(&ranges));
    }

    #[test]
    fn key_range_overlap() {
        let a = KeyRange::new(((*b"a").into(), (*b"f").into()));
        let b = KeyRange::new(((*b"b").into(), (*b"h").into()));
        assert!(a.overlaps_with_key_range(&b));
    }

    #[test]
    fn key_range_overlap_edge() {
        let a = KeyRange::new(((*b"a").into(), (*b"f").into()));
        let b = KeyRange::new(((*b"f").into(), (*b"t").into()));
        assert!(a.overlaps_with_key_range(&b));
    }

    #[test]
    fn key_range_no_overlap() {
        let a = KeyRange::new(((*b"a").into(), (*b"f").into()));
        let b = KeyRange::new(((*b"g").into(), (*b"t").into()));
        assert!(!a.overlaps_with_key_range(&b));
    }

    #[test]
    fn key_range_not_disjunct() {
        let ranges = [
            KeyRange::new(((*b"a").into(), (*b"f").into())),
            KeyRange::new(((*b"b").into(), (*b"h").into())),
        ];
        assert!(!KeyRange::is_disjunct(&ranges));

        let ranges = [
            KeyRange::new(((*b"a").into(), (*b"d").into())),
            KeyRange::new(((*b"d").into(), (*b"e").into())),
            KeyRange::new(((*b"f").into(), (*b"z").into())),
        ];
        assert!(!KeyRange::is_disjunct(&ranges));
    }

    #[test]
    fn key_range_contains_prefix() {
        let key_range = KeyRange::new(((*b"a").into(), (*b"d").into()));
        assert!(key_range.contains_prefix(b"b"));

        let key_range: KeyRange = KeyRange::new(((*b"d").into(), (*b"h").into()));
        assert!(!key_range.contains_prefix(b"b"));

        let key_range = KeyRange::new(((*b"a").into(), (*b"d").into()));
        assert!(key_range.contains_prefix(b"abc"));

        let key_range = KeyRange::new(((*b"a").into(), (*b"z").into()));
        assert!(key_range.contains_prefix(b"abc"));

        let key_range = KeyRange::new(((*b"d").into(), (*b"h").into()));
        assert!(!key_range.contains_prefix(b"abc"));

        let key_range = KeyRange::new(((*b"a").into(), (*b"z").into()));
        assert!(key_range.contains_prefix(b""));

        let key_range = KeyRange::new(((*b"a").into(), (*b"c").into()));
        assert!(!key_range.contains_prefix(b"def"));

        let key_range = KeyRange::new(((*b"a").into(), (*b"d").into()));
        assert!(key_range.contains_prefix(b"bbb"));

        let key_range = KeyRange::new(((*b"a").into(), (*b"d").into()));
        assert!(!key_range.contains_prefix(b"da"));

        let key_range = KeyRange::new(((*b"abc").into(), (*b"b").into()));
        assert!(key_range.contains_prefix(b"a"));
    }
}
