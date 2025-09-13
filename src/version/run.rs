// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{binary_search::partition_point, KeyRange};
use std::ops::{Bound, RangeBounds};

pub trait Ranged {
    fn key_range(&self) -> &KeyRange;
}

/// Item inside a run
///
/// May point to an interval [min, max] of segments in the next run.
pub struct Indexed<T: Ranged> {
    inner: T,
    // cascade_indexes: (u32, u32),
}

/* impl<T: Ranged> Indexed<T> {
    pub fn update_cascading(&mut self, next_run: &Run<T>) {
        let kr = self.key_range();
        let range = &**kr.min()..=&**kr.max();

        if let Some((lo, hi)) = next_run.range_indexes(range) {
            // NOTE: There are never 4+ billion segments in a run
            #[allow(clippy::cast_possible_truncation)]
            let interval = (lo as u32, hi as u32);

            self.cascade_indexes = interval;
        } else {
            self.cascade_indexes = (u32::MAX, u32::MAX);
        }
    }
} */

impl<T: Ranged> Ranged for Indexed<T> {
    fn key_range(&self) -> &KeyRange {
        self.inner.key_range()
    }
}

impl<T: Ranged> std::ops::Deref for Indexed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A disjoint run of disk segments
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Run<T: Ranged>(Vec<T>);

impl<T: Ranged> std::ops::Deref for Run<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Ranged> Run<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self(items)
    }

    pub fn push(&mut self, item: T) {
        self.0.push(item);

        self.0
            .sort_by(|a, b| a.key_range().min().cmp(b.key_range().min()));
    }

    pub fn extend(&mut self, items: Vec<T>) {
        self.0.extend(items);
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.0.retain(f);
    }

    pub fn remove(&mut self, idx: usize) -> T {
        self.0.remove(idx)
    }

    /// Returns the segment tha'a,t possibly contains the key.
    pub fn get_for_key(&self, key: &[u8]) -> Option<&T> {
        let idx = partition_point(self, |x| x.key_range().max() < &key);

        self.0.get(idx).filter(|x| x.key_range().min() <= &key)
    }

    /// Returns the run's key range.
    pub fn aggregate_key_range(&self) -> KeyRange {
        // NOTE: Run invariant
        #[allow(clippy::expect_used)]
        let lo = self.first().expect("run should never be empty");

        // NOTE: Run invariant
        #[allow(clippy::expect_used)]
        let hi = self.last().expect("run should never be empty");

        KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
    }

    /// Returns the sub slice of segments in the run that have
    /// a key range overlapping the input key range.
    pub fn get_overlapping<'a>(&'a self, key_range: &'a KeyRange) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes::<crate::Slice, _>(&range) else {
            return &[];
        };

        self.get(lo..=hi).unwrap_or_default()
    }

    /// Returns the sub slice of segments of segments in the run that have
    /// a key range fully contained in the input key range.
    pub fn get_contained<'a>(&'a self, key_range: &KeyRange) -> &'a [T] {
        fn trim_slice<T, F>(s: &[T], pred: F) -> &[T]
        where
            F: Fn(&T) -> bool,
        {
            // find first index where pred holds
            let start = s.iter().position(&pred).unwrap_or(s.len());

            // find last index where pred holds
            let end = s.iter().rposition(&pred).map_or(start, |i| i + 1);

            s.get(start..end).expect("should be in range")
        }

        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes::<crate::Slice, _>(&range) else {
            return &[];
        };

        self.get(lo..=hi)
            .map(|slice| trim_slice(slice, |x| key_range.contains_range(x.key_range())))
            .unwrap_or_default()
    }

    /// Returns the indexes of the interval [min, max] of segments that overlap with a given range.
    pub fn range_overlap_indexes<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        key_range: &R,
    ) -> Option<(usize, usize)> {
        let level = &self.0;

        let lo = match key_range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(start_key) => {
                partition_point(level, |x| x.key_range().max() < start_key)
            }
            Bound::Excluded(start_key) => {
                partition_point(level, |x| x.key_range().max() <= start_key)
            }
        };

        if lo >= level.len() {
            return None;
        }

        // NOTE: We check for level length above
        #[allow(clippy::indexing_slicing)]
        let truncated_level = &level[lo..];

        let hi = match key_range.end_bound() {
            Bound::Unbounded => level.len() - 1,
            Bound::Included(end_key) => {
                // IMPORTANT: We need to add back `lo` because we sliced it off
                let idx = lo + partition_point(truncated_level, |x| x.key_range().min() <= end_key);

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1) // To avoid underflow
            }
            Bound::Excluded(end_key) => {
                // IMPORTANT: We need to add back `lo` because we sliced it off
                let idx = lo + partition_point(truncated_level, |x| x.key_range().min() < end_key);

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1) // To avoid underflow
            }
        };

        if lo > hi {
            return None;
        }

        Some((lo, hi))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use test_log::test;

    #[derive(Clone)]
    struct FakeSegment {
        id: u64,
        key_range: KeyRange,
    }

    impl Ranged for FakeSegment {
        fn key_range(&self) -> &KeyRange {
            &self.key_range
        }
    }

    fn s(id: u64, min: &str, max: &str) -> FakeSegment {
        FakeSegment {
            id,
            key_range: KeyRange::new((min.as_bytes().into(), max.as_bytes().into())),
        }
    }

    #[test]
    fn run_aggregate_key_range() {
        let items = vec![
            s(0, "a", "d"),
            s(1, "e", "j"),
            s(2, "k", "o"),
            s(3, "p", "z"),
        ];
        let run = Run(items);

        assert_eq!(
            KeyRange::new((b"a".into(), b"z".into())),
            run.aggregate_key_range(),
        );
    }

    #[test]
    fn run_point_lookup() {
        let items = vec![
            s(0, "a", "d"),
            s(1, "e", "j"),
            s(2, "k", "o"),
            s(3, "p", "z"),
        ];
        let run = Run(items);

        assert_eq!(0, run.get_for_key(b"a").unwrap().id);
        assert_eq!(0, run.get_for_key(b"aaa").unwrap().id);
        assert_eq!(0, run.get_for_key(b"b").unwrap().id);
        assert_eq!(0, run.get_for_key(b"c").unwrap().id);
        assert_eq!(0, run.get_for_key(b"d").unwrap().id);
        assert_eq!(1, run.get_for_key(b"e").unwrap().id);
        assert_eq!(1, run.get_for_key(b"j").unwrap().id);
        assert_eq!(2, run.get_for_key(b"k").unwrap().id);
        assert_eq!(2, run.get_for_key(b"o").unwrap().id);
        assert_eq!(3, run.get_for_key(b"p").unwrap().id);
        assert_eq!(3, run.get_for_key(b"z").unwrap().id);
        assert!(run.get_for_key(b"zzz").is_none());
    }

    #[test]
    fn run_range_culling() {
        let items = vec![
            s(0, "a", "d"),
            s(1, "e", "j"),
            s(2, "k", "o"),
            s(3, "p", "z"),
        ];
        let run = Run(items);

        assert_eq!(Some((0, 3)), run.range_overlap_indexes::<&[u8], _>(&..));
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes(&(b"a" as &[u8]..=b"a"))
        );
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes(&(b"a" as &[u8]..=b"b"))
        );
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes(&(b"a" as &[u8]..=b"d"))
        );
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes(&(b"d" as &[u8]..=b"d"))
        );
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes(&(b"a" as &[u8]..b"d"))
        );
        assert_eq!(
            Some((0, 1)),
            run.range_overlap_indexes(&(b"a" as &[u8]..=b"g"))
        );
        assert_eq!(
            Some((1, 1)),
            run.range_overlap_indexes(&(b"j" as &[u8]..=b"j"))
        );
        assert_eq!(
            Some((0, 3)),
            run.range_overlap_indexes(&(b"a" as &[u8]..=b"z"))
        );
        assert_eq!(
            Some((3, 3)),
            run.range_overlap_indexes(&(b"z" as &[u8]..=b"zzz"))
        );
        assert_eq!(Some((3, 3)), run.range_overlap_indexes(&(b"z" as &[u8]..)));
        assert!(run
            .range_overlap_indexes(&(b"zzz" as &[u8]..=b"zzzzzzz"))
            .is_none());
    }

    #[test]
    fn run_range_contained() {
        use crate::SegmentId;

        let items = vec![
            s(0, "a", "d"),
            s(1, "e", "j"),
            s(2, "k", "o"),
            s(3, "p", "z"),
        ];
        let run = Run(items);

        assert_eq!(
            &[] as &[SegmentId],
            &*run
                .get_contained(&KeyRange::new((b"a".into(), b"a".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0],
            &*run
                .get_contained(&KeyRange::new((b"a".into(), b"d".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0, 1],
            &*run
                .get_contained(&KeyRange::new((b"a".into(), b"j".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0, 1],
            &*run
                .get_contained(&KeyRange::new((b"a".into(), b"k".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0, 1],
            &*run
                .get_contained(&KeyRange::new((b"a".into(), b"l".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0, 1, 2, 3],
            &*run
                .get_contained(&KeyRange::new((b"a".into(), b"z".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn run_range_overlaps() {
        let items = vec![
            s(0, "a", "d"),
            s(1, "e", "j"),
            s(2, "k", "o"),
            s(3, "p", "z"),
        ];
        let run = Run(items);

        assert_eq!(
            &[0],
            &*run
                .get_overlapping(&KeyRange::new((b"a".into(), b"a".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0],
            &*run
                .get_overlapping(&KeyRange::new((b"d".into(), b"d".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0],
            &*run
                .get_overlapping(&KeyRange::new((b"a".into(), b"d".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0, 1],
            &*run
                .get_overlapping(&KeyRange::new((b"a".into(), b"f".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[0, 1, 2, 3],
            &*run
                .get_overlapping(&KeyRange::new((b"a".into(), b"zzz".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            &[] as &[u64],
            &*run
                .get_overlapping(&KeyRange::new((b"zzz".into(), b"zzzz".into())))
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );
    }
}
