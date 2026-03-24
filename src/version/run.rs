// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::comparator::UserComparator;
use crate::KeyRange;
use std::ops::{Bound, RangeBounds};

pub trait Ranged {
    fn key_range(&self) -> &KeyRange;
}

/// Item inside a run
///
/// May point to an interval [min, max] of tables in the next run.
#[expect(dead_code, reason = "planned for cascading index optimization")]
pub struct Indexed<T: Ranged> {
    inner: T,
    // cascade_indexes: (u32, u32),
}

/* impl<T: Ranged> Indexed<T> {
    pub fn update_cascading(&mut self, next_run: &Run<T>) {
        let kr = self.key_range();
        let range = &**kr.min()..=&**kr.max();

        if let Some((lo, hi)) = next_run.range_indexes(range) {
            // NOTE: There are never 4+ billion tables in a run
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

/// A disjoint run of tables
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Run<T: Ranged>(Vec<T>);

impl<T: Ranged> std::ops::Deref for Run<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Returns the span between the first and last element matching `pred`.
///
/// Note: non-matching elements *between* matches are included. This is
/// correct for `get_contained` / `get_contained_cmp` where the overlap
/// window guarantees contiguity of matching tables.
fn trim_slice<T, F>(s: &[T], pred: F) -> &[T]
where
    F: Fn(&T) -> bool,
{
    let start = s.iter().position(&pred).unwrap_or(s.len());
    let end = s.iter().rposition(&pred).map_or(start, |i| i + 1);

    #[expect(
        clippy::expect_used,
        reason = "start..end are derived from position/rposition on the same slice"
    )]
    s.get(start..end).expect("should be in range")
}

impl<T: Ranged> Run<T> {
    pub fn new(items: Vec<T>) -> Option<Self> {
        if items.is_empty() {
            None
        } else {
            Some(Self(items))
        }
    }

    pub fn inner_mut(&mut self) -> &mut Vec<T> {
        &mut self.0
    }

    /// Pushes a table into the run and re-sorts by min key using lexicographic
    /// byte ordering.
    ///
    /// Only correct when the tree uses the default (lexicographic) comparator.
    /// For custom comparators, use [`push_cmp`] instead.
    pub fn push_lexicographic(&mut self, item: T) {
        self.0.push(item);

        self.0
            .sort_by(|a, b| a.key_range().min().cmp(b.key_range().min()));
    }

    /// Pushes a table and re-sorts using a custom comparator for key ordering.
    ///
    /// Re-sorts the entire run on each call (mirrors [`push_lexicographic`]
    /// behavior). Acceptable for typical run sizes (<100 tables); for bulk
    /// insertion use [`extend`] followed by [`sort_by_cmp`].
    pub fn push_cmp(&mut self, item: T, cmp: &dyn UserComparator) {
        self.0.push(item);
        self.sort_by_cmp(cmp);
    }

    /// Sorts the run by min key using the provided user comparator.
    ///
    /// Use after [`extend`] to re-establish ordering in a single pass.
    pub fn sort_by_cmp(&mut self, cmp: &dyn UserComparator) {
        self.0
            .sort_by(|a, b| cmp.compare(a.key_range().min(), b.key_range().min()));
    }

    /// Appends items without re-sorting. Callers must ensure the run remains
    /// sorted (e.g. via [`sort_by_cmp`] after all items are added).
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

    /// Returns the table that may possibly contains the given key.
    pub fn get_for_key(&self, key: &[u8]) -> Option<&T> {
        let idx = self.partition_point(|x| x.key_range().max() < &key);

        self.0.get(idx).filter(|x| x.key_range().min() <= &key)
    }

    /// Like [`get_for_key`], but uses a custom comparator for key ordering.
    ///
    /// # Precondition (guaranteed by construction)
    ///
    /// Tables within a run are sorted by `key_range` in comparator order.
    /// This holds because tables are flushed from comparator-sorted memtables
    /// and compaction preserves the ordering. The binary search here must
    /// use the same comparator to maintain the invariant.
    pub fn get_for_key_cmp(
        &self,
        key: &[u8],
        cmp: &dyn crate::comparator::UserComparator,
    ) -> Option<&T> {
        let idx = self
            .partition_point(|x| cmp.compare(x.key_range().max(), key) == std::cmp::Ordering::Less);

        self.0
            .get(idx)
            .filter(|x| cmp.compare(x.key_range().min(), key) != std::cmp::Ordering::Greater)
    }

    /// Returns the run's key range.
    pub fn aggregate_key_range(&self) -> KeyRange {
        #[expect(clippy::expect_used, reason = "by definition, runs are never empty")]
        let lo = self.first().expect("run should never be empty");

        #[expect(clippy::expect_used, reason = "by definition, runs are never empty")]
        let hi = self.last().expect("run should never be empty");

        KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
    }

    /// Returns the sub slice of tables in the run that have
    /// a key range overlapping the input key range.
    ///
    /// Uses lexicographic ordering. For custom comparators, use [`get_overlapping_cmp`].
    pub fn get_overlapping<'a>(&'a self, key_range: &'a KeyRange) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes::<crate::Slice, _>(&range) else {
            return &[];
        };

        self.get(lo..=hi).unwrap_or_default()
    }

    /// Like [`get_overlapping`], but uses a custom comparator for key ordering.
    ///
    /// Lifetime on `key_range` mirrors [`get_overlapping`] for API consistency.
    pub fn get_overlapping_cmp<'a>(
        &'a self,
        key_range: &'a KeyRange,
        cmp: &dyn UserComparator,
    ) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes_cmp::<crate::Slice, _>(&range, cmp) else {
            return &[];
        };

        self.get(lo..=hi).unwrap_or_default()
    }

    /// Returns the sub slice of tables of tables in the run that have
    /// a key range fully contained in the input key range.
    pub fn get_contained<'a>(&'a self, key_range: &KeyRange) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes::<crate::Slice, _>(&range) else {
            return &[];
        };

        self.get(lo..=hi)
            .map(|slice| trim_slice(slice, |x| key_range.contains_range(x.key_range())))
            .unwrap_or_default()
    }

    /// Like [`get_contained`], but uses a custom comparator for key ordering.
    pub fn get_contained_cmp<'a>(
        &'a self,
        key_range: &KeyRange,
        cmp: &dyn UserComparator,
    ) -> &'a [T] {
        let range = key_range.min()..=key_range.max();

        let Some((lo, hi)) = self.range_overlap_indexes_cmp::<crate::Slice, _>(&range, cmp) else {
            return &[];
        };

        self.get(lo..=hi)
            .map(|slice| trim_slice(slice, |x| key_range.contains_range_cmp(x.key_range(), cmp)))
            .unwrap_or_default()
    }

    /// Returns the indexes of the interval [min, max] of tables that overlap with a given range.
    pub fn range_overlap_indexes<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        key_range: &R,
    ) -> Option<(usize, usize)> {
        let level = &self.0;

        let lo = match key_range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(start_key) => {
                level.partition_point(|x| x.key_range().max() < start_key)
            }
            Bound::Excluded(start_key) => {
                level.partition_point(|x| x.key_range().max() <= start_key)
            }
        };

        if lo >= level.len() {
            return None;
        }

        // NOTE: We check for level length above
        #[expect(clippy::indexing_slicing)]
        let truncated_level = &level[lo..];

        let hi = match key_range.end_bound() {
            Bound::Unbounded => level.len() - 1,
            Bound::Included(end_key) => {
                // IMPORTANT: We need to add back `lo` because we sliced it off
                let idx = lo + truncated_level.partition_point(|x| x.key_range().min() <= end_key);

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1) // To avoid underflow
            }
            Bound::Excluded(end_key) => {
                // IMPORTANT: We need to add back `lo` because we sliced it off
                let idx = lo + truncated_level.partition_point(|x| x.key_range().min() < end_key);

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

    /// Like [`range_overlap_indexes`], but uses a custom comparator for key ordering.
    pub fn range_overlap_indexes_cmp<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        key_range: &R,
        cmp: &dyn UserComparator,
    ) -> Option<(usize, usize)> {
        use std::cmp::Ordering;

        let level = &self.0;

        let lo = match key_range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(start_key) => level.partition_point(|x| {
                cmp.compare(x.key_range().max(), start_key.as_ref()) == Ordering::Less
            }),
            Bound::Excluded(start_key) => level.partition_point(|x| {
                cmp.compare(x.key_range().max(), start_key.as_ref()) != Ordering::Greater
            }),
        };

        if lo >= level.len() {
            return None;
        }

        #[expect(clippy::indexing_slicing)]
        let truncated_level = &level[lo..];

        let hi = match key_range.end_bound() {
            Bound::Unbounded => level.len() - 1,
            Bound::Included(end_key) => {
                let idx = lo
                    + truncated_level.partition_point(|x| {
                        cmp.compare(x.key_range().min(), end_key.as_ref()) != Ordering::Greater
                    });

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1)
            }
            Bound::Excluded(end_key) => {
                let idx = lo
                    + truncated_level.partition_point(|x| {
                        cmp.compare(x.key_range().min(), end_key.as_ref()) == Ordering::Less
                    });

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1)
            }
        };

        if lo > hi {
            return None;
        }

        Some((lo, hi))
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    use test_log::test;

    use crate::comparator::DefaultUserComparator;

    #[derive(Clone)]
    struct FakeTable {
        id: u64,
        key_range: KeyRange,
    }

    impl Ranged for FakeTable {
        fn key_range(&self) -> &KeyRange {
            &self.key_range
        }
    }

    fn s(id: u64, min: &str, max: &str) -> FakeTable {
        FakeTable {
            id,
            key_range: KeyRange::new((min.as_bytes().into(), max.as_bytes().into())),
        }
    }

    /// Reverse comparator for testing non-lexicographic ordering.
    struct ReverseCmp;

    impl UserComparator for ReverseCmp {
        fn name(&self) -> &'static str {
            "reverse"
        }

        fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            b.cmp(a)
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
        use crate::TableId;

        let items = vec![
            s(0, "a", "d"),
            s(1, "e", "j"),
            s(2, "k", "o"),
            s(3, "p", "z"),
        ];
        let run = Run(items);

        assert_eq!(
            &[] as &[TableId],
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
    fn run_range_contained_cmp_reverse() {
        use crate::comparator::UserComparator;
        use crate::TableId;

        struct ReverseCmp;
        impl UserComparator for ReverseCmp {
            fn name(&self) -> &'static str {
                "reverse"
            }
            fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
                b.cmp(a)
            }
        }

        // Reverse comparator: tables store (comparator_min, comparator_max).
        // In reverse order "z" < "p" < "o" < ... < "a", so key ranges are
        // (z,p), (o,k), (j,e), (d,a) — matching production SST metadata.
        let items = vec![
            s(0, "z", "p"),
            s(1, "o", "k"),
            s(2, "j", "e"),
            s(3, "d", "a"),
        ];
        let run = Run(items);
        let cmp = ReverseCmp;

        // Full range contains all
        assert_eq!(
            &[0, 1, 2, 3],
            &*run
                .get_contained_cmp(&KeyRange::new((b"z".into(), b"a".into())), &cmp)
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        // Partial: z..k contains tables 0 and 1
        assert_eq!(
            &[0, 1],
            &*run
                .get_contained_cmp(&KeyRange::new((b"z".into(), b"k".into())), &cmp)
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        // Exact match: single table
        assert_eq!(
            &[2 as TableId],
            &*run
                .get_contained_cmp(&KeyRange::new((b"j".into(), b"e".into())), &cmp)
                .iter()
                .map(|x| x.id)
                .collect::<Vec<_>>(),
        );

        // No table fully contained
        assert_eq!(
            &[] as &[TableId],
            &*run
                .get_contained_cmp(&KeyRange::new((b"z".into(), b"z".into())), &cmp)
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

    #[test]
    fn push_lexicographic_sorts_by_min_key() {
        let mut run = Run::new(vec![s(0, "e", "j")]).unwrap();

        // Insert a table whose min key is lexicographically before "e"
        run.push_lexicographic(s(1, "a", "d"));
        assert_eq!(1, run[0].id); // "a" sorts first
        assert_eq!(0, run[1].id); // "e" sorts second
    }

    #[test]
    fn push_cmp_sorts_by_comparator() {
        let mut run = Run::new(vec![s(0, "a", "d")]).unwrap();

        // With default (lexicographic) comparator, "e" > "a" → appended after
        run.push_cmp(s(1, "e", "j"), &DefaultUserComparator);
        assert_eq!(0, run[0].id);
        assert_eq!(1, run[1].id);

        // With reverse comparator, "k" is "smaller" than "e" → sorted before
        let mut rev_run = Run::new(vec![s(0, "e", "j")]).unwrap();
        rev_run.push_cmp(s(1, "k", "o"), &ReverseCmp);
        // Reverse order: k > e lexicographically, but ReverseCmp reverses → k < e
        assert_eq!(1, rev_run[0].id); // "k" sorts first in reverse
        assert_eq!(0, rev_run[1].id); // "e" sorts second in reverse
    }

    #[test]
    fn get_overlapping_cmp_reverse() {
        // With reverse comparator, SST key ranges store (comparator-min, comparator-max).
        // Reverse comparator-min is the lexicographic max, so min > max lexicographically.
        // Run sorted by comparator-min: z, o, j, d (descending lexicographic).
        let items = vec![
            s(3, "z", "p"),
            s(2, "o", "k"),
            s(1, "j", "e"),
            s(0, "d", "a"),
        ];
        let run = Run(items);

        let result = run
            .get_overlapping_cmp(&KeyRange::new((b"j".into(), b"j".into())), &ReverseCmp)
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>();
        assert_eq!(&[1], &*result);

        let result = run
            .get_overlapping_cmp(&KeyRange::new((b"o".into(), b"e".into())), &ReverseCmp)
            .iter()
            .map(|x| x.id)
            .collect::<Vec<_>>();
        assert_eq!(&[2, 1], &*result);
    }

    #[test]
    fn range_overlap_indexes_cmp_reverse() {
        let items = vec![
            s(3, "z", "p"),
            s(2, "o", "k"),
            s(1, "j", "e"),
            s(0, "d", "a"),
        ];
        let run = Run(items);
        let cmp = ReverseCmp;

        assert_eq!(
            Some((0, 3)),
            run.range_overlap_indexes_cmp::<&[u8], _>(&.., &cmp)
        );

        // Inclusive range covering one table (z..=p in reverse = first table)
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes_cmp(&(b"z" as &[u8]..=b"p"), &cmp)
        );

        // Inclusive range covering two tables (z..=k)
        assert_eq!(
            Some((0, 1)),
            run.range_overlap_indexes_cmp(&(b"z" as &[u8]..=b"k"), &cmp)
        );

        // Out of range (beyond last table in reverse order)
        assert!(run
            .range_overlap_indexes_cmp(&(b"\x00" as &[u8]..=b"\x00"), &cmp)
            .is_none());

        // Exclusive start bound: skip first table (z..p), start from second (o..k)
        let bounds_excl_start: (Bound<&[u8]>, Bound<&[u8]>) =
            (Bound::Excluded(b"p"), Bound::Included(b"a"));
        assert_eq!(
            Some((1, 3)),
            run.range_overlap_indexes_cmp::<&[u8], _>(&bounds_excl_start, &cmp)
        );

        // Exclusive end bound: include first table only
        let bounds_excl_end: (Bound<&[u8]>, Bound<&[u8]>) =
            (Bound::Included(b"z"), Bound::Excluded(b"o"));
        assert_eq!(
            Some((0, 0)),
            run.range_overlap_indexes_cmp::<&[u8], _>(&bounds_excl_end, &cmp)
        );

        // Semi-open range (start..): Included start, Unbounded end
        assert_eq!(
            Some((2, 3)),
            run.range_overlap_indexes_cmp(&(b"j" as &[u8]..), &cmp)
        );
    }

    #[test]
    fn get_for_key_cmp_reverse() {
        let items = vec![
            s(3, "z", "p"),
            s(2, "o", "k"),
            s(1, "j", "e"),
            s(0, "d", "a"),
        ];
        let run = Run(items);
        let cmp = ReverseCmp;

        assert_eq!(3, run.get_for_key_cmp(b"z", &cmp).unwrap().id);
        assert_eq!(3, run.get_for_key_cmp(b"p", &cmp).unwrap().id);
        assert_eq!(2, run.get_for_key_cmp(b"k", &cmp).unwrap().id);
        assert_eq!(1, run.get_for_key_cmp(b"e", &cmp).unwrap().id);
        assert_eq!(0, run.get_for_key_cmp(b"a", &cmp).unwrap().id);
        assert!(run.get_for_key_cmp(b"\x00", &cmp).is_none());
    }
}
