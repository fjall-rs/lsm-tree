// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    prefix::SharedPrefixExtractor, version::Run, BoxedIterator, InternalValue, Table, UserKey,
};
use std::ops::Bound::{self};
use std::{
    ops::{Deref, RangeBounds},
    sync::Arc,
};

/// Reads through a disjoint run
pub struct RunReader {
    run: Arc<Run<Table>>,
    lo: usize,
    hi: usize,
    lo_reader: Option<BoxedIterator<'static>>,
    hi_reader: Option<BoxedIterator<'static>>,

    // Owned range bounds for creating new per-table readers during iteration
    range_start: Bound<UserKey>,
    range_end: Bound<UserKey>,

    // Optional extractor for prefix-aware pruning during lazy advancement
    extractor: Option<SharedPrefixExtractor>,
}

impl RunReader {
    /// Creates a run reader over a disjoint set of tables. Returns None when up-front
    /// prefix filter pruning determines that no table in the run may contain keys for the range.
    /// Uses common-prefix pruning only; per-table skipping happens lazily during iteration.
    #[must_use]
    pub fn new<R: RangeBounds<UserKey>>(
        run: Arc<Run<Table>>,
        range: R,
        extractor: Option<SharedPrefixExtractor>,
    ) -> Option<Self> {
        assert!(!run.is_empty(), "level reader cannot read empty level");

        let (lo, hi) = run.range_overlap_indexes(&range)?;

        // Compute pruning prefix: only when both bounds' first extracted prefixes exist and are equal.
        let common_prefix = if let Some(ex) = extractor.as_ref() {
            let start_first = match range.start_bound() {
                Bound::Included(uk) | Bound::Excluded(uk) => {
                    ex.extract_first(uk.as_ref()).map(<[u8]>::to_vec)
                }
                Bound::Unbounded => None,
            };
            let end_first = match range.end_bound() {
                Bound::Included(uk) | Bound::Excluded(uk) => {
                    ex.extract_first(uk.as_ref()).map(<[u8]>::to_vec)
                }
                Bound::Unbounded => None,
            };
            match (start_first, end_first) {
                (Some(s), Some(e)) if s == e => Some(s),
                _ => None,
            }
        } else {
            None
        };

        // Early optimization
        if let Some(ex) = extractor.clone() {
            // Compute start bound key once
            let start_key = match range.start_bound() {
                std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => Some(k.as_ref()),
                std::ops::Bound::Unbounded => None,
            };

            // Common-prefix pruning when bounds share the same extracted prefix
            if common_prefix.is_some() {
                const MAX_UPFRONT_CHECKS: usize = 10;
                let mut checks = 0usize;
                let mut has_potential_match = false;

                for idx in lo..=hi {
                    let table = run.deref().get(idx).expect("should exist");
                    // SAFETY INVARIANT: range_overlap_indexes uses binary search on
                    // table min/max keys and is exact for disjoint sorted runs —
                    // every table in lo..=hi genuinely overlaps the query range.
                    // If this invariant were ever violated (e.g. by a future refactor),
                    // the impact is benign: table.range() would return an empty iterator
                    // (no data corruption, just wasted I/O).
                    debug_assert!(
                        table.check_key_range_overlap(&(
                            range.start_bound().map(AsRef::as_ref),
                            range.end_bound().map(AsRef::as_ref),
                        )),
                        "range_overlap_indexes returned a non-overlapping table in upfront pruning"
                    );

                    // common_prefix.is_some() guarantees both bounds are
                    // Included/Excluded (not Unbounded), so start_key is always Some.
                    let probe =
                        start_key.expect("common_prefix requires both bounds to be concrete");
                    if !matches!(
                        table.maybe_contains_prefix(probe, ex.as_ref()),
                        Ok(Some(false))
                    ) {
                        has_potential_match = true;
                        break;
                    }

                    checks += 1;
                    if checks >= MAX_UPFRONT_CHECKS {
                        has_potential_match = true;
                        break;
                    }
                }

                if !has_potential_match {
                    return None;
                }
            }
        }

        Some(Self::culled(run, range, (Some(lo), Some(hi)), extractor))
    }

    /// Creates a run reader with precomputed overlap indices.
    ///
    /// This variant assumes the caller already determined the overlapping table
    /// indices. It initializes boundary table readers and
    /// performs lazy per-table prefix-filter skipping during iteration.
    #[must_use]
    pub fn culled<R: RangeBounds<UserKey>>(
        run: Arc<Run<Table>>,
        range: R,
        (lo, hi): (Option<usize>, Option<usize>),
        extractor: Option<SharedPrefixExtractor>,
    ) -> Self {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);

        // Materialize owned range bounds for reuse when creating readers for other tables
        let owned_start: std::ops::Bound<UserKey> = match range.start_bound() {
            Included(k) => Included(k.clone()),
            Excluded(k) => Excluded(k.clone()),
            Unbounded => Unbounded,
        };
        let owned_end: std::ops::Bound<UserKey> = match range.end_bound() {
            Included(k) => Included(k.clone()),
            Excluded(k) => Excluded(k.clone()),
            Unbounded => Unbounded,
        };

        // TODO: lazily init readers?
        #[expect(
            clippy::expect_used,
            reason = "we trust the caller to pass valid indexes"
        )]
        let lo_table = run.deref().get(lo).expect("should exist");
        let lo_reader = lo_table.range((owned_start.clone(), owned_end.clone()));

        let hi_reader = if hi > lo {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let hi_table = run.deref().get(hi).expect("should exist");
            Some(hi_table.range((owned_start.clone(), owned_end.clone())))
        } else {
            None
        };

        Self {
            run,
            lo,
            hi,
            lo_reader: Some(Box::new(lo_reader)),
            hi_reader: hi_reader.map(|x| Box::new(x) as BoxedIterator),
            range_start: owned_start,
            range_end: owned_end,
            extractor,
        }
    }
}

impl Iterator for RunReader {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(lo_reader) = &mut self.lo_reader {
                if let Some(item) = lo_reader.next() {
                    return Some(item);
                }

                // NOTE: Lo reader is empty, get next one
                self.lo_reader = None;
                self.lo += 1;

                if self.lo < self.hi {
                    // Lazily advance to the next table that overlaps the key range
                    loop {
                        if self.lo >= self.hi {
                            break;
                        }

                        #[expect(
                            clippy::expect_used,
                            reason = "hi is at most equal to the last slot; so because 0 <= lo < hi, it must be a valid index"
                        )]
                        let table = self.run.get(self.lo).expect("should exist");

                        // SAFETY INVARIANT: range_overlap_indexes uses binary search on
                        // table min/max keys and is exact for disjoint sorted runs —
                        // every table in lo..hi genuinely overlaps the query range.
                        // If this invariant were ever violated (e.g. by a future refactor),
                        // the impact is benign: table.range() would return an empty iterator
                        // (no data corruption, just wasted I/O).
                        debug_assert!(
                            table.check_key_range_overlap(&(
                                self.range_start.as_ref().map(AsRef::as_ref),
                                self.range_end.as_ref().map(AsRef::as_ref),
                            )),
                            "range_overlap_indexes returned a non-overlapping table in forward lazy loop"
                        );

                        if let Some(ex) = &self.extractor {
                            let tmp_range = (self.range_start.clone(), self.range_end.clone());
                            if table.should_skip_range_by_prefix_filter(&tmp_range, ex.as_ref()) {
                                self.lo += 1;
                                continue;
                            }
                        }

                        let reader =
                            table.range((self.range_start.clone(), self.range_end.clone()));
                        self.lo_reader = Some(Box::new(reader));
                        break;
                    }
                }
            } else if let Some(hi_reader) = &mut self.hi_reader {
                // NOTE: We reached the hi marker, so consume from it instead
                //
                // If it returns nothing, it is empty, so we are done
                return hi_reader.next();
            } else {
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for RunReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(hi_reader) = &mut self.hi_reader {
                if let Some(item) = hi_reader.next_back() {
                    return Some(item);
                }

                // NOTE: Hi reader is empty, get prev one
                self.hi_reader = None;
                self.hi -= 1;

                if self.lo < self.hi {
                    // Lazily move to previous table that overlaps the key range
                    loop {
                        if self.hi <= self.lo {
                            break;
                        }

                        #[expect(
                            clippy::expect_used,
                            reason = "because 0 <= lo <= hi, and hi monotonically decreases, hi must be a valid index"
                        )]
                        let table = self.run.get(self.hi).expect("should exist");

                        // SAFETY INVARIANT: range_overlap_indexes uses binary search on
                        // table min/max keys and is exact for disjoint sorted runs —
                        // every table in lo..hi genuinely overlaps the query range.
                        // If this invariant were ever violated (e.g. by a future refactor),
                        // the impact is benign: table.range() would return an empty iterator
                        // (no data corruption, just wasted I/O).
                        debug_assert!(
                            table.check_key_range_overlap(&(
                                self.range_start.as_ref().map(AsRef::as_ref),
                                self.range_end.as_ref().map(AsRef::as_ref),
                            )),
                            "range_overlap_indexes returned a non-overlapping table in backward lazy loop"
                        );

                        if let Some(ex) = &self.extractor {
                            let tmp_range = (self.range_start.clone(), self.range_end.clone());
                            if table.should_skip_range_by_prefix_filter(&tmp_range, ex.as_ref()) {
                                self.hi -= 1;
                                continue;
                            }
                        }

                        let reader =
                            table.range((self.range_start.clone(), self.range_end.clone()));
                        self.hi_reader = Some(Box::new(reader));
                        break;
                    }
                }
            } else if let Some(lo_reader) = &mut self.lo_reader {
                // NOTE: We reached the lo marker, so consume from it instead
                //
                // If it returns nothing, it is empty, so we are done
                return lo_reader.next_back();
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{AbstractTree, SequenceNumberCounter, Slice};
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn run_reader_skip() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(
            &tempdir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Run::new(tables).unwrap());

        assert!(
            RunReader::new(level.clone(), UserKey::from("y")..=UserKey::from("z"), None).is_none()
        );

        assert!(RunReader::new(level, UserKey::from("y").., None).is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn run_reader_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(
            &tempdir,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        for batch in ids {
            for id in batch {
                tree.insert(id, vec![], 0);
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Run::new(tables).unwrap());

        {
            let multi_reader = RunReader::culled(level.clone(), .., (Some(1), None), None);
            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), .., None).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), .., None).unwrap();

            let mut iter = multi_reader.rev().flatten();

            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), .., None).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"a"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"b"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"c"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"d"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"e"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next_back().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"f"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next_back().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level.clone(), UserKey::from("g").., None).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        {
            let multi_reader = RunReader::new(level, UserKey::from("g").., None).unwrap();

            let mut iter = multi_reader.flatten().rev();

            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
        }

        Ok(())
    }

    mod prefix_extractor {
        use super::super::*;
        use crate::prefix::{FixedLengthExtractor, SharedPrefixExtractor};
        use crate::{range::prefix_upper_range, AbstractTree, SequenceNumberCounter};
        use std::ops::Bound;
        use test_log::test;

        #[test]
        fn run_reader_prefix_range_pruning_absent() -> crate::Result<()> {
            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    .open()?;

            // Create multiple tables with prefixes "aaa" and "bbb"
            for p in [b"aaa", b"bbb"] {
                for i in 0..10u32 {
                    let mut k = p.to_vec();
                    k.extend_from_slice(format!("{i:04}").as_bytes());
                    tree.insert(k, b"v", seqno.next());
                }
                tree.flush_active_memtable(0)?;
            }

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();
            let level = std::sync::Arc::new(Run::new(tables).unwrap());

            // Query a prefix range for a non-existent prefix "zzz"
            let prefix = b"zzz".to_vec();
            let start = Bound::Included(UserKey::from(prefix.clone()));
            let end = prefix_upper_range(&prefix);
            let ex = Some(ex);

            // All overlapped tables report Some(false) -> should prune (None)
            let reader = RunReader::new(level, (start, end), ex);
            assert!(reader.is_none());

            Ok(())
        }

        #[test]
        fn run_reader_prefix_range_no_pruning_when_possible_hit() -> crate::Result<()> {
            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    .open()?;

            // Tables with prefixes: "aaa" and "zzz"
            for p in [b"aaa", b"zzz"] {
                for i in 0..5u32 {
                    let mut k = p.to_vec();
                    k.extend_from_slice(format!("{i:02}").as_bytes());
                    tree.insert(k, b"v", seqno.next());
                }
                tree.flush_active_memtable(0)?;
            }

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();
            let level = std::sync::Arc::new(Run::new(tables).unwrap());

            // Query a prefix range for existing prefix "zzz"
            let prefix = b"zzz".to_vec();
            let start = Bound::Included(UserKey::from(prefix.clone()));
            let end = prefix_upper_range(&prefix);
            let ex = Some(ex);

            let reader = RunReader::new(level, (start, end), ex);
            assert!(reader.is_some());

            Ok(())
        }

        /// Helper: create a multi-table run where each table has a wide key range
        /// (from "aaa..." to "zzz...") but only contains specific prefixes in its filter.
        /// This ensures key range overlaps with queries for absent prefixes.
        fn create_wide_range_run_with_prefixes(
            prefixes_per_table: &[&[&[u8]]],
        ) -> crate::Result<(tempfile::TempDir, Arc<Run<Table>>, SharedPrefixExtractor)> {
            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    .open()?;

            for prefixes in prefixes_per_table {
                for p in *prefixes {
                    for i in 0..5u32 {
                        let mut k = p.to_vec();
                        k.extend_from_slice(format!("{i:02}").as_bytes());
                        tree.insert(k, b"v", seqno.next());
                    }
                }
                tree.flush_active_memtable(0)?;
            }

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();
            let level = Arc::new(Run::new(tables).unwrap());

            Ok((tempdir, level, ex))
        }

        /// Upfront pruning: all tables' key ranges overlap but ALL filters exclude the prefix.
        /// The run reader should return None (no results possible).
        #[test]
        fn run_reader_upfront_pruning_all_excluded() -> crate::Result<()> {
            use crate::config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry};

            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            // Extractor length 3: start "mmm..." and end "mmn..." both share prefix "mmm"/"mmn"...
            // Actually: for prefix_upper_range("mmm") = Excluded("mmn"). Extract("mmm") = "mmm",
            // Extract("mmn") = "mmn". Those differ! So we need the prefix range boundaries
            // to share the same extracted prefix. Use the full prefix as the key:
            // prefix = "mmm00" (5 bytes), extractor = 3, extract("mmm00") = "mmm",
            // prefix_upper_range("mmm00") = Excluded("mmm01"), extract("mmm01") = "mmm".
            // => common_prefix = "mmm".
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    // Use high bits-per-key to minimize false positive rate
                    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
                        BloomConstructionPolicy::BitsPerKey(50.0),
                    )))
                    .open()?;

            // Table 1: wide key range (aaa..zzz) but only specific prefixes
            for p in [b"aaa" as &[u8], b"bbb", b"ccc", b"xxx", b"yyy", b"zzz"] {
                for i in 0..30u32 {
                    let mut k = p.to_vec();
                    k.extend_from_slice(format!("{i:04}").as_bytes());
                    tree.insert(k, b"v", seqno.next());
                }
            }
            tree.flush_active_memtable(0)?;

            // Table 2: wide key range, different prefixes, still no "mmm"
            for p in [b"ddd" as &[u8], b"eee", b"fff", b"vvv", b"www", b"zzz"] {
                for i in 0..30u32 {
                    let mut k = p.to_vec();
                    k.extend_from_slice(format!("{i:04}").as_bytes());
                    tree.insert(k, b"v", seqno.next());
                }
            }
            tree.flush_active_memtable(0)?;

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();
            let level = Arc::new(Run::new(tables).unwrap());

            // Use "mmm00" as the prefix range key so both bounds share extracted prefix "mmm":
            // start = Included("mmm00"), extract = "mmm"
            // end = prefix_upper_range("mmm00") = Excluded("mmm01"), extract = "mmm"
            // => common_prefix = Some("mmm")
            let prefix = b"mmm00".to_vec();
            let start = Bound::Included(UserKey::from(prefix.clone()));
            let end = prefix_upper_range(&prefix);

            let reader = RunReader::new(level, (start, end), Some(ex));
            assert!(
                reader.is_none(),
                "should prune: no table contains prefix mmm"
            );

            Ok(())
        }

        /// Upfront pruning: one table's filter contains the prefix, so pruning does NOT return None.
        /// The run reader should return Some since a potential match exists.
        #[test]
        fn run_reader_upfront_pruning_one_hit() -> crate::Result<()> {
            // Table 1: prefixes "aaa" and "zzz"
            // Table 2: prefixes "bbb" and "mmm" — this one has "mmm"!
            let (_dir, level, ex) =
                create_wide_range_run_with_prefixes(&[&[b"aaa", b"zzz"], &[b"bbb", b"mmm"]])?;

            // Use "mmm00" so both bounds share the same 3-byte extracted prefix "mmm":
            // start=Included("mmm00") → prefix "mmm", end=Excluded("mmm01") → prefix "mmm"
            let prefix = b"mmm00".to_vec();
            let start = Bound::Included(UserKey::from(prefix.clone()));
            let end = prefix_upper_range(&prefix);

            let reader = RunReader::new(level, (start, end), Some(ex));
            assert!(
                reader.is_some(),
                "should NOT prune: table 2 contains prefix mmm"
            );

            Ok(())
        }

        /// Upfront pruning with >10 tables: exceeds `MAX_UPFRONT_CHECKS` limit.
        /// When too many tables need checking, pruning bails out and returns Some.
        #[test]
        fn run_reader_upfront_pruning_exceeds_max_checks() -> crate::Result<()> {
            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    .open()?;

            // Create 12 tables, each with wide key range (aaa..zzz) but unique middle prefixes
            for i in 0..12u32 {
                // Each table has "aaa" and "zzz" to ensure wide key range overlap
                for p in [b"aaa", b"zzz"] {
                    let mut k = p.to_vec();
                    k.extend_from_slice(format!("{i:02}").as_bytes());
                    tree.insert(k, b"v", seqno.next());
                }
                // Each table also has a unique prefix that is NOT "mmm"
                let unique = format!("p{i:02}");
                tree.insert(unique.as_bytes(), b"v", seqno.next());
                tree.flush_active_memtable(0)?;
            }

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();
            assert!(tables.len() >= 11, "need >10 tables for this test");
            let level = Arc::new(Run::new(tables).unwrap());

            // Query for "mmm00" — no table has it, but after 10 checks we bail out
            // and return Some (don't prune) because we exceeded MAX_UPFRONT_CHECKS.
            // Use "mmm00" so both bounds share the same 3-byte extracted prefix "mmm".
            let prefix = b"mmm00".to_vec();
            let start = Bound::Included(UserKey::from(prefix.clone()));
            let end = prefix_upper_range(&prefix);

            let reader = RunReader::new(level, (start, end), Some(ex));
            assert!(
                reader.is_some(),
                "should NOT prune: exceeded max upfront checks"
            );

            Ok(())
        }

        /// Unbounded start range with extractor: `common_prefix` should be None.
        /// No upfront pruning occurs when the start bound is unbounded.
        #[test]
        fn run_reader_unbounded_start_with_extractor() -> crate::Result<()> {
            let (_dir, level, ex) =
                create_wide_range_run_with_prefixes(&[&[b"aaa", b"zzz"], &[b"bbb", b"yyy"]])?;

            // Unbounded start: common_prefix = None, no upfront pruning
            let reader = RunReader::new(level, ..UserKey::from("mmm99"), Some(ex));
            assert!(reader.is_some());

            Ok(())
        }

        /// Unbounded end range with extractor: `common_prefix` should be None.
        /// No upfront pruning occurs when the end bound is unbounded.
        #[test]
        fn run_reader_unbounded_end_with_extractor() -> crate::Result<()> {
            let (_dir, level, ex) =
                create_wide_range_run_with_prefixes(&[&[b"aaa", b"zzz"], &[b"bbb", b"yyy"]])?;

            // Unbounded end: common_prefix = None, no upfront pruning
            let reader = RunReader::new(level, UserKey::from("mmm00").., Some(ex));
            assert!(reader.is_some());

            Ok(())
        }

        /// Cross-prefix range: start and end have different prefixes.
        /// No upfront pruning occurs when the range spans multiple prefixes.
        #[test]
        fn run_reader_cross_prefix_range() -> crate::Result<()> {
            let (_dir, level, ex) =
                create_wide_range_run_with_prefixes(&[&[b"aaa", b"zzz"], &[b"bbb", b"yyy"]])?;

            // Start prefix "aaa", end prefix "bbb" — different, so common_prefix = None
            let reader = RunReader::new(
                level,
                UserKey::from("aaa00")..UserKey::from("bbb99"),
                Some(ex),
            );
            assert!(reader.is_some());

            Ok(())
        }

        /// Helper: creates overlapping L0 tables with wide key ranges for lazy skip testing.
        ///
        /// Each table spans "aaa" to "zzz" (via anchor keys) but only some tables
        /// contain the target prefix "mmm". This forces ALL tables to overlap any
        /// prefix query, so the lazy skip loop in RunReader must check the prefix
        /// filter on middle tables rather than having them excluded by
        /// `range_overlap_indexes`.
        ///
        /// Layout (4 tables, each with "aaa" and "zzz" anchors):
        ///   Table 0: "aaa", "mmm", "zzz"  — has target prefix
        ///   Table 1: "aaa", "zzz"          — NO target prefix → lazy skip fires
        ///   Table 2: "aaa", "mmm", "zzz"  — has target prefix
        ///   Table 3: "aaa", "zzz"          — NO target prefix
        fn create_overlapping_run_for_lazy_skip(
        ) -> crate::Result<(tempfile::TempDir, Arc<Run<Table>>, SharedPrefixExtractor)> {
            use crate::config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry};

            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    // High bits-per-key to eliminate false positives
                    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
                        BloomConstructionPolicy::BitsPerKey(50.0),
                    )))
                    .open()?;

            // Table layout: each flush creates one L0 table.
            // All tables have "aaa" and "zzz" keys as anchors for wide key range.
            let table_prefixes: &[&[&[u8]]] = &[
                &[b"aaa", b"mmm", b"zzz"], // Table 0: has "mmm"
                &[b"aaa", b"zzz"],         // Table 1: NO "mmm"
                &[b"aaa", b"mmm", b"zzz"], // Table 2: has "mmm"
                &[b"aaa", b"zzz"],         // Table 3: NO "mmm"
            ];

            for prefixes in table_prefixes {
                for p in *prefixes {
                    for i in 0..5u32 {
                        let mut k = p.to_vec();
                        k.extend_from_slice(format!("{i:02}").as_bytes());
                        tree.insert(k, b"v", seqno.next());
                    }
                }
                tree.flush_active_memtable(0)?;
            }

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(
                tables.len(),
                4,
                "expected exactly 4 L0 tables, got {}",
                tables.len()
            );

            let level = Arc::new(Run::new(tables).unwrap());
            Ok((tempdir, level, ex))
        }

        /// Lazy per-table prefix skip during forward iteration.
        ///
        /// With 4 overlapping tables (all key ranges span "aaa".."zzz"), querying
        /// prefix "mmm" causes `range_overlap_indexes` to set lo=0, hi=3. The lo
        /// and hi readers are created for tables 0 and 3. As forward iteration
        /// exhausts the lo reader and advances, the lazy skip loop processes middle
        /// tables 1 and 2:
        ///   - Table 1 has no "mmm" in its filter → skip branch fires (lo += 1)
        ///   - Table 2 has "mmm" → reader is created
        #[test]
        fn run_reader_lazy_forward_prefix_skip() -> crate::Result<()> {
            let (_dir, level, ex) = create_overlapping_run_for_lazy_skip()?;

            // Range ["mmm00", "mmm99"]: both bounds extract to "mmm" (FixedLengthExtractor(3)),
            // so common_prefix = Some("mmm") and should_skip_range_by_prefix_filter works.
            let start = Bound::Included(UserKey::from("mmm00"));
            let end = Bound::Included(UserKey::from("mmm99"));

            let reader = RunReader::new(level, (start, end), Some(ex));
            assert!(reader.is_some());

            let results: Vec<_> = reader.unwrap().flatten().collect();
            // Tables 0 and 2 each have 5 "mmm" keys ("mmm00".."mmm04").
            // Tables 1 and 3 have no "mmm" keys.
            // Table 0 is read by lo_reader, table 3 by hi_reader (yields nothing).
            // The lazy loop skips table 1 (no "mmm") and reads table 2 (has "mmm").
            // Total: 5 (table 0) + 5 (table 2) = 10 keys.
            assert_eq!(results.len(), 10, "expected 10 mmm keys from 2 tables");
            for item in &results {
                assert!(
                    item.key.user_key.starts_with(b"mmm"),
                    "unexpected key: {:?}",
                    item.key.user_key
                );
            }

            Ok(())
        }

        /// Lazy per-table prefix skip during reverse iteration.
        ///
        /// Same 4-table layout. Reverse iteration starts from hi_reader (table 3),
        /// then the lazy skip loop processes middle tables in reverse (2, then 1):
        ///   - Table 2 has "mmm" → reader created
        ///   - Table 1 has no "mmm" → skip branch fires (hi -= 1)
        #[test]
        fn run_reader_lazy_reverse_prefix_skip() -> crate::Result<()> {
            let (_dir, level, ex) = create_overlapping_run_for_lazy_skip()?;

            // Range ["mmm00", "mmm99"]: both bounds extract to "mmm"
            let start = Bound::Included(UserKey::from("mmm00"));
            let end = Bound::Included(UserKey::from("mmm99"));

            let reader = RunReader::new(level, (start, end), Some(ex));
            assert!(reader.is_some());

            let results: Vec<_> = reader.unwrap().rev().flatten().collect();
            // Same 10 mmm keys, but in reverse order
            assert_eq!(results.len(), 10, "expected 10 mmm keys from 2 tables");
            for item in &results {
                assert!(
                    item.key.user_key.starts_with(b"mmm"),
                    "unexpected key: {:?}",
                    item.key.user_key
                );
            }

            Ok(())
        }

        /// Helper: 4 tables where only the first and last contain the target prefix.
        ///
        /// Layout (all tables share "aaa" and "zzz" anchors for wide key range):
        ///   Table 0: "aaa", "mmm", "zzz"  — has target prefix (lo)
        ///   Table 1: "aaa", "zzz"          — NO target prefix
        ///   Table 2: "aaa", "zzz"          — NO target prefix
        ///   Table 3: "aaa", "mmm", "zzz"  — has target prefix (hi)
        ///
        /// During backward iteration, after the hi reader (T3) is exhausted,
        /// the inner loop must skip T2 and T1 (both lack "mmm"), decrementing
        /// hi all the way down to lo — exercising the `hi <= lo` break.
        fn create_run_for_backward_hi_meets_lo(
        ) -> crate::Result<(tempfile::TempDir, Arc<Run<Table>>, SharedPrefixExtractor)> {
            use crate::config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry};

            let tempdir = tempfile::tempdir()?;
            let seqno = SequenceNumberCounter::default();
            let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
            let tree =
                crate::Config::new(&tempdir, seqno.clone(), SequenceNumberCounter::default())
                    .prefix_extractor(ex.clone())
                    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
                        BloomConstructionPolicy::BitsPerKey(50.0),
                    )))
                    .open()?;

            let table_prefixes: &[&[&[u8]]] = &[
                &[b"aaa", b"mmm", b"zzz"], // Table 0: has "mmm"
                &[b"aaa", b"zzz"],         // Table 1: NO "mmm"
                &[b"aaa", b"zzz"],         // Table 2: NO "mmm"
                &[b"aaa", b"mmm", b"zzz"], // Table 3: has "mmm"
            ];

            for prefixes in table_prefixes {
                for p in *prefixes {
                    for i in 0..5u32 {
                        let mut k = p.to_vec();
                        k.extend_from_slice(format!("{i:02}").as_bytes());
                        tree.insert(k, b"v", seqno.next());
                    }
                }
                tree.flush_active_memtable(0)?;
            }

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(
                tables.len(),
                4,
                "expected exactly 4 L0 tables, got {}",
                tables.len()
            );

            let level = Arc::new(Run::new(tables).unwrap());
            Ok((tempdir, level, ex))
        }

        /// Backward lazy loop: hi decrements past all middle tables to meet lo.
        ///
        /// With T0 and T3 having "mmm" and T1/T2 lacking it, reverse iteration
        /// exhausts hi_reader (T3), then the inner loop skips T2 and T1 via the
        /// prefix filter, decrementing hi to 0 where `hi <= lo` triggers the break.
        /// Iteration then falls through to lo_reader (T0).
        #[test]
        fn run_reader_backward_lazy_hi_meets_lo() -> crate::Result<()> {
            let (_dir, level, ex) = create_run_for_backward_hi_meets_lo()?;

            let start = Bound::Included(UserKey::from("mmm00"));
            let end = Bound::Included(UserKey::from("mmm99"));

            let reader = RunReader::new(level, (start, end), Some(ex));
            assert!(reader.is_some());

            let results: Vec<_> = reader.unwrap().rev().flatten().collect();
            // T3 has 5 "mmm" keys, T0 has 5 "mmm" keys → 10 total
            assert_eq!(
                results.len(),
                10,
                "expected 10 mmm keys from tables 0 and 3"
            );
            for item in &results {
                assert!(
                    item.key.user_key.starts_with(b"mmm"),
                    "unexpected key: {:?}",
                    item.key.user_key
                );
            }

            Ok(())
        }

        /// Excluded start bound: verifies that a range with an excluded start bound
        /// is handled correctly and still returns results.
        #[test]
        fn run_reader_excluded_start_bound() -> crate::Result<()> {
            let (_dir, level, ex) =
                create_wide_range_run_with_prefixes(&[&[b"aaa", b"zzz"], &[b"bbb", b"yyy"]])?;

            // Use Excluded start bound — not a common API pattern, but exercises the branch
            let reader = RunReader::new(
                level,
                (
                    Bound::Excluded(UserKey::from("aaa00")),
                    Bound::Included(UserKey::from("zzz99")),
                ),
                Some(ex),
            );
            assert!(reader.is_some());

            Ok(())
        }

        /// Terminal None in backward iteration: after forward iteration fully
        /// consumes `lo_reader`, calling `next_back()` with no `hi_reader` and
        /// no `lo_reader` returns None immediately.
        #[test]
        fn run_reader_backward_terminal_none_after_forward_exhaustion() -> crate::Result<()> {
            let tempdir = tempfile::tempdir()?;
            let tree = crate::Config::new(
                &tempdir,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()?;

            // Two tables with disjoint key ranges
            tree.insert("a", vec![], 0);
            tree.flush_active_memtable(0)?;
            tree.insert("z", vec![], 0);
            tree.flush_active_memtable(0)?;

            let tables = tree
                .current_version()
                .iter_tables()
                .cloned()
                .collect::<Vec<_>>();
            let level = Arc::new(Run::new(tables).unwrap());

            // Create reader over a narrow range that only overlaps one table
            // hi == lo → no hi_reader created
            let mut reader =
                RunReader::new(level, UserKey::from("a")..=UserKey::from("a"), None).unwrap();

            // Forward-exhaust the lo_reader
            assert!(reader.next().is_some()); // "a"
            assert!(reader.next().is_none()); // lo_reader exhausted, lo_reader = None

            // Now call next_back — no hi_reader, no lo_reader → return None (line 291)
            assert!(reader.next_back().is_none());

            Ok(())
        }
    }
}
