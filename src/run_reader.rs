// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    prefix::SharedPrefixExtractor, version::Run, BoxedIterator, InternalValue, Table, UserKey,
};
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
    range_start: std::ops::Bound<UserKey>,
    range_end: std::ops::Bound<UserKey>,
    // Optional extractor for prefix-aware pruning during lazy advancement
    extractor: Option<SharedPrefixExtractor>,
}

impl RunReader {
    /// Creates a run reader over a disjoint set of tables. Returns None when up-front
    /// prefix filter pruning determines that no table in the run may contain keys for the range.
    /// Uses common-prefix pruning only; per-table skipping happens lazily during iteration.
    #[must_use]
    pub fn new<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
        extractor: Option<SharedPrefixExtractor>,
    ) -> Option<Self> {
        assert!(!run.is_empty(), "level reader cannot read empty level");

        let (lo, hi) = run.range_overlap_indexes(&range)?;

        // Compute pruning prefix: only when both bounds' first extracted prefixes exist and are equal.
        let mut common_prefix: Option<Vec<u8>> = None;
        if let Some(ex) = extractor.as_ref() {
            use std::ops::Bound;
            let start_first = match range.start_bound() {
                Bound::Included(uk) | Bound::Excluded(uk) => {
                    ex.extract(uk.as_ref()).next().map(|p| p.to_vec())
                }
                Bound::Unbounded => None,
            };
            let end_first = match range.end_bound() {
                Bound::Included(uk) | Bound::Excluded(uk) => {
                    ex.extract(uk.as_ref()).next().map(|p| p.to_vec())
                }
                Bound::Unbounded => None,
            };
            common_prefix = match (start_first, end_first) {
                (Some(s), Some(e)) if s == e => Some(s),
                _ => None,
            };
        }

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
                    let bounds = (
                        range.start_bound().map(|k| k.as_ref()),
                        range.end_bound().map(|k| k.as_ref()),
                    );
                    if !table.check_key_range_overlap(&bounds) {
                        continue;
                    }

                    // Use a bound key as the probe so the extractor can derive the prefix and
                    // the filter index selection remains consistent.
                    let probe = start_key.or_else(|| match range.end_bound() {
                        std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => {
                            Some(k.as_ref())
                        }
                        std::ops::Bound::Unbounded => None,
                    });
                    if let Some(probe) = probe {
                        match table.maybe_contains_prefix(probe, ex.as_ref()) {
                            Ok(Some(false)) => { /* keep checking other tables */ }
                            _ => {
                                has_potential_match = true;
                                break;
                            }
                        }
                    } else {
                        // Without a concrete probe key, we cannot consult; treat as potential match
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
    pub fn culled<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
        (lo, hi): (Option<usize>, Option<usize>),
        extractor: Option<SharedPrefixExtractor>,
    ) -> Self {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);

        // Materialize owned range bounds for reuse when creating readers for other tables
        use std::ops::Bound::{Excluded, Included, Unbounded};
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

        // Init readers for boundary tables with proper range
        let lo_table = run.deref().get(lo).expect("should exist");
        let lo_reader = lo_table.range((owned_start.clone(), owned_end.clone()));

        let hi_reader = if hi > lo {
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
                        let table = self.run.get(self.lo).expect("should exist");
                        let bounds = (
                            self.range_start.as_ref().map(|k| k.as_ref()),
                            self.range_end.as_ref().map(|k| k.as_ref()),
                        );
                        if table.check_key_range_overlap(&bounds) {
                            if let Some(ex) = &self.extractor {
                                let tmp_range = (self.range_start.clone(), self.range_end.clone());
                                if table.should_skip_range_by_prefix_filter(&tmp_range, ex.as_ref())
                                {
                                    self.lo += 1;
                                    continue;
                                }
                            }
                            let reader =
                                table.range((self.range_start.clone(), self.range_end.clone()));
                            self.lo_reader = Some(Box::new(reader));
                            break;
                        }
                        self.lo += 1;
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
                        let table = self.run.get(self.hi).expect("should exist");
                        let bounds = (
                            self.range_start.as_ref().map(|k| k.as_ref()),
                            self.range_end.as_ref().map(|k| k.as_ref()),
                        );
                        if table.check_key_range_overlap(&bounds) {
                            if let Some(ex) = &self.extractor {
                                let tmp_range = (self.range_start.clone(), self.range_end.clone());
                                if table.should_skip_range_by_prefix_filter(&tmp_range, ex.as_ref())
                                {
                                    self.hi -= 1;
                                    continue;
                                }
                            }
                            let reader =
                                table.range((self.range_start.clone(), self.range_end.clone()));
                            self.hi_reader = Some(Box::new(reader));
                            break;
                        }
                        self.hi -= 1;
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
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::prefix::{FixedLengthExtractor, SharedPrefixExtractor};
    use crate::{range::prefix_upper_range, AbstractTree, SequenceNumberCounter, Slice};
    use std::ops::Bound;
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn run_reader_skip() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(&tempdir, SequenceNumberCounter::default()).open()?;

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

        let level = Arc::new(Run::new(tables));

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
        let tree = crate::Config::new(&tempdir, SequenceNumberCounter::default()).open()?;

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

        let level = Arc::new(Run::new(tables));

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
        }

        Ok(())
    }

    #[test]
    fn run_reader_prefix_range_pruning_absent() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let seqno = SequenceNumberCounter::default();
        let ex: SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));
        let tree = crate::Config::new(&tempdir, seqno.clone())
            .prefix_extractor(ex.clone())
            .open()?;

        // Create multiple tables with prefixes "aaa" and "bbb"
        for p in [b"aaa", b"bbb"] {
            for i in 0..10u32 {
                let mut k = p.to_vec();
                k.extend_from_slice(format!("{:04}", i).as_bytes());
                tree.insert(k, b"v", seqno.next());
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();
        let level = std::sync::Arc::new(Run::new(tables));

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
        let tree = crate::Config::new(&tempdir, seqno.clone())
            .prefix_extractor(ex.clone())
            .open()?;

        // Tables with prefixes: "aaa" and "zzz"
        for p in [b"aaa", b"zzz"] {
            for i in 0..5u32 {
                let mut k = p.to_vec();
                k.extend_from_slice(format!("{:02}", i).as_bytes());
                tree.insert(k, b"v", seqno.next());
            }
            tree.flush_active_memtable(0)?;
        }

        let tables = tree
            .current_version()
            .iter_tables()
            .cloned()
            .collect::<Vec<_>>();
        let level = std::sync::Arc::new(Run::new(tables));

        // Query a prefix range for existing prefix "zzz"
        let prefix = b"zzz".to_vec();
        let start = Bound::Included(UserKey::from(prefix.clone()));
        let end = prefix_upper_range(&prefix);
        let ex = Some(ex);

        let reader = RunReader::new(level, (start, end), ex);
        assert!(reader.is_some());

        Ok(())
    }
}
