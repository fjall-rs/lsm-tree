// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{version::Run, BoxedIterator, InternalValue, Table, UserKey};
use std::{
    ops::{Bound, Deref, RangeBounds},
    sync::Arc,
};

type OwnedRange = (Bound<UserKey>, Bound<UserKey>);

fn to_owned_range<R: RangeBounds<UserKey>>(range: &R) -> OwnedRange {
    (
        match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        },
        match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        },
    )
}

/// Reads through a disjoint run with lazy reader initialization.
///
/// `lo_reader` and `hi_reader` are constructed on first `next()` /
/// `next_back()` respectively, deferring the `table.range()` seek.
pub struct RunReader {
    run: Arc<Run<Table>>,
    range: OwnedRange,
    lo: usize,
    hi: usize,
    lo_reader: Option<BoxedIterator<'static>>,
    hi_reader: Option<BoxedIterator<'static>>,
    lo_initialized: bool,
    hi_initialized: bool,
}

impl RunReader {
    #[must_use]
    pub fn new<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
    ) -> Option<Self> {
        assert!(!run.is_empty(), "level reader cannot read empty level");

        let (lo, hi) = run.range_overlap_indexes(&range)?;

        Some(Self::culled(run, range, (Some(lo), Some(hi))))
    }

    #[must_use]
    pub fn culled<R: RangeBounds<UserKey> + Clone + Send + 'static>(
        run: Arc<Run<Table>>,
        range: R,
        (lo, hi): (Option<usize>, Option<usize>),
    ) -> Self {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);
        let owned_range = to_owned_range(&range);

        Self {
            run,
            range: owned_range,
            lo,
            hi,
            lo_reader: None,
            hi_reader: None,
            lo_initialized: false,
            hi_initialized: lo >= hi,
        }
    }

    fn ensure_lo_initialized(&mut self) {
        if !self.lo_initialized {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let lo_table = self.run.deref().get(self.lo).expect("should exist");
            self.lo_reader = Some(Box::new(lo_table.range(self.range.clone())));
            self.lo_initialized = true;
        }
    }

    fn ensure_hi_initialized(&mut self) {
        if !self.hi_initialized {
            #[expect(
                clippy::expect_used,
                reason = "we trust the caller to pass valid indexes"
            )]
            let hi_table = self.run.deref().get(self.hi).expect("should exist");
            self.hi_reader = Some(Box::new(hi_table.range(self.range.clone())));
            self.hi_initialized = true;
        }
    }
}

impl Iterator for RunReader {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ensure_lo_initialized();

        loop {
            if let Some(lo_reader) = &mut self.lo_reader {
                if let Some(item) = lo_reader.next() {
                    return Some(item);
                }

                // NOTE: Lo reader is empty, get next one
                self.lo_reader = None;
                self.lo += 1;

                // Strict `<`: when lo reaches hi, this branch is skipped and
                // the hi table is read via ensure_hi_initialized (which uses
                // table.range() to respect the range end bound). `.iter()` is
                // only used for middle tables that are fully consumed.
                if self.lo < self.hi {
                    self.lo_reader = Some(Box::new(
                        #[expect(
                            clippy::expect_used,
                            reason = "hi is at most equal to the last slot; so because 0 <= lo < hi, it must be a valid index"
                        )]
                        self.run.get(self.lo).expect("should exist").iter(),
                    ));
                }
            } else {
                // Lo exhausted — initialize hi reader if needed and consume from it
                self.ensure_hi_initialized();

                if let Some(hi_reader) = &mut self.hi_reader {
                    return hi_reader.next();
                }
                return None;
            }
        }
    }
}

impl DoubleEndedIterator for RunReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.ensure_hi_initialized();

        loop {
            if let Some(hi_reader) = &mut self.hi_reader {
                if let Some(item) = hi_reader.next_back() {
                    return Some(item);
                }

                // NOTE: Hi reader is empty, get prev one
                self.hi_reader = None;
                self.hi -= 1;

                if self.lo < self.hi {
                    self.hi_reader = Some(Box::new(
                        #[expect(
                            clippy::expect_used,
                            reason = "because 0 <= lo <= hi, and hi monotonically decreases, hi must be a valid index"
                        )]
                        self.run.get(self.hi).expect("should exist").iter(),
                    ));
                }
            } else {
                // Hi exhausted — initialize lo reader if needed and consume from it
                self.ensure_lo_initialized();

                if let Some(lo_reader) = &mut self.lo_reader {
                    return lo_reader.next_back();
                }
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

        assert!(RunReader::new(level.clone(), UserKey::from("y")..=UserKey::from("z"),).is_none());

        assert!(RunReader::new(level, UserKey::from("y")..).is_none());

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
            let multi_reader = RunReader::culled(level.clone(), .., (Some(1), None));
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
            let multi_reader = RunReader::new(level.clone(), ..).unwrap();

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
            let multi_reader = RunReader::new(level.clone(), ..).unwrap();

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
            let multi_reader = RunReader::new(level.clone(), ..).unwrap();

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
            let multi_reader = RunReader::new(level.clone(), UserKey::from("g")..).unwrap();

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
            let multi_reader = RunReader::new(level, UserKey::from("g")..).unwrap();

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
}
