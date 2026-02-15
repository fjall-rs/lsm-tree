// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{fs::FileSystem, table::Scanner, version::Run, InternalValue, Table};
use std::sync::Arc;

/// Scans through a disjoint run
///
/// Optimized for compaction, by using a `TableScanner` instead of `TableReader`.
pub struct RunScanner<F: FileSystem> {
    tables: Arc<Run<Table<F>>>,
    lo: usize,
    hi: usize,
    lo_reader: Option<Scanner<F>>,
}

impl<F: FileSystem> RunScanner<F> {
    pub fn culled(
        run: Arc<Run<Table<F>>>,
        (lo, hi): (Option<usize>, Option<usize>),
    ) -> crate::Result<Self> {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);

        #[expect(
            clippy::expect_used,
            reason = "we trust the caller to pass valid indexes"
        )]
        let lo_table = run.get(lo).expect("should exist");

        let lo_reader = lo_table.scan()?;

        Ok(Self {
            tables: run,
            lo,
            hi,
            lo_reader: Some(lo_reader),
        })
    }
}

impl<F: FileSystem> Iterator for RunScanner<F> {
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

                if self.lo <= self.hi {
                    #[expect(
                        clippy::expect_used,
                        reason = "hi is at most equal to the last slot; so because 0 <= lo <= hi, it must be a valid index"
                    )]
                    let scanner =
                        fail_iter!(self.tables.get(self.lo).expect("should exist").scan());

                    self.lo_reader = Some(scanner);
                }
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
    use test_log::test;

    #[test]
    fn run_scanner_basic() -> crate::Result<()> {
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

        #[expect(clippy::unwrap_used)]
        {
            let multi_reader = RunScanner::culled(level.clone(), (None, None))?;

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

        #[expect(clippy::unwrap_used)]
        {
            let multi_reader = RunScanner::culled(level, (Some(1), None))?;

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

        Ok(())
    }
}
