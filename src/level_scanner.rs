// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{level_manifest::level::Level, segment::scanner::Scanner, InternalValue};
use std::{path::PathBuf, sync::Arc};

/// Scans through a disjoint level
///
/// Optimized for compaction, by using a `SegmentScanner` instead of `SegmentReader`.
pub struct LevelScanner {
    base_folder: PathBuf,
    segments: Arc<Level>,
    lo: usize,
    hi: usize,
    lo_reader: Option<Scanner>,
    hi_reader: Option<Scanner>,
}

impl LevelScanner {
    pub fn from_indexes(
        base_folder: PathBuf,
        level: Arc<Level>,
        (lo, hi): (Option<usize>, Option<usize>),
    ) -> crate::Result<Self> {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(level.len() - 1);

        let lo_segment = level.segments.get(lo).expect("should exist");

        let lo_reader = lo_segment.scan(&base_folder)?;

        let hi_reader = if hi > lo {
            let hi_segment = level.segments.get(hi).expect("should exist");

            Some(hi_segment.scan(&base_folder)?)
        } else {
            None
        };

        Ok(Self {
            base_folder,
            segments: level,
            lo,
            hi,
            lo_reader: Some(lo_reader),
            hi_reader,
        })
    }
}

impl Iterator for LevelScanner {
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
                    let scanner = fail_iter!(self
                        .segments
                        .get(self.lo)
                        .expect("should exist")
                        .scan(&self.base_folder));

                    self.lo_reader = Some(scanner);
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{AbstractTree, Slice};
    use test_log::test;

    #[test]
    fn level_scanner_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let tree = crate::Config::new(&tempdir).open()?;

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

        let segments = tree
            .levels
            .read()
            .expect("lock is poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Level {
            segments,
            is_disjoint: true,
        });

        #[allow(clippy::unwrap_used)]
        {
            let multi_reader = LevelScanner::from_indexes(
                tempdir.path().join("segments"),
                level.clone(),
                (None, None),
            )?;

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
        #[allow(clippy::unwrap_used)]
        {
            let multi_reader = LevelScanner::from_indexes(
                tempdir.path().join("segments"),
                level.clone(),
                (Some(1), None),
            )?;

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
        }

        Ok(())
    }
}
