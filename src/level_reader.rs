// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    level_manifest::level::Level,
    segment::{range::Range, value_block::CachePolicy},
    InternalValue, UserKey,
};
use std::{ops::Bound, sync::Arc};

/// Reads through a disjoint level
pub struct LevelReader {
    segments: Arc<Level>,
    lo: usize,
    hi: usize,
    lo_reader: Option<Range>,
    hi_reader: Option<Range>,
    cache_policy: CachePolicy,
}

impl LevelReader {
    #[must_use]
    pub fn new(
        level: Arc<Level>,
        range: &(Bound<UserKey>, Bound<UserKey>),
        cache_policy: CachePolicy,
    ) -> Option<Self> {
        assert!(!level.is_empty(), "level reader cannot read empty level");

        let disjoint_level = level.as_disjoint().expect("level should be disjoint");

        let (lo, hi) = disjoint_level.range_indexes(range)?;

        Some(Self::from_indexes(
            level,
            range,
            (Some(lo), Some(hi)),
            cache_policy,
        ))
    }

    #[must_use]
    pub fn from_indexes(
        level: Arc<Level>,
        range: &(Bound<UserKey>, Bound<UserKey>),
        (lo, hi): (Option<usize>, Option<usize>),
        cache_policy: CachePolicy,
    ) -> Self {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(level.len() - 1);

        // TODO: lazily init readers?
        let lo_segment = level.segments.get(lo).expect("should exist");
        let lo_reader = lo_segment.range(range.clone()).cache_policy(cache_policy);

        // TODO: lazily init readers?
        let hi_reader = if hi > lo {
            let hi_segment = level.segments.get(hi).expect("should exist");
            Some(hi_segment.range(range.clone()).cache_policy(cache_policy))
        } else {
            None
        };

        Self {
            segments: level,
            lo,
            hi,
            lo_reader: Some(lo_reader),
            hi_reader,
            cache_policy,
        }
    }
}

impl Iterator for LevelReader {
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
                    self.lo_reader = Some(
                        self.segments
                            .get(self.lo)
                            .expect("should exist")
                            .iter()
                            .cache_policy(self.cache_policy),
                    );
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

impl DoubleEndedIterator for LevelReader {
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
                    self.hi_reader = Some(
                        self.segments
                            .get(self.hi)
                            .expect("should exist")
                            .iter()
                            .cache_policy(self.cache_policy),
                    );
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
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{AbstractTree, Slice};
    use std::ops::Bound::{Included, Unbounded};
    use test_log::test;

    #[test]
    fn level_reader_skip() -> crate::Result<()> {
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

        assert!(LevelReader::new(
            level.clone(),
            &(Included(b"y".into()), Included(b"z".into())),
            CachePolicy::Read
        )
        .is_none());

        assert!(LevelReader::new(
            level,
            &(Included(b"y".into()), Unbounded),
            CachePolicy::Read
        )
        .is_none());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn level_reader_basic() -> crate::Result<()> {
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

        {
            let multi_reader =
                LevelReader::new(level.clone(), &(Unbounded, Unbounded), CachePolicy::Read)
                    .unwrap();

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
            let multi_reader =
                LevelReader::new(level.clone(), &(Unbounded, Unbounded), CachePolicy::Read)
                    .unwrap();

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
            let multi_reader =
                LevelReader::new(level.clone(), &(Unbounded, Unbounded), CachePolicy::Read)
                    .unwrap();

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
            let multi_reader = LevelReader::new(
                level.clone(),
                &(Included(b"g".into()), Unbounded),
                CachePolicy::Read,
            )
            .unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        }

        {
            let multi_reader = LevelReader::new(
                level,
                &(Included(b"g".into()), Unbounded),
                CachePolicy::Read,
            )
            .unwrap();

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
}
