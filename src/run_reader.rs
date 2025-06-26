// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{segment::CachePolicy, version::Run, BoxedIterator, InternalValue, Segment, UserKey};
use std::{
    ops::{Deref, RangeBounds},
    sync::Arc,
};

/// Reads through a disjoint run
pub struct RunReader {
    run: Arc<Run<Segment>>,
    lo: usize,
    hi: usize,
    lo_reader: Option<Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>>>>,
    hi_reader: Option<Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>>>>,
    cache_policy: CachePolicy,
}

impl RunReader {
    #[must_use]
    pub fn new<R: RangeBounds<UserKey> + Clone + 'static>(
        run: Arc<Run<Segment>>,
        range: R,
        cache_policy: CachePolicy,
    ) -> Option<Self> {
        assert!(!run.is_empty(), "level reader cannot read empty level");

        let (lo, hi) = run.range_indexes(&range)?;

        Some(Self::culled(run, range, (Some(lo), Some(hi)), cache_policy))
    }

    #[must_use]
    pub fn culled<R: RangeBounds<UserKey> + Clone + 'static>(
        run: Arc<Run<Segment>>,
        range: R,
        (lo, hi): (Option<usize>, Option<usize>),
        cache_policy: CachePolicy,
    ) -> Self {
        let lo = lo.unwrap_or_default();
        let hi = hi.unwrap_or(run.len() - 1);

        // TODO: lazily init readers?
        let lo_segment = run.deref().get(lo).expect("should exist");
        let lo_reader = lo_segment.range(range.clone())/* .cache_policy(cache_policy) */;

        // TODO: lazily init readers?
        let hi_reader = if hi > lo {
            let hi_segment = run.deref().get(hi).expect("should exist");
            Some(
                hi_segment.range(range), /* .cache_policy(cache_policy) */
            )
        } else {
            None
        };

        Self {
            run,
            lo,
            hi,
            lo_reader: Some(Box::new(lo_reader)),
            hi_reader: hi_reader.map(|x| Box::new(x) as BoxedIterator),
            cache_policy,
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
                    self.lo_reader = Some(Box::new(
                        self.run.get(self.lo).expect("should exist").iter(),
                    ) /* .cache_policy(self.cache_policy) */);
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
                    self.hi_reader = Some(Box::new(
                        self.run.get(self.hi).expect("should exist").iter(),
                    ) /* .cache_policy(self.cache_policy) */);
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
    use test_log::test;

    #[test]
    fn v3_run_reader_skip() -> crate::Result<()> {
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
            .manifest
            .read()
            .expect("lock is poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Run::new(segments));

        assert!(RunReader::new(
            level.clone(),
            UserKey::from("y")..=UserKey::from("z"),
            CachePolicy::Read
        )
        .is_none());

        assert!(RunReader::new(level, UserKey::from("y").., CachePolicy::Read).is_none());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_run_reader_basic() -> crate::Result<()> {
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
            .manifest
            .read()
            .expect("lock is poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        let level = Arc::new(Run::new(segments));

        {
            let multi_reader = RunReader::new(level.clone(), .., CachePolicy::Read).unwrap();

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
            let multi_reader = RunReader::new(level.clone(), .., CachePolicy::Read).unwrap();

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
            let multi_reader = RunReader::new(level.clone(), .., CachePolicy::Read).unwrap();

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
            let multi_reader =
                RunReader::new(level.clone(), UserKey::from("g").., CachePolicy::Read).unwrap();

            let mut iter = multi_reader.flatten();

            assert_eq!(Slice::from(*b"g"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"h"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"i"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"j"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"k"), iter.next().unwrap().key.user_key);
            assert_eq!(Slice::from(*b"l"), iter.next().unwrap().key.user_key);
        }

        {
            let multi_reader =
                RunReader::new(level, UserKey::from("g").., CachePolicy::Read).unwrap();

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
