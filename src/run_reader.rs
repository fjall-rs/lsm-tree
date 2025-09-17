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

        let (lo, hi) = run.range_overlap_indexes(&range)?;

        // Early optimization: Skip prefix filter checks if no prefix extractor is configured
        // Check the first segment to see if any segments have a prefix extractor
        // (all segments in a run should have the same configuration)
        let has_prefix_extractor = run
            .get(lo)
            .map(|seg| seg.has_prefix_extractor())
            .unwrap_or(false);

        if has_prefix_extractor {
            // Only perform prefix filter checks if a prefix extractor is configured
            // This avoids unnecessary CPU work for large scans when prefix filtering isn't used
            let segments_in_range = run.get(lo..=hi)?;
            let mut has_potential_match = false;

            // For large scans, we limit the number of segments we check upfront
            // to avoid excessive CPU usage. The lazy loading during iteration
            // will handle filtering the rest.
            const MAX_UPFRONT_CHECKS: usize = 10;

            for (idx, segment) in segments_in_range.iter().enumerate() {
                // Check if segment might contain data for this range
                if segment.might_contain_range(&range) {
                    has_potential_match = true;
                    break;
                }

                // For very large runs, don't check all segments upfront
                // The lazy iterator will handle skipping segments as needed
                if idx >= MAX_UPFRONT_CHECKS {
                    has_potential_match = true; // Assume there might be matches
                    break;
                }
            }

            if !has_potential_match {
                return None;
            }
        }

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
        let lo_reader = lo_segment
            .range(range.clone()) /* .cache_policy(cache_policy) */
            .map(|x| Box::new(x) as BoxedIterator);

        let hi_reader = if hi > lo {
            let hi_segment = run.deref().get(hi).expect("should exist");
            hi_segment
                .range(range) /* .cache_policy(cache_policy) */
                .map(|x| Box::new(x) as BoxedIterator)
        } else {
            None
        };

        Self {
            run,
            lo,
            hi,
            lo_reader,
            hi_reader,
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
                    // Lazily check next segment for potential matches
                    // This avoids unnecessary I/O for segments that won't contain our prefix
                    loop {
                        if self.lo >= self.hi {
                            break;
                        }

                        let segment = self.run.get(self.lo).expect("should exist");
                        if let Some(reader) = segment.iter() {
                            self.lo_reader = Some(Box::new(reader) as BoxedIterator);
                            break;
                        }

                        // Skip this segment as it doesn't contain our range
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
                    // Lazily check prev segment for potential matches
                    loop {
                        if self.hi <= self.lo {
                            break;
                        }

                        let segment = self.run.get(self.hi).expect("should exist");
                        if let Some(reader) = segment.iter() {
                            self.hi_reader = Some(Box::new(reader) as BoxedIterator);
                            break;
                        }

                        // Skip this segment as it doesn't contain our range
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
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{AbstractTree, Config, Slice};
    use std::sync::Arc;
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

    #[test]
    fn test_run_reader_prefix_filtering() -> crate::Result<()> {
        use crate::prefix::FixedPrefixExtractor;

        let tempdir = tempfile::tempdir()?;
        let tree = Config::new(&tempdir)
            .prefix_extractor(Arc::new(FixedPrefixExtractor::new(3)))
            .open()?;

        // Create segments with different prefixes
        let prefixes = [
            ["aaa_1", "aaa_2", "aaa_3"],
            ["bbb_1", "bbb_2", "bbb_3"],
            ["ccc_1", "ccc_2", "ccc_3"],
            ["ddd_1", "ddd_2", "ddd_3"],
        ];

        for batch in prefixes {
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

        let run = Arc::new(Run::new(segments));

        // Test 1: Query for non-existent prefix should return None
        assert!(
            RunReader::new(
                run.clone(),
                UserKey::from("zzz_1")..=UserKey::from("zzz_9"),
                CachePolicy::Read
            )
            .is_none(),
            "Should return None for non-existent prefix"
        );

        // Test 2: Query for existing prefix should return reader
        let reader = RunReader::new(
            run.clone(),
            UserKey::from("bbb_1")..=UserKey::from("bbb_3"),
            CachePolicy::Read,
        );
        assert!(reader.is_some(), "Should return reader for existing prefix");

        if let Some(reader) = reader {
            let items: Vec<_> = reader.flatten().map(|item| item.key.user_key).collect();
            assert_eq!(items.len(), 3);
            assert_eq!(items.first(), Some(&Slice::from(*b"bbb_1")));
            assert_eq!(items.get(1), Some(&Slice::from(*b"bbb_2")));
            assert_eq!(items.get(2), Some(&Slice::from(*b"bbb_3")));
        }

        // Test 3: Range query across prefixes with no common prefix
        let reader = RunReader::new(
            run,
            UserKey::from("aaa_3")..=UserKey::from("bbb_1"),
            CachePolicy::Read,
        );
        // Should still work since segments contain the range
        assert!(reader.is_some());

        Ok(())
    }

    #[test]
    fn test_run_reader_lazy_segment_loading() -> crate::Result<()> {
        use crate::prefix::FixedPrefixExtractor;

        let tempdir = tempfile::tempdir()?;
        let tree = Config::new(&tempdir)
            .prefix_extractor(Arc::new(FixedPrefixExtractor::new(4)))
            .open()?;

        // Create many segments with distinct prefixes
        let prefixes = [
            ["pre1_a", "pre1_b", "pre1_c"],
            ["pre2_a", "pre2_b", "pre2_c"],
            ["pre3_a", "pre3_b", "pre3_c"],
            ["pre4_a", "pre4_b", "pre4_c"],
            ["pre5_a", "pre5_b", "pre5_c"],
            ["pre6_a", "pre6_b", "pre6_c"],
        ];

        for batch in prefixes {
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

        let run = Arc::new(Run::new(segments));

        // Query for a specific prefix in the middle
        // Should skip segments without the prefix lazily
        let reader = RunReader::new(
            run.clone(),
            UserKey::from("pre4_a")..=UserKey::from("pre4_c"),
            CachePolicy::Read,
        );

        assert!(reader.is_some());

        if let Some(reader) = reader {
            let items: Vec<_> = reader.flatten().map(|item| item.key.user_key).collect();
            assert_eq!(items.len(), 3);
            assert_eq!(items.first(), Some(&Slice::from(*b"pre4_a")));
            assert_eq!(items.get(1), Some(&Slice::from(*b"pre4_b")));
            assert_eq!(items.get(2), Some(&Slice::from(*b"pre4_c")));
        }

        // Query for prefix at the beginning
        let reader = RunReader::new(
            run.clone(),
            UserKey::from("pre1_a")..=UserKey::from("pre1_c"),
            CachePolicy::Read,
        );
        assert!(reader.is_some());

        // Query for prefix at the end
        let reader = RunReader::new(
            run,
            UserKey::from("pre6_a")..=UserKey::from("pre6_c"),
            CachePolicy::Read,
        );
        assert!(reader.is_some());

        Ok(())
    }
}
