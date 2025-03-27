// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{segment::meta::SegmentId, HashSet, KeyRange, Segment, UserKey};
use std::ops::Bound;

/// Level of an LSM-tree
#[derive(Clone, Debug)]
pub struct Level {
    /// List of segments
    #[doc(hidden)]
    pub segments: Vec<Segment>,

    /// If the level is disjoint
    ///
    /// is only recomputed when the level is changed
    /// to avoid unnecessary CPU work
    pub is_disjoint: bool,
    // pub key_range: KeyRange,
}

impl std::fmt::Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for segment in self.segments.iter().rev().take(2).rev() {
            write!(f, "[{}]", segment.id())?;
        }
        Ok(())
    }
}

impl std::ops::Deref for Level {
    type Target = Vec<Segment>;

    fn deref(&self) -> &Self::Target {
        &self.segments
    }
}

impl Default for Level {
    fn default() -> Self {
        Self {
            is_disjoint: true,
            segments: Vec::new(),
            // key_range: KeyRange::empty(),
        }
    }
}

impl Level {
    pub fn list_ids(&self) -> HashSet<SegmentId> {
        self.segments.iter().map(Segment::id).collect()
    }

    pub fn update_metadata(&mut self) {
        self.set_disjoint_flag();
        self.sort();
    }

    pub fn insert(&mut self, segment: Segment) {
        self.segments.push(segment);
        self.update_metadata();
    }

    pub fn remove(&mut self, segment_id: SegmentId) -> Option<Segment> {
        if let Some(idx) = self.segments.iter().position(|x| x.id() == segment_id) {
            let segment = self.segments.remove(idx);
            self.update_metadata();
            Some(segment)
        } else {
            None
        }
    }

    pub(crate) fn sort(&mut self) {
        if self.is_disjoint {
            self.sort_by_key_range();
        } else {
            self.sort_by_seqno();
        }
    }

    /// Sorts the level by key range ascending.
    ///
    /// segment 1   segment 2   segment 3
    /// [key:a]     [key:c]     [key:z]
    pub(crate) fn sort_by_key_range(&mut self) {
        self.segments
            .sort_by(|a, b| a.metadata.key_range.min().cmp(b.metadata.key_range.min()));
    }

    /// Sorts the level from newest to oldest.
    ///
    /// This will make segments with highest seqno get checked first,
    /// so if there are two versions of an item, the fresher one is seen first:
    ///
    /// segment 1     segment 2
    /// [key:asd:2]   [key:asd:1]
    ///
    /// point read ----------->
    pub(crate) fn sort_by_seqno(&mut self) {
        self.segments
            .sort_by(|a, b| b.metadata.seqnos.1.cmp(&a.metadata.seqnos.1));
    }

    /// Returns an iterator over the level's segment IDs.
    pub fn ids(&self) -> impl Iterator<Item = SegmentId> + '_ {
        self.segments.iter().map(Segment::id)
    }

    /// Returns `true` if the level contains no segments.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Returns the number of segments.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Returns the level size in bytes.
    pub fn size(&self) -> u64 {
        self.segments.iter().map(|x| x.metadata.file_size).sum()
    }

    pub(crate) fn compute_is_disjoint(&self) -> bool {
        let ranges = self
            .segments
            .iter()
            .map(|x| &x.metadata.key_range)
            .collect::<Vec<_>>();

        KeyRange::is_disjoint(&ranges)
    }

    /// Checks if the level is disjoint and caches the result in `is_disjoint`.
    fn set_disjoint_flag(&mut self) {
        self.is_disjoint = self.compute_is_disjoint();
    }

    /// Returns an iterator over segments in the level that have a key range
    /// overlapping the input key range.
    pub fn overlapping_segments<'a>(
        &'a self,
        key_range: &'a KeyRange,
    ) -> impl Iterator<Item = &'a Segment> {
        self.segments
            .iter()
            .filter(|x| x.metadata.key_range.overlaps_with_key_range(key_range))
    }

    /// Returns an iterator over segments in the level that have a key range
    /// fully contained in the input key range.
    pub fn contained_segments<'a>(
        &'a self,
        key_range: &'a KeyRange,
    ) -> impl Iterator<Item = &'a Segment> {
        self.segments
            .iter()
            .filter(|x| key_range.contains_range(&x.metadata.key_range))
    }

    pub fn as_disjoint(&self) -> Option<DisjointLevel<'_>> {
        if self.is_disjoint {
            Some(DisjointLevel(self))
        } else {
            None
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct DisjointLevel<'a>(&'a Level);

impl<'a> DisjointLevel<'a> {
    /// Returns the segment that possibly contains the key.
    pub fn get_segment_containing_key(&self, key: &[u8]) -> Option<Segment> {
        let level = &self.0;

        let idx = level
            .segments
            .partition_point(|x| x.metadata.key_range.max() < &key);

        level
            .segments
            .get(idx)
            .filter(|x| x.metadata.key_range.min() <= &key)
            .cloned()
    }

    // TODO: use a single custom binary search instead of partition_point... benchmark it and add some unit tests before
    pub fn range_indexes(
        &'a self,
        key_range: &'a (Bound<UserKey>, Bound<UserKey>),
    ) -> Option<(usize, usize)> {
        let level = &self.0;

        let lo = match &key_range.0 {
            Bound::Unbounded => 0,
            Bound::Included(start_key) => {
                level.partition_point(|segment| segment.metadata.key_range.max() < start_key)
            }
            Bound::Excluded(start_key) => {
                level.partition_point(|segment| segment.metadata.key_range.max() <= start_key)
            }
        };

        if lo >= level.len() {
            return None;
        }

        let hi = match &key_range.1 {
            Bound::Unbounded => level.len() - 1,
            Bound::Included(end_key) => {
                let idx =
                    level.partition_point(|segment| segment.metadata.key_range.min() <= end_key);

                if idx == 0 {
                    return None;
                }

                idx.saturating_sub(1) // To avoid underflow
            }
            Bound::Excluded(end_key) => {
                let idx =
                    level.partition_point(|segment| segment.metadata.key_range.min() < end_key);

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
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{
            block::offset::BlockOffset,
            block_index::{two_level_index::TwoLevelBlockIndex, BlockIndexImpl},
            file_offsets::FileOffsets,
            meta::{Metadata, SegmentId},
            Segment, SegmentInner,
        },
        AbstractTree, KeyRange, Slice,
    };
    use std::sync::{atomic::AtomicBool, Arc};
    use test_log::test;

    #[allow(clippy::expect_used)]
    fn fixture_segment(id: SegmentId, key_range: KeyRange) -> Segment {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        let block_index = TwoLevelBlockIndex::new((0, id).into(), block_cache.clone());
        let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

        SegmentInner {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index,

            offsets: FileOffsets {
                bloom_ptr: BlockOffset(0),
                range_filter_ptr: BlockOffset(0),
                index_block_ptr: BlockOffset(0),
                metadata_ptr: BlockOffset(0),
                range_tombstones_ptr: BlockOffset(0),
                tli_ptr: BlockOffset(0),
                pfx_ptr: BlockOffset(0),
            },

            metadata: Metadata {
                data_block_count: 0,
                index_block_count: 0,
                data_block_size: 4_096,
                index_block_size: 4_096,
                created_at: 0,
                id,
                file_size: 0,
                compression: crate::segment::meta::CompressionType::None,
                table_type: crate::segment::meta::TableType::Block,
                item_count: 0,
                key_count: 0,
                key_range,
                tombstone_count: 0,
                range_tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,

            bloom_filter: Some(crate::bloom::BloomFilter::with_fp_rate(1, 0.1)),

            path: "a".into(),
            is_deleted: AtomicBool::default(),
        }
        .into()
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn level_disjoint_cull() {
        let level = Level {
            is_disjoint: true,
            // key_range: KeyRange::empty(),
            segments: vec![
                fixture_segment(0, KeyRange::new((Slice::from("a"), Slice::from("c")))),
                fixture_segment(1, KeyRange::new((Slice::from("d"), Slice::from("g")))),
                fixture_segment(2, KeyRange::new((Slice::from("h"), Slice::from("k")))),
            ],
        };
        let level = level.as_disjoint().unwrap();

        {
            let range = (Bound::Unbounded, Bound::Included(Slice::from("0")));
            let indexes = level.range_indexes(&range);
            assert_eq!(None, indexes);
        }

        {
            let range = (Bound::Included(Slice::from("l")), Bound::Unbounded);
            let indexes = level.range_indexes(&range);
            assert_eq!(None, indexes);
        }

        {
            let range = (
                Bound::Included(Slice::from("d")),
                Bound::Included(Slice::from("g")),
            );
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((1, 1)), indexes);
        }

        {
            let range = (
                Bound::Excluded(Slice::from("d")),
                Bound::Included(Slice::from("g")),
            );
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((1, 1)), indexes);
        }

        {
            let range = (
                Bound::Included(Slice::from("d")),
                Bound::Excluded(Slice::from("h")),
            );
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((1, 1)), indexes);
        }

        {
            let range = (
                Bound::Included(Slice::from("d")),
                Bound::Included(Slice::from("h")),
            );
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((1, 2)), indexes);
        }

        {
            let range = (Bound::Included(Slice::from("d")), Bound::Unbounded);
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((1, 2)), indexes);
        }

        {
            let range = (
                Bound::Included(Slice::from("a")),
                Bound::Included(Slice::from("d")),
            );
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((0, 1)), indexes);
        }

        {
            let range = (
                Bound::Included(Slice::from("a")),
                Bound::Excluded(Slice::from("d")),
            );
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((0, 0)), indexes);
        }

        {
            let range = (Bound::Unbounded, Bound::Unbounded);
            let indexes = level.range_indexes(&range);
            assert_eq!(Some((0, 2)), indexes);
        }
    }

    #[test]
    fn level_disjoint() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(&folder).open()?;

        let mut x = 0_u64;

        for _ in 0..10 {
            for _ in 0..10 {
                let key = x.to_be_bytes();
                x += 1;
                tree.insert(key, key, 0);
            }
            tree.flush_active_memtable(0).expect("should flush");
        }

        assert!(
            tree.levels
                .read()
                .expect("lock is poisoned")
                .levels
                .first()
                .expect("should exist")
                .is_disjoint
        );

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn level_disjoint_containing_key() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(&folder).open()?;

        for k in 'c'..'k' {
            tree.insert([k as u8], "", 0);
            tree.flush_active_memtable(0).expect("should flush");
        }

        let first = tree
            .levels
            .read()
            .expect("lock is poisoned")
            .levels
            .first()
            .expect("should exist")
            .clone();

        let dis = first.as_disjoint().unwrap();
        assert!(dis.get_segment_containing_key(b"a").is_none());
        assert!(dis.get_segment_containing_key(b"b").is_none());
        for k in 'c'..'k' {
            assert!(dis.get_segment_containing_key(&[k as u8]).is_some());
        }
        assert!(dis.get_segment_containing_key(b"l").is_none());

        Ok(())
    }

    #[test]
    fn level_not_disjoint() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(&folder).open()?;

        for i in 0..10 {
            tree.insert("a", "", i);
            tree.insert("z", "", i);
            tree.flush_active_memtable(0).expect("should flush");
        }

        assert!(
            !tree
                .levels
                .read()
                .expect("lock is poisoned")
                .levels
                .first()
                .expect("should exist")
                .is_disjoint
        );

        Ok(())
    }

    #[test]
    fn level_overlaps() {
        let seg0 = fixture_segment(
            1,
            KeyRange::new((b"c".to_vec().into(), b"k".to_vec().into())),
        );
        let seg1 = fixture_segment(
            2,
            KeyRange::new((b"l".to_vec().into(), b"z".to_vec().into())),
        );

        let mut level = Level::default();
        level.insert(seg0);
        level.insert(seg1);

        assert_eq!(
            Vec::<SegmentId>::new(),
            level
                .overlapping_segments(&KeyRange::new((b"a".to_vec().into(), b"b".to_vec().into())))
                .map(Segment::id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            vec![1],
            level
                .overlapping_segments(&KeyRange::new((b"d".to_vec().into(), b"k".to_vec().into())))
                .map(Segment::id)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            vec![1, 2],
            level
                .overlapping_segments(&KeyRange::new((b"f".to_vec().into(), b"x".to_vec().into())))
                .map(Segment::id)
                .collect::<Vec<_>>(),
        );
    }
}
