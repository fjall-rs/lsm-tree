// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod optimize;
pub mod run;

pub use run::Run;

use crate::blob_tree::{FragmentationEntry, FragmentationMap};
use crate::coding::Encode;
use crate::{
    vlog::{BlobFile, BlobFileId},
    HashSet, KeyRange, Segment, SegmentId, SeqNo,
};
use optimize::optimize_runs;
use run::Ranged;
use std::{collections::BTreeMap, ops::Deref, sync::Arc};

pub const DEFAULT_LEVEL_COUNT: u8 = 7;

/// Monotonically increasing ID of a version.
pub type VersionId = u64;

impl Ranged for Segment {
    fn key_range(&self) -> &KeyRange {
        &self.metadata.key_range
    }
}

pub struct GenericLevel<T: Ranged> {
    runs: Vec<Arc<Run<T>>>,
}

impl<T: Ranged> std::ops::Deref for GenericLevel<T> {
    type Target = [Arc<Run<T>>];

    fn deref(&self) -> &Self::Target {
        &self.runs
    }
}

impl<T: Ranged> GenericLevel<T> {
    pub fn new(runs: Vec<Arc<Run<T>>>) -> Self {
        Self { runs }
    }

    pub fn segment_count(&self) -> usize {
        self.iter().map(|x| x.len()).sum()
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn is_disjoint(&self) -> bool {
        self.run_count() == 1
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Arc<Run<T>>> {
        self.runs.iter()
    }

    pub fn get_for_key<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a T> {
        self.iter().filter_map(|x| x.get_for_key(key))
    }

    pub fn get_overlapping<'a>(&'a self, key_range: &'a KeyRange) -> impl Iterator<Item = &'a T> {
        self.iter().flat_map(|x| x.get_overlapping(key_range))
    }

    pub fn get_contained<'a>(&'a self, key_range: &'a KeyRange) -> impl Iterator<Item = &'a T> {
        self.iter().flat_map(|x| x.get_contained(key_range))
    }
}

#[derive(Clone)]
pub struct Level(Arc<GenericLevel<Segment>>);

impl std::ops::Deref for Level {
    type Target = GenericLevel<Segment>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Level {
    pub fn empty() -> Self {
        Self::from_runs(vec![])
    }

    pub fn from_runs(runs: Vec<Arc<Run<Segment>>>) -> Self {
        Self(Arc::new(GenericLevel { runs }))
    }

    pub fn list_ids(&self) -> HashSet<SegmentId> {
        self.iter()
            .flat_map(|run| run.iter())
            .map(Segment::id)
            .collect()
    }

    pub fn first_run(&self) -> Option<&Arc<Run<Segment>>> {
        self.runs.first()
    }

    /// Returns the on-disk size of the level.
    pub fn size(&self) -> u64 {
        self.0
            .iter()
            .flat_map(|x| x.iter())
            .map(Segment::file_size)
            .sum()
    }

    pub fn aggregate_key_range(&self) -> KeyRange {
        if self.run_count() == 1 {
            // NOTE: We check for run_count, so the first run must exist
            #[allow(clippy::expect_used)]
            self.runs
                .first()
                .expect("should exist")
                .aggregate_key_range()
        } else {
            let key_ranges = self
                .iter()
                .map(|x| Run::aggregate_key_range(x))
                .collect::<Vec<_>>();

            KeyRange::aggregate(key_ranges.iter())
        }
    }
}

pub struct VersionInner {
    /// The version's ID
    id: VersionId,

    /// The individual LSM-tree levels which consist of runs of tables
    pub(crate) levels: Vec<Level>,

    // TODO: 3.0.0 this should really be a newtype
    // NOTE: We purposefully use Arc<_> to avoid deep cloning the blob files again and again
    //
    // Changing the value log tends to happen way less often than other modifications to the
    // LSM-tree
    //
    /// Blob files for large values (value log)
    pub(crate) value_log: Arc<BTreeMap<BlobFileId, BlobFile>>,

    /// Blob file fragmentation
    gc_stats: Arc<FragmentationMap>,
}

/// A version is an immutable, point-in-time view of a tree's structure
///
/// Any time a segment is created or deleted, a new version is created.
#[derive(Clone)]
pub struct Version {
    inner: Arc<VersionInner>,

    /// The sequence number at the time the version was installed
    ///
    /// We keep all versions that have `seqno_watermark` > `mvcc_watermark` to prevent
    /// snapshots losing data
    pub(crate) seqno_watermark: SeqNo,
}

impl std::ops::Deref for Version {
    type Target = VersionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// TODO: impl using generics so we can easily unit test Version transformation functions
impl Version {
    /// Returns the version ID.
    pub fn id(&self) -> VersionId {
        self.id
    }

    pub fn gc_stats(&self) -> &FragmentationMap {
        &self.gc_stats
    }

    /// Creates a new empty version.
    pub fn new(id: VersionId) -> Self {
        let levels = (0..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()).collect();

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: Arc::default(),
                gc_stats: Arc::default(),
            }),
            seqno_watermark: 0,
        }
    }

    /// Creates a new pre-populated version.
    pub fn from_levels(
        id: VersionId,
        levels: Vec<Level>,
        blob_files: BTreeMap<BlobFileId, BlobFile>,
        gc_stats: FragmentationMap,
    ) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: Arc::new(blob_files),
                gc_stats: Arc::new(gc_stats),
            }),
            seqno_watermark: 0,
        }
    }

    /// Returns the number of levels.
    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    /// Returns an iterator through all levels.
    pub fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.levels.iter()
    }

    /// Returns the number of segments in all levels.
    pub fn segment_count(&self) -> usize {
        self.iter_levels().map(|x| x.segment_count()).sum()
    }

    pub fn blob_file_count(&self) -> usize {
        self.value_log.len()
    }

    /// Returns an iterator over all segments.
    pub fn iter_segments(&self) -> impl Iterator<Item = &Segment> {
        self.levels
            .iter()
            .flat_map(|x| x.iter())
            .flat_map(|x| x.iter())
    }

    pub(crate) fn get_segment(&self, id: SegmentId) -> Option<&Segment> {
        self.iter_segments().find(|x| x.metadata.id == id)
    }

    /// Gets the n-th level.
    pub fn level(&self, n: usize) -> Option<&Level> {
        self.levels.get(n)
    }

    /// Creates a new version with the additional run added to the "top" of L0.
    pub fn with_new_l0_run(
        &self,
        run: &[Segment],
        blob_files: Option<&[BlobFile]>,
        diff: Option<FragmentationMap>,
    ) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        // L0
        levels.push({
            // Copy-on-write the first level with new run at top

            // NOTE: We always have at least one level
            #[allow(clippy::expect_used)]
            let l0 = self.levels.first().expect("L0 should always exist");

            let prev_runs = l0
                .runs
                .iter()
                .map(|run| {
                    let run: Run<Segment> = run.deref().clone();
                    run
                })
                .collect::<Vec<_>>();

            let mut runs = Vec::with_capacity(prev_runs.len() + 1);
            runs.push(Run::new(run.to_vec()));
            runs.extend(prev_runs);

            let runs = optimize_runs(runs);

            Level::from_runs(runs.into_iter().map(Arc::new).collect())
        });

        // L1+
        levels.extend(self.levels.iter().skip(1).cloned());

        // Value log
        let value_log = if let Some(blob_files) = blob_files {
            let mut copy = self.value_log.deref().clone();
            copy.extend(blob_files.iter().cloned().map(|bf| (bf.id(), bf)));
            copy.into()
        } else {
            self.value_log.clone()
        };

        let gc_map = if let Some(diff) = diff {
            let mut copy = self.gc_stats.deref().clone();
            diff.merge_into(&mut copy);
            copy.prune(&self.value_log);
            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log,
                gc_stats: gc_map,
            }),
            seqno_watermark: 0,
        }
    }

    /// Returns a new version with a list of segments removed.
    ///
    /// The segment files are not immediately deleted, this is handled by the version system's free list.
    pub fn with_dropped(&self, ids: &[SegmentId]) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        let mut dropped_segments = vec![];

        for level in &self.levels {
            let runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<Segment> = run.deref().clone();

                    let removed_segments = run
                        .inner_mut()
                        .extract_if(.., |x| ids.contains(&x.metadata.id));

                    dropped_segments = removed_segments.collect();

                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let gc_stats = if dropped_segments.is_empty() {
            self.gc_stats.clone()
        } else {
            let mut copy = self.gc_stats.deref().clone();

            for segment in &dropped_segments {
                let linked_blob_files = segment
                    .get_linked_blob_files()
                    .expect("TODO: handle error")
                    .unwrap_or_default();

                for blob_file in linked_blob_files {
                    copy.entry(blob_file.blob_file_id)
                        .and_modify(|counter| {
                            counter.bytes += blob_file.bytes;
                            counter.len += blob_file.len;
                        })
                        .or_insert_with(|| FragmentationEntry::new(blob_file.len, blob_file.bytes));
                }
            }

            Arc::new(copy)
        };

        let value_log = if dropped_segments.is_empty() {
            self.value_log.clone()
        } else {
            // TODO: 3.0.0 this should really be a newtype
            let mut copy = self.value_log.deref().clone();
            copy.retain(|_, blob_file| !blob_file.is_dead(&gc_stats));
            Arc::new(copy)
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log,
                gc_stats,
            }),
            seqno_watermark: 0,
        }
    }

    pub fn with_merge(
        &self,
        old_ids: &[SegmentId],
        new_segments: &[Segment],
        dest_level: usize,
        diff: Option<FragmentationMap>,
        new_blob_files: Vec<BlobFile>,
        blob_files_to_drop: HashSet<BlobFileId>,
    ) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<Segment> = run.deref().clone();
                    run.retain(|x| !old_ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level {
                runs.insert(0, Run::new(new_segments.to_vec()));
            }

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let has_diff = diff.is_some();

        let gc_stats = if has_diff || !blob_files_to_drop.is_empty() {
            let mut copy = self.gc_stats.deref().clone();

            if let Some(diff) = diff {
                diff.merge_into(&mut copy);
            }

            for id in &blob_files_to_drop {
                copy.remove(id);
            }

            copy.prune(&self.value_log);

            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        let value_log = if has_diff || !new_blob_files.is_empty() {
            let mut copy = self.value_log.deref().clone();

            for blob_file in new_blob_files {
                copy.insert(blob_file.id(), blob_file);
            }

            for id in blob_files_to_drop {
                copy.remove(&id);
            }

            Arc::new(copy)
        } else {
            self.value_log.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log,
                gc_stats,
            }),
            seqno_watermark: 0,
        }
    }

    pub fn with_moved(&self, ids: &[SegmentId], dest_level: usize) -> Self {
        let id = self.id + 1;

        let affected_segments = self
            .iter_segments()
            .filter(|x| ids.contains(&x.id()))
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(affected_segments.len(), ids.len(), "invalid segment IDs");

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<Segment> = run.deref().clone();
                    run.retain(|x| !ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level {
                runs.insert(0, Run::new(affected_segments.clone()));
            }

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: self.value_log.clone(),
                gc_stats: Arc::default(),
            }),
            seqno_watermark: 0,
        }
    }
}

impl Version {
    pub(crate) fn encode_into(&self, writer: &mut sfa::Writer) -> Result<(), crate::EncodeError> {
        use byteorder::{LittleEndian, WriteBytesExt};

        writer.start("tables")?;

        // Level count
        // NOTE: We know there are always less than 256 levels
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u8(self.level_count() as u8)?;

        for level in self.iter_levels() {
            // Run count
            // NOTE: We know there are always less than 256 runs
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u8(level.len() as u8)?;

            for run in level.iter() {
                // Segment count
                // NOTE: We know there are always less than 4 billion segments in a run
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32::<LittleEndian>(run.len() as u32)?;

                // Segment IDs
                for id in run.iter().map(Segment::id) {
                    writer.write_u64::<LittleEndian>(id)?;
                }
            }
        }

        writer.start("blob_files")?;

        // Blob file count
        // NOTE: We know there are always less than 4 billion blob files
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u32::<LittleEndian>(self.value_log.len() as u32)?;

        for file in self.value_log.values() {
            writer.write_u64::<LittleEndian>(file.id())?;
        }

        writer.start("blob_gc_stats")?;

        self.gc_stats.encode_into(writer)?;

        Ok(())
    }
}
