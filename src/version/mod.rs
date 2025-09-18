// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod optimize;
pub mod run;

pub use run::Run;

use crate::{
    coding::Encode,
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

    // We purposefully use Arc<_> to avoid deep cloning the blob files again and again
    //
    // Changing the value log tends to happen way less often than other modifications to the
    // LSM-tree
    /// Blob files for large values (value log)
    pub(crate) value_log: Arc<BTreeMap<BlobFileId, BlobFile>>,
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

    /// Creates a new empty version.
    pub fn new(id: VersionId) -> Self {
        let levels = (0..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()).collect();

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: Arc::default(),
            }),
            seqno_watermark: 0,
        }
    }

    /// Creates a new pre-populated version.
    pub fn from_levels(
        id: VersionId,
        levels: Vec<Level>,
        blob_files: BTreeMap<BlobFileId, BlobFile>,
    ) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: Arc::new(blob_files),
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

    /// Gets the n-th level.
    pub fn level(&self, n: usize) -> Option<&Level> {
        self.levels.get(n)
    }

    /// Creates a new version with the additional run added to the "top" of L0.
    pub fn with_new_l0_run(&self, run: &[Segment], blob_files: Option<&[BlobFile]>) -> Self {
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

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log,
            }),
            seqno_watermark: 0,
        }
    }

    /// Returns a new version with a list of segments removed.
    ///
    /// The segment files are not immediately deleted, this is handled in the compaction worker.
    pub fn with_dropped(&self, ids: &[SegmentId]) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        for level in &self.levels {
            let runs = level
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

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: self.value_log.clone(),
            }),
            seqno_watermark: 0,
        }
    }

    pub fn with_merge(
        &self,
        old_ids: &[SegmentId],
        new_segments: &[Segment],
        dest_level: usize,
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

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                value_log: self.value_log.clone(),
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
            }),
            seqno_watermark: 0,
        }
    }
}

impl Encode for Version {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), crate::EncodeError> {
        use crate::file::MAGIC_BYTES;
        use byteorder::{LittleEndian, WriteBytesExt};

        // Magic
        writer.write_all(&MAGIC_BYTES)?;

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

        // Blob file count
        // NOTE: We know there are always less than 4 billion blob files
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u32::<LittleEndian>(self.value_log.len() as u32)?;

        for file in self.value_log.values() {
            writer.write_u64::<LittleEndian>(file.id())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn version_encode_empty() {
        let bytes = Version::new(0).encode_into_vec();

        #[rustfmt::skip]
        let raw = &[
            // Magic
            b'L', b'S', b'M', 3,

            // Level count
            7,

            // L0 runs
            0,
            // L1 runs
            0,
            // L2 runs
            0,
            // L3 runs
            0,
            // L4 runs
            0,
            // L5 runs
            0,
            // L6 runs
            0,

            // Blob file count
            0,
            0,
            0,
            0,
        ];

        assert_eq!(bytes, raw);
    }
}
