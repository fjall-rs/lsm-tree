// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod optimize;
pub mod run;

pub use run::Run;

use crate::{HashSet, KeyRange, Segment, SegmentId};
use optimize::optimize_runs;
use run::Ranged;
use std::{ops::Deref, sync::Arc};

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
            .map(|x| x.metadata.file_size)
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
    id: VersionId,
    pub(crate) levels: Vec<Level>,
}

/// A version is a point-in-time view of a tree's structure
///
/// Any time a segment is created or deleted, a new version is created.
#[derive(Clone)]
pub struct Version(Arc<VersionInner>);

impl std::ops::Deref for Version {
    type Target = VersionInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// TODO: impl using generics so we can easily unit test Version transformation functions
impl Version {
    pub fn id(&self) -> VersionId {
        self.id
    }

    pub fn new(id: VersionId) -> Self {
        let levels = (0..7).map(|_| Level::empty()).collect();

        Self(Arc::new(VersionInner { id, levels }))
    }

    pub fn from_levels(id: VersionId, levels: Vec<Level>) -> Self {
        Self(Arc::new(VersionInner { id, levels }))
    }

    /// Returns the amount of levels.
    pub fn level_count(&self) -> usize {
        self.levels.len()
    }

    pub fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.levels.iter()
    }

    pub fn segment_count(&self) -> usize {
        self.iter_levels().map(|x| x.segment_count()).sum()
    }

    pub fn iter_segments(&self) -> impl Iterator<Item = &Segment> {
        self.levels
            .iter()
            .flat_map(|x| x.iter())
            .flat_map(|x| x.iter())
    }

    pub fn level(&self, n: usize) -> Option<&Level> {
        self.levels.get(n)
    }

    pub fn with_new_l0_segment(&self, run: &[Segment]) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        // L0
        levels.push({
            // Copy-on-write the first level with new run at top
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

        Self(Arc::new(VersionInner { id, levels }))
    }

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

        Self(Arc::new(VersionInner { id, levels }))
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

        Self(Arc::new(VersionInner { id, levels }))
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

        Self(Arc::new(VersionInner { id, levels }))
    }
}
