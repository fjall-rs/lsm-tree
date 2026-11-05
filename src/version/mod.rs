// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod blob_file_list;
mod edit;
mod hydrate;
mod optimize;
mod raw;
pub mod run;
mod super_version;

pub use blob_file_list::BlobFileList;
pub use edit::{EditRecord, VersionEdit};
pub use hydrate::hydrate;
pub use raw::{BlobFileRef, RawVersionData, TableRef};
pub use run::Run;
pub use super_version::DeletedFiles;
pub use super_version::{SuperVersion, SuperVersionSet};

use crate::blob_tree::FragmentationMap;
use crate::compaction::state::hidden_set::HiddenSet;
use crate::{
    vlog::{BlobFile, BlobFileId},
    HashSet, KeyRange, SeqNo, Table, TableId,
};
use optimize::optimize_runs;
use run::Ranged;
use std::{ops::Deref, sync::Arc};

pub const DEFAULT_LEVEL_COUNT: u8 = 7;

/// Monotonically increasing ID of a version.
pub type VersionId = u64;

impl Ranged for Table {
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

    pub fn table_count(&self) -> usize {
        self.iter().map(|x| x.len()).sum()
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn is_disjoint(&self) -> bool {
        self.run_count() <= 1
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &Arc<Run<T>>> {
        self.runs.iter()
    }
}

#[derive(Clone)]
pub struct Level(Arc<GenericLevel<Table>>);

impl std::ops::Deref for Level {
    type Target = GenericLevel<Table>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Level {
    pub fn empty() -> Self {
        Self::from_runs(vec![])
    }

    pub fn from_runs(runs: Vec<Arc<Run<Table>>>) -> Self {
        Self(Arc::new(GenericLevel { runs }))
    }

    pub fn list_ids(&self) -> HashSet<TableId> {
        self.iter()
            .flat_map(|run| run.iter())
            .map(Table::id)
            .collect()
    }

    pub fn first_run(&self) -> Option<&Arc<Run<Table>>> {
        self.runs.first()
    }

    /// Returns the on-disk size of the level.
    pub fn size(&self) -> u64 {
        self.0
            .iter()
            .flat_map(|x| x.iter())
            .map(Table::file_size)
            .sum()
    }

    pub fn aggregate_key_range(&self) -> KeyRange {
        if self.run_count() == 1 {
            #[expect(
                clippy::expect_used,
                reason = "we check for run_count, so the first run must exist"
            )]
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

    /// Sequence number at which this version became visible.
    seqno: SeqNo,

    /// Every item with a seqno at or below this is in a table, or was removed
    /// on purpose (flush, compaction, drop range, clear).
    ///
    /// Moved up by flushes (highest seqno of the flushed memtables, which may be above the
    /// created tables, because a flush can drop its newest items) and clears (the clear's seqno),
    /// never by compactions, so dropped items stay dropped.
    ///
    /// `None` if nothing was flushed yet.
    flush_watermark: Option<SeqNo>,

    /// The individual LSM-tree levels which consist of runs of tables
    levels: Vec<Level>,

    // NOTE: We purposefully use Arc<_> to avoid deep cloning the blob files again and again
    //
    // Changing the value log tends to happen way less often than other modifications to the
    // LSM-tree
    //
    /// Blob files for large values (value log)
    #[doc(hidden)]
    pub blob_files: Arc<BlobFileList>,

    /// Blob file fragmentation
    gc_stats: Arc<FragmentationMap>,
}

/// A version is an immutable, point-in-time view of a tree's structure
///
/// Any time a table is created or deleted, a new version is created.
#[derive(Clone)]
pub struct Version {
    inner: Arc<VersionInner>,
}

impl std::ops::Deref for Version {
    type Target = VersionInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Prints the version's layout: every non-empty level, its runs (newest first)
/// and their tables, plus the blob files
impl std::fmt::Debug for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Version #{} (seqno {}, flush watermark {:?})",
            self.id, self.seqno, self.flush_watermark,
        )?;

        for (level_idx, level) in self.levels.iter().enumerate() {
            if level.is_empty() {
                continue;
            }

            writeln!(f, "  L{level_idx}:")?;

            for (run_idx, run) in level.iter().enumerate() {
                writeln!(f, "    run #{run_idx}:")?;

                for table in run.iter() {
                    writeln!(
                        f,
                        "      table {} [{:?}..={:?}] seqno {}..={}",
                        table.id(),
                        String::from_utf8_lossy(table.metadata.key_range.min()),
                        String::from_utf8_lossy(table.metadata.key_range.max()),
                        table.get_lowest_seqno(),
                        table.get_highest_seqno(),
                    )?;
                }
            }
        }

        let mut blob_file_ids = self.blob_files.list_ids().copied().collect::<Vec<_>>();
        blob_file_ids.sort_unstable();
        write!(f, "  blob files: {blob_file_ids:?}")
    }
}

// TODO: impl using generics so we can easily unit test Version transformation functions
impl Version {
    /// Returns the version ID.
    #[doc(hidden)]
    #[must_use]
    pub fn id(&self) -> VersionId {
        self.id
    }

    /// Returns the sequence number at which this version became visible.
    #[must_use]
    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    /// Returns the flush watermark: every item with a seqno at or below it
    /// is in a table, or was removed on purpose (flush, compaction, drop range, clear).
    #[must_use]
    pub fn flush_watermark(&self) -> Option<SeqNo> {
        self.flush_watermark
    }

    #[doc(hidden)]
    pub fn gc_stats(&self) -> &FragmentationMap {
        &self.gc_stats
    }

    pub(crate) fn l0(&self) -> &Level {
        #[expect(clippy::expect_used)]
        self.levels.first().expect("L0 should exist")
    }

    #[must_use]
    pub(crate) fn level_is_busy(&self, idx: usize, hidden_set: &HiddenSet) -> bool {
        self.level(idx).is_some_and(|level| {
            level
                .iter()
                .flat_map(|run| run.iter())
                .any(|table| hidden_set.is_hidden(table.id()))
        })
    }

    /// Creates a new empty version.
    pub(crate) fn new(id: VersionId, version_seqno: SeqNo, flush_watermark: Option<SeqNo>) -> Self {
        let levels = (0..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()).collect();

        Self {
            inner: Arc::new(VersionInner {
                id,
                seqno: version_seqno,
                flush_watermark,
                levels,
                blob_files: Arc::default(),
                gc_stats: Arc::default(),
            }),
        }
    }

    // TODO: v4 maybe move into legacy
    /// Creates a new pre-populated version.
    pub(crate) fn from_levels(
        id: VersionId,
        levels: Vec<Level>,
        blob_files: BlobFileList,
        gc_stats: FragmentationMap,
        version_seqno: SeqNo,
        flush_watermark: Option<SeqNo>,
    ) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id,
                seqno: version_seqno,
                flush_watermark,
                levels,
                blob_files: Arc::new(blob_files),
                gc_stats: Arc::new(gc_stats),
            }),
        }
    }

    /// Returns edit records that rebuild this version
    /// when applied to an empty [`RawVersionData`].
    ///
    /// Used to write a full snapshot of a version into a manifest,
    /// e.g. when migrating a tree or rotating a manifest.
    #[must_use]
    pub fn get_edit_records(&self) -> Vec<EditRecord> {
        let mut records = Vec::new();

        for (level, lvl) in self.iter_levels().enumerate() {
            // IMPORTANT: `RawVersionData::apply` puts every added run at the front
            // of its level (newest first), so emit the oldest run first
            // to rebuild the same run order
            for run in lvl.iter().rev() {
                records.push(EditRecord::AddTables {
                    level,
                    tables: run.iter().map(TableRef::from).collect(),
                });
            }
        }

        if self.blob_file_count() > 0 {
            records.push(EditRecord::AddBlobFiles {
                blob_files: self.blob_files.iter().map(BlobFileRef::from).collect(),
            });
        }

        if !self.gc_stats.is_empty() {
            records.push(EditRecord::UpdateFragMap(self.gc_stats().clone()));
        }

        if let Some(flush_watermark) = self.flush_watermark() {
            records.push(EditRecord::SetFlushWatermark(flush_watermark));
        }

        records.push(EditRecord::SetVersionSeqno(self.seqno()));

        records
    }

    /// Returns the number of levels.
    pub(crate) fn level_count(&self) -> usize {
        self.levels.len()
    }

    /// Returns an iterator through all levels.
    pub(crate) fn iter_levels(&self) -> impl Iterator<Item = &Level> {
        self.levels.iter()
    }

    /// Returns the number of tables in all levels.
    pub(crate) fn table_count(&self) -> usize {
        self.iter_levels().map(|x| x.table_count()).sum()
    }

    /// Returns the number of blob files.
    pub(crate) fn blob_file_count(&self) -> usize {
        self.blob_files.len()
    }

    /// Returns an iterator over all tables.
    pub fn iter_tables(&self) -> impl Iterator<Item = &Table> {
        self.levels
            .iter()
            .flat_map(|x| x.iter())
            .flat_map(|x| x.iter())
    }

    pub(crate) fn get_table(&self, id: TableId) -> Option<&Table> {
        self.iter_tables().find(|x| x.metadata.id == id)
    }

    /// Gets the n-th level.
    #[doc(hidden)]
    #[must_use]
    pub fn level(&self, n: usize) -> Option<&Level> {
        self.levels.get(n)
    }

    /// Creates a new version with the additional run added to the "top" of L0.
    pub(crate) fn with_new_l0_run(
        &self,
        run: &[Table],
        blob_files: Option<&[BlobFile]>,
        diff: Option<FragmentationMap>,
        flush_watermark: Option<SeqNo>,
        version_seqno: SeqNo,
    ) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        // L0
        levels.push({
            // Copy-on-write the first level with new run at top

            #[expect(clippy::expect_used, reason = "L0 always exists")]
            let l0 = self.levels.first().expect("L0 should always exist");

            let prev_runs = l0
                .runs
                .iter()
                .map(|run| {
                    let run: Run<_> = run.deref().clone();
                    run
                })
                .collect::<Vec<_>>();

            let mut runs = Vec::with_capacity(prev_runs.len() + 1);

            if let Some(run) = Run::new(run.to_vec()) {
                runs.push(run);
            }

            runs.extend(prev_runs);

            let runs = optimize_runs(runs);

            Level::from_runs(runs.into_iter().map(Arc::new).collect())
        });

        // L1+
        levels.extend(self.levels.iter().skip(1).cloned());

        // Value log
        let value_log = if let Some(blob_files) = blob_files {
            let mut copy = self.blob_files.deref().clone();
            copy.extend(blob_files.iter().cloned().map(|bf| (bf.id(), bf)));
            copy.into()
        } else {
            self.blob_files.clone()
        };

        let gc_stats = if let Some(diff) = diff {
            let mut copy = self.gc_stats.deref().clone();
            diff.merge_into(&mut copy);
            copy.prune(&value_log);
            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                seqno: version_seqno,
                flush_watermark: flush_watermark.or(self.flush_watermark),
                levels,
                blob_files: value_log,
                gc_stats,
            }),
        }
    }

    /// Returns a new version with a list of tables removed.
    ///
    /// The table files are not immediately deleted, this is handled
    /// by the version system's free list.
    ///
    /// Returns a new version without the given tables.
    pub(crate) fn with_dropped(
        &self,
        ids: &[TableId],
        diff: Option<FragmentationMap>,
        blob_files_to_drop: &[BlobFileId],
        version_seqno: SeqNo,
    ) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        for level in &self.levels {
            let runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let value_log = if blob_files_to_drop.is_empty() {
            self.blob_files.clone()
        } else {
            let mut copy = self.blob_files.deref().clone();

            for &id in blob_files_to_drop {
                copy.remove(id);
            }

            Arc::new(copy)
        };

        let gc_stats = if diff.is_some() || !blob_files_to_drop.is_empty() {
            let mut copy = self.gc_stats.deref().clone();

            if let Some(diff) = diff {
                diff.merge_into(&mut copy);
            }

            copy.prune(&value_log);

            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                seqno: version_seqno,
                flush_watermark: self.flush_watermark,
                levels,
                blob_files: value_log,
                gc_stats,
            }),
        }
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn with_merge(
        &self,
        old_ids: &[TableId],
        new_tables: &[Table],
        dest_level: usize,
        diff: Option<FragmentationMap>,
        new_blob_files: Vec<BlobFile>,
        blob_files_to_drop: &Vec<BlobFileId>,
        version_seqno: SeqNo,
    ) -> Self {
        let id = self.id + 1;

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !old_ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level {
                if let Some(run) = Run::new(new_tables.to_vec()) {
                    runs.insert(0, run);
                }
            }

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let has_diff = diff.is_some();

        let value_log = if has_diff || !new_blob_files.is_empty() || !blob_files_to_drop.is_empty()
        {
            let mut copy = self.blob_files.deref().clone();

            for blob_file in new_blob_files {
                copy.insert(blob_file.id(), blob_file);
            }

            for &id in blob_files_to_drop {
                copy.remove(id);
            }

            Arc::new(copy)
        } else {
            self.blob_files.clone()
        };

        let gc_stats = if has_diff || !blob_files_to_drop.is_empty() {
            let mut copy = self.gc_stats.deref().clone();

            if let Some(diff) = diff {
                diff.merge_into(&mut copy);
            }

            copy.prune(&value_log);

            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                seqno: version_seqno,
                flush_watermark: self.flush_watermark,
                levels,
                blob_files: value_log,
                gc_stats,
            }),
        }
    }

    pub(crate) fn with_moved(
        &self,
        ids: &[TableId],
        dest_level: usize,
        version_seqno: SeqNo,
    ) -> Self {
        let id = self.id + 1;

        let affected_tables = self
            .iter_tables()
            .filter(|x| ids.contains(&x.id()))
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(affected_tables.len(), ids.len(), "invalid table IDs");

        let mut levels = vec![];

        for (level_idx, level) in self.levels.iter().enumerate() {
            let mut runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level {
                if let Some(run) = Run::new(affected_tables.clone()) {
                    runs.insert(0, run);
                }
            }

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        Self {
            inner: Arc::new(VersionInner {
                id,
                seqno: version_seqno,
                flush_watermark: self.flush_watermark,
                levels,
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
            }),
        }
    }
}
