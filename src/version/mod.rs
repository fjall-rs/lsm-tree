// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod blob_file_list;
mod optimize;
mod persist;
pub mod recovery;
pub mod run;
mod super_version;

pub use blob_file_list::BlobFileList;
pub use persist::persist_version;
pub use run::Run;
pub use super_version::{SuperVersion, SuperVersions};

use crate::blob_tree::{FragmentationEntry, FragmentationMap};
use crate::coding::Encode;
use crate::compaction::state::hidden_set::HiddenSet;
use crate::version::recovery::Recovery;
use crate::{
    vlog::{BlobFile, BlobFileId},
    HashSet, KeyRange, Table, TableId,
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

// TODO: impl using generics so we can easily unit test Version transformation functions
impl Version {
    /// Returns the version ID.
    pub fn id(&self) -> VersionId {
        self.id
    }

    pub fn gc_stats(&self) -> &FragmentationMap {
        &self.gc_stats
    }

    pub fn l0(&self) -> &Level {
        #[expect(clippy::expect_used)]
        self.levels.first().expect("L0 should exist")
    }

    #[must_use]
    pub fn level_is_busy(&self, idx: usize, hidden_set: &HiddenSet) -> bool {
        self.level(idx).is_some_and(|level| {
            level
                .iter()
                .flat_map(|run| run.iter())
                .any(|table| hidden_set.is_hidden(table.id()))
        })
    }

    /// Creates a new empty version.
    pub fn new(id: VersionId) -> Self {
        let levels = (0..DEFAULT_LEVEL_COUNT).map(|_| Level::empty()).collect();

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                blob_files: Arc::default(),
                gc_stats: Arc::default(),
            }),
        }
    }

    pub(crate) fn from_recovery(
        recovery: Recovery,
        tables: &[Table],
        blob_files: &[BlobFile],
    ) -> crate::Result<Self> {
        let version_levels = recovery
            .table_ids
            .iter()
            .map(|level| {
                let level_runs = level
                    .iter()
                    .map(|run| {
                        let run_tables = run
                            .iter()
                            .map(|&(table_id, _)| {
                                tables
                                    .iter()
                                    .find(|x| x.id() == table_id)
                                    .cloned()
                                    .ok_or(crate::Error::Unrecoverable)
                            })
                            .collect::<crate::Result<Vec<_>>>()?;

                        Ok(Arc::new(Run::new(run_tables)))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                Ok(Level::from_runs(level_runs))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self::from_levels(
            recovery.curr_version_id,
            version_levels,
            BlobFileList::new(blob_files.iter().cloned().map(|bf| (bf.id(), bf)).collect()),
            recovery.gc_stats,
        ))
    }

    /// Creates a new pre-populated version.
    pub fn from_levels(
        id: VersionId,
        levels: Vec<Level>,
        blob_files: BlobFileList,
        gc_stats: FragmentationMap,
    ) -> Self {
        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                blob_files: Arc::new(blob_files),
                gc_stats: Arc::new(gc_stats),
            }),
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

    /// Returns the number of tables in all levels.
    pub fn table_count(&self) -> usize {
        self.iter_levels().map(|x| x.table_count()).sum()
    }

    pub fn blob_file_count(&self) -> usize {
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
    pub fn level(&self, n: usize) -> Option<&Level> {
        self.levels.get(n)
    }

    /// Creates a new version with the additional run added to the "top" of L0.
    pub fn with_new_l0_run(
        &self,
        run: &[Table],
        blob_files: Option<&[BlobFile]>,
        diff: Option<FragmentationMap>,
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
            runs.push(Run::new(run.to_vec()));
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
                levels,
                blob_files: value_log,
                gc_stats,
            }),
        }
    }

    /// Returns a new version with a list of tables removed.
    ///
    /// The table files are not immediately deleted, this is handled by the version system's free list.
    pub fn with_dropped(
        &self,
        ids: &[TableId],
        dropped_blob_files: &mut Vec<BlobFile>,
    ) -> crate::Result<Self> {
        let id = self.id + 1;

        let mut levels = vec![];

        let mut dropped_tables: Vec<Table> = vec![];

        for level in &self.levels {
            let runs = level
                .runs
                .iter()
                .map(|run| {
                    // TODO: don't clone Arc inner if we don't need to modify
                    let mut run: Run<_> = run.deref().clone();

                    let removed_tables = run
                        .inner_mut()
                        .extract_if(.., |x| ids.contains(&x.metadata.id));

                    dropped_tables.extend(removed_tables);

                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        let gc_stats = if dropped_tables.is_empty() {
            self.gc_stats.clone()
        } else {
            let mut copy = self.gc_stats.deref().clone();

            for table in &dropped_tables {
                let linked_blob_files = table.list_blob_file_references()?.unwrap_or_default();

                for blob_file in linked_blob_files {
                    copy.entry(blob_file.blob_file_id)
                        .and_modify(|counter| {
                            counter.bytes += blob_file.bytes;
                            counter.len += blob_file.len;
                        })
                        .or_insert_with(|| {
                            FragmentationEntry::new(
                                blob_file.len,
                                blob_file.bytes,
                                blob_file.on_disk_bytes,
                            )
                        });
                }
            }

            Arc::new(copy)
        };

        let value_log = if dropped_tables.is_empty() {
            self.blob_files.clone()
        } else {
            let mut copy = self.blob_files.deref().clone();
            dropped_blob_files.extend(copy.prune_dead(&gc_stats));
            Arc::new(copy)
        };

        Ok(Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                blob_files: value_log,
                gc_stats,
            }),
        })
    }

    pub fn with_merge(
        &self,
        old_ids: &[TableId],
        new_tables: &[Table],
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
                    let mut run: Run<_> = run.deref().clone();
                    run.retain(|x| !old_ids.contains(&x.metadata.id));
                    run
                })
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>();

            if level_idx == dest_level {
                runs.insert(0, Run::new(new_tables.to_vec()));
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

            for &id in &blob_files_to_drop {
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

            for id in &blob_files_to_drop {
                copy.remove(id);
            }

            copy.prune(&value_log);

            Arc::new(copy)
        } else {
            self.gc_stats.clone()
        };

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                blob_files: value_log,
                gc_stats,
            }),
        }
    }

    pub fn with_moved(&self, ids: &[TableId], dest_level: usize) -> Self {
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
                runs.insert(0, Run::new(affected_tables.clone()));
            }

            let runs = optimize_runs(runs);

            levels.push(Level::from_runs(runs.into_iter().map(Arc::new).collect()));
        }

        Self {
            inner: Arc::new(VersionInner {
                id,
                levels,
                blob_files: self.blob_files.clone(),
                gc_stats: self.gc_stats.clone(),
            }),
        }
    }
}

impl Version {
    pub(crate) fn encode_into(&self, writer: &mut sfa::Writer) -> Result<(), crate::Error> {
        use byteorder::{LittleEndian, WriteBytesExt};

        writer.start("tables")?;

        // Level count
        #[expect(
            clippy::cast_possible_truncation,
            reason = "there are always less than 256 levels"
        )]
        writer.write_u8(self.level_count() as u8)?;

        for level in self.iter_levels() {
            // Run count
            #[expect(
                clippy::cast_possible_truncation,
                reason = "there are always less than 256 runs"
            )]
            writer.write_u8(level.len() as u8)?;

            for run in level.iter() {
                // Table count
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion tables in a run"
                )]
                writer.write_u32::<LittleEndian>(run.len() as u32)?;

                // Tables
                for table in run.iter() {
                    writer.write_u64::<LittleEndian>(table.id())?;
                    writer.write_u8(0)?; // Checksum type, 0 = XXH3
                    writer.write_u128::<LittleEndian>(table.checksum().into_u128())?;
                }
            }
        }

        writer.start("blob_files")?;

        // Blob file count
        #[expect(
            clippy::cast_possible_truncation,
            reason = "there are always less than 4 billion blob files"
        )]
        writer.write_u32::<LittleEndian>(self.blob_files.len() as u32)?;

        for file in self.blob_files.iter() {
            writer.write_u64::<LittleEndian>(file.id())?;
            writer.write_u8(0)?; // Checksum type, 0 = XXH3
            writer.write_u128::<LittleEndian>(file.0.checksum.into_u128())?;
        }

        writer.start("blob_gc_stats")?;

        self.gc_stats.encode_into(writer)?;

        Ok(())
    }
}
