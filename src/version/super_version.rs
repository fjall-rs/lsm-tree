// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    abstract_tree::FlushToTablesResult,
    compaction::worker::{CompactionOperation, NewCompactionResult},
    memtable::Memtable,
    tree::sealed::SealedMemtables,
    version::Version,
    BlobFile, MemtableId, SeqNo, Table,
};
use std::{collections::VecDeque, sync::Arc};

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    #[doc(hidden)]
    pub active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version,
}

#[derive(Default)]
pub struct DeletedFiles {
    tables: Vec<Table>,
    blob_files: Vec<BlobFile>,
}

impl DeletedFiles {
    /// Collects all tables and blob files of a version, e.g. when the whole version is dropped.
    pub fn from_version(version: &Version) -> Self {
        Self {
            tables: version.iter_tables().cloned().collect(),
            blob_files: version.blob_files.iter().cloned().collect(),
        }
    }

    pub fn commit(self) {
        log::trace!("Marking {} tables as deleted", self.tables.len());
        log::trace!("Marking {} blob_files as deleted", self.blob_files.len());

        for table in self.tables {
            table.mark_as_deleted();
        }

        for blob_file in self.blob_files {
            blob_file.mark_as_deleted();
        }
    }
}

impl SuperVersion {
    pub fn register_compaction(
        &self,
        result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> (Self, DeletedFiles) {
        let mut copy = self.clone();

        let mut deleted = DeletedFiles::default();

        match result.inner {
            CompactionOperation::Noop => {}
            CompactionOperation::Merge {
                created_tables,
                dst_lvl,
                tables_to_delete,
                blob_files_to_drop,
                blob_frag_map,
                created_blob_files,
            } => {
                copy.version = copy.version.with_merge(
                    &tables_to_delete.iter().map(Table::id).collect::<Vec<_>>(),
                    &created_tables,
                    dst_lvl,
                    if blob_frag_map.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map)
                    },
                    created_blob_files,
                    &blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<Vec<_>>(),
                    version_seqno,
                );

                deleted.tables.extend(tables_to_delete);
                deleted.blob_files.extend(blob_files_to_drop);
            }
            CompactionOperation::Move { dst_lvl, tables } => {
                copy.version = copy.version.with_moved(
                    &tables.iter().map(Table::id).collect::<Vec<_>>(),
                    dst_lvl,
                    version_seqno,
                );
            }
            CompactionOperation::Drop {
                tables,
                blob_frag_map,
                blob_files_to_drop,
            } => {
                copy.version = copy.version.with_dropped(
                    &tables.iter().map(Table::id).collect::<Vec<_>>(),
                    if blob_frag_map.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map)
                    },
                    &blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<Vec<_>>(),
                    version_seqno,
                );

                deleted.tables.extend(tables);
                deleted.blob_files.extend(blob_files_to_drop);
            }
        }

        (copy, deleted)
    }

    /// Returns `true` if all memtables the flush processed are still sealed in this version.
    ///
    /// They are gone if the memtables were dropped while the flush was running,
    /// e.g. by a clear; the flush result must then not be registered, otherwise
    /// the dropped data comes back.
    ///
    /// Fixes: <https://github.com/fjall-rs/fjall/issues/287#issuecomment-4938188362>
    pub(crate) fn can_register_flush(&self, flush_result: &FlushToTablesResult) -> bool {
        flush_result
            .evicted_sealed_memtables
            .iter()
            .all(|id| self.sealed_memtables.contains(id))
    }

    pub(crate) fn register_flush(
        &self,
        flush_result: FlushToTablesResult,
        version_seqno: SeqNo,
    ) -> Option<Self> {
        if !self.can_register_flush(&flush_result) {
            log::debug!("Not registering tables because flush task processed some sealed memtables which do not exist (anymore)");
            flush_result.discard();
            return None;
        }

        let mut copy = self.clone();

        copy.version = copy.version.with_new_l0_run(
            &flush_result.tables,
            flush_result.blob_files.as_deref(),
            flush_result.frag_map.filter(|x| !x.is_empty()),
            flush_result.flush_watermark,
            version_seqno,
        );

        for table_id in flush_result.evicted_sealed_memtables {
            log::trace!("Releasing sealed memtable #{table_id}");
            copy.sealed_memtables = Arc::new(copy.sealed_memtables.remove(table_id));
        }

        Some(copy)
    }

    pub fn rotate_active_memtable(&self, memtable_id: MemtableId) -> Option<Self> {
        if self.active_memtable.is_empty() {
            return None;
        }

        let yanked_memtable = &self.active_memtable;

        // Rotate does not modify the memtable so it cannot break snapshots
        // So we just use the same seqno
        let mut copy = self.clone();
        copy.active_memtable = Arc::new(Memtable::new(memtable_id));
        copy.sealed_memtables = Arc::new(self.sealed_memtables.add(yanked_memtable.clone()));

        log::trace!(
            "Added memtable id={} to sealed memtables",
            yanked_memtable.id,
        );

        Some(copy)
    }
}

pub struct SuperVersionSet(VecDeque<SuperVersion>);

impl SuperVersionSet {
    pub fn new(version: Version) -> Self {
        Self(
            vec![SuperVersion {
                active_memtable: Arc::new(Memtable::new(0)),
                sealed_memtables: Arc::default(),
                version,
            }]
            .into(),
        )
    }

    pub fn memtable_size_sum(&self) -> u64 {
        let mut set = crate::HashMap::default();

        for super_version in &self.0 {
            set.entry(super_version.active_memtable.id)
                .and_modify(|bytes| *bytes += super_version.active_memtable.size())
                .or_insert_with(|| super_version.active_memtable.size());

            for sealed in super_version.sealed_memtables.iter() {
                set.entry(sealed.id)
                    .and_modify(|bytes| *bytes += sealed.size())
                    .or_insert_with(|| sealed.size());
            }
        }

        set.into_values().sum()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn free_list_len(&self) -> usize {
        self.len().saturating_sub(1)
    }

    /// Removes super versions that no snapshot at or above `gc_watermark` can read anymore,
    /// releasing their memtables and versions (and with them, deleted table and blob files).
    ///
    /// A snapshot reads the newest version below its seqno, so the newest version below
    /// the watermark has to be kept: only the versions before it are removed.
    pub fn maintenance(&mut self, gc_watermark: SeqNo) {
        if gc_watermark == 0 {
            return;
        }

        if self.free_list_len() < 1 {
            return;
        }

        log::trace!("Running manifest GC with watermark={gc_watermark}");

        if let Some(hi_idx) = self
            .0
            .iter()
            .rposition(|x| x.version.seqno() < gc_watermark)
        {
            // IMPORTANT: Keep the version at `hi_idx`
            //
            // Otherwise:
            // 1. versions with seqnos 0, 1, 2, snapshot at 3 (= watermark)
            // 2. the snapshot reads version 2, which would be removed as well
            for _ in 0..hi_idx {
                let Some(head) = self.0.front() else {
                    break;
                };

                log::trace!(
                    "Removing version #{} (seqno={})",
                    head.version.id(),
                    head.version.seqno(),
                );

                self.0.pop_front();
            }
        }

        log::trace!("Manifest GC done, version length now {}", self.0.len());
    }

    pub fn append_version(&mut self, version: SuperVersion) {
        self.0.push_back(version);
    }

    pub fn replace_latest_version(&mut self, version: SuperVersion) {
        if self.0.pop_back().is_some() {
            self.0.push_back(version);
        }
    }

    pub fn latest_version(&self) -> SuperVersion {
        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        self.0
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        if seqno == 0 {
            #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
            return self
                .0
                .front()
                .cloned()
                .expect("should always find a SuperVersion");
        }

        let version = self
            .0
            .iter()
            .rev()
            .find(|version| version.version.seqno() < seqno)
            .cloned();

        if version.is_none() {
            log::error!("Failed to find a SuperVersion for snapshot with seqno={seqno}");
            log::error!("SuperVersions:");

            for version in self.0.iter().rev() {
                log::error!(
                    "-> {}, seqno={}",
                    version.version.id(),
                    version.version.seqno(),
                );
            }
        }

        #[expect(clippy::expect_used, reason = "SuperVersion is expected to exist")]
        version.expect("should always find a SuperVersion")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    fn history(seqnos: &[SeqNo]) -> SuperVersionSet {
        SuperVersionSet(
            seqnos
                .iter()
                .enumerate()
                .map(|(id, &seqno)| SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(id as crate::version::VersionId, seqno, None),
                })
                .collect(),
        )
    }

    fn seqnos(history: &SuperVersionSet) -> Vec<SeqNo> {
        history.0.iter().map(|x| x.version.seqno()).collect()
    }

    #[test]
    fn super_version_gc_zero_watermark() {
        let mut history = history(&[0, 1, 2]);
        history.maintenance(0);
        assert_eq!(vec![0, 1, 2], seqnos(&history));
    }

    #[test]
    fn super_version_gc_above_watermark() {
        // NOTE: No version is below the watermark, so nothing can be removed
        let mut history = history(&[1, 2, 3]);
        history.maintenance(1);
        assert_eq!(vec![1, 2, 3], seqnos(&history));
    }

    #[test]
    fn super_version_gc_below_watermark_simple() {
        let mut history = history(&[0, 1, 2]);
        history.maintenance(3);
        assert_eq!(vec![2], seqnos(&history));
    }

    #[test]
    fn super_version_gc_below_watermark_simple_2() {
        let mut history = history(&[0, 1, 2, 8]);
        history.maintenance(3);
        assert_eq!(vec![2, 8], seqnos(&history));
    }

    #[test]
    fn super_version_gc_below_watermark_keep() {
        let mut history = history(&[0, 8]);
        history.maintenance(3);
        assert_eq!(vec![0, 8], seqnos(&history));
    }

    #[test]
    fn super_version_gc_below_watermark_shadowed() {
        let mut history = history(&[0, 2]);
        history.maintenance(3);
        assert_eq!(vec![2], seqnos(&history));
    }

    #[test]
    fn super_version_gc_max_watermark_keeps_latest() {
        let mut history = history(&[0, 1, 2, 8]);
        history.maintenance(SeqNo::MAX);
        assert_eq!(vec![8], seqnos(&history));
    }

    /// Every snapshot at or above the watermark must still find the same version
    #[test]
    fn super_version_gc_keeps_versions_of_snapshots() {
        let unpruned = history(&[0, 1, 2, 8]);

        let mut pruned = history(&[0, 1, 2, 8]);
        pruned.maintenance(3);

        for snapshot_seqno in 3..=10 {
            assert_eq!(
                unpruned
                    .get_version_for_snapshot(snapshot_seqno)
                    .version
                    .seqno(),
                pruned
                    .get_version_for_snapshot(snapshot_seqno)
                    .version
                    .seqno(),
                "snapshot at {snapshot_seqno}",
            );
        }
    }
}
