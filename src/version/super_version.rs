// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    abstract_tree::NewFlushToTablesResult,
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

    // v---- TODO: move into Version
    pub(crate) seqno: SeqNo,
}

#[derive(Default)]
pub struct DeletedFiles {
    tables: Vec<Table>,
    blob_files: Vec<BlobFile>,
}

impl DeletedFiles {
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
    // TODO: v----- TODO: move this into Version
    pub fn register_compaction(
        &self,
        result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> crate::Result<(Self, DeletedFiles)> {
        let mut copy = self.clone();
        copy.seqno = version_seqno;

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
                );

                deleted.tables.extend(tables_to_delete);
                deleted.blob_files.extend(blob_files_to_drop);
            }
            CompactionOperation::Move { dst_lvl, tables } => {
                copy.version = copy
                    .version
                    .with_moved(&tables.iter().map(Table::id).collect::<Vec<_>>(), dst_lvl);
            }
            CompactionOperation::Drop { tables } => {
                let mut blob_files_to_drop = vec![];

                copy.version = copy.version.with_dropped(
                    &tables.iter().map(Table::id).collect::<Vec<_>>(),
                    // TODO: -> we need to drop blob files from disk; and tables too; need tests for that
                    &mut blob_files_to_drop,
                )?;

                deleted.blob_files.extend(blob_files_to_drop);
            }
        }

        Ok((copy, deleted))
    }

    pub(crate) fn register_flush(&self, flush_result: NewFlushToTablesResult) -> Self {
        let mut copy = self.clone();

        copy.version = copy.version.with_new_l0_run(
            &flush_result.tables,
            flush_result.blob_files.as_deref(),
            flush_result.frag_map.filter(|x| !x.is_empty()),
        );

        for table_id in flush_result.evicted_sealed_memtables {
            log::trace!("Releasing sealed memtable #{table_id}");
            copy.sealed_memtables = Arc::new(copy.sealed_memtables.remove(table_id));
        }

        copy
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
            "rotate: added memtable id={} to sealed memtables",
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
                seqno: 0,
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

    pub fn maintenance(&mut self, gc_watermark: SeqNo) {
        if gc_watermark == 0 {
            return;
        }

        if self.free_list_len() < 1 {
            return;
        }

        log::trace!("Running manifest GC with watermark={gc_watermark}");

        if let Some(hi_idx) = self.0.iter().rposition(|x| x.seqno < gc_watermark) {
            for _ in 0..=hi_idx {
                let Some(head) = self.0.front() else {
                    break;
                };

                log::trace!(
                    "Removing version #{} (seqno={})",
                    head.version.id(),
                    head.seqno,
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
            .find(|version| version.seqno < seqno)
            .cloned();

        if version.is_none() {
            log::error!("Failed to find a SuperVersion for snapshot with seqno={seqno}");
            log::error!("SuperVersions:");

            for version in self.0.iter().rev() {
                log::error!("-> {}, seqno={}", version.version.id(), version.seqno);
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

    /*
    #[test]
    fn super_version_gc_above_watermark() -> crate::Result<()> {
        let mut history = SuperVersionSet(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 0)?;

        assert_eq!(history.free_list_len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple() -> crate::Result<()> {
        let mut history = SuperVersionSet(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_simple_2() -> crate::Result<()> {
        let mut history = SuperVersionSet(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 1,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_keep() -> crate::Result<()> {
        let mut history = SuperVersionSet(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 8,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 2);

        Ok(())
    }

    #[test]
    fn super_version_gc_below_watermark_shadowed() -> crate::Result<()> {
        let mut history = SuperVersionSet(
            vec![
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 0,
                },
                SuperVersion {
                    active_memtable: Arc::new(Memtable::new(0)),
                    sealed_memtables: Arc::default(),
                    version: Version::new(0, crate::TreeType::Standard),
                    seqno: 2,
                },
            ]
            .into(),
        );

        history.maintenance(Path::new("."), 3)?;

        assert_eq!(history.len(), 1);

        Ok(())
    }
    */
}
