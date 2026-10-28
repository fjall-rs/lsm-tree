// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::mem::MemoryVersionSetProvider;
use crate::{
    abstract_tree::NewFlushToTablesResult,
    compaction::worker::NewCompactionResult,
    version::{SuperVersion, SuperVersionSet, Version},
    version_set::mem::range_bounds_to_owned_bounds,
    AbstractTree, EditRecord, MemtableId, SeqNo, VersionEdit, VersionSetProvider,
};
use std::{ops::RangeBounds, sync::Arc};

/// Persists changes to the version set
pub trait Persister: Send + Sync {
    /// Persists a version edit.
    fn persist(&self, edit: &VersionEdit) -> crate::Result<()>;
}

/// A version-set provider that persists edits before applying them in memory
pub struct IoVersionSetProvider {
    inner: MemoryVersionSetProvider,
    persister: Arc<dyn Persister>,
}

impl IoVersionSetProvider {
    /// Creates a provider with an empty initial version.
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self {
            inner: MemoryVersionSetProvider::default(),
            persister,
        }
    }

    /// Creates a provider from an existing super-version set.
    pub fn with_version_set(vset: SuperVersionSet, persister: Arc<dyn Persister>) -> Self {
        Self {
            inner: MemoryVersionSetProvider::new(vset),
            persister,
        }
    }

    /// Creates a provider with an existing initial version.
    #[must_use]
    pub fn with_initial_version(version: Version, persister: Arc<dyn Persister>) -> Self {
        Self {
            inner: MemoryVersionSetProvider::with_initial_version(version),
            persister,
        }
    }

    /// Replaces the latest in-memory version.
    pub fn overwrite(&self, super_version: SuperVersion) {
        self.inner.overwrite(super_version);
    }

    /// Prunes versions that are no longer needed by snapshots.
    pub fn prune(&self) {
        self.inner.prune();
    }

    /// Rotates and flushes the active memtable.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing or persisting the resulting version edit
    /// fails.
    #[expect(clippy::expect_used)]
    pub fn flush_active_memtable(
        &self,
        tree: &impl AbstractTree,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<u64> {
        if self.rotate_active_memtable(tree).is_none() {
            return Ok(0);
        }

        log::trace!("Flushing write buffer");

        let flush_result = tree
            .new_flush(gc_watermark)?
            .expect("should flush to table files");

        self.register_flush(flush_result, version_seqno)
    }

    /// Runs and registers a compaction.
    ///
    /// # Errors
    ///
    /// Returns an error if compaction or persisting its version edit fails.
    pub fn compact(
        &self,
        tree: &impl AbstractTree,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        let compaction_result = tree.run_compaction(strategy, gc_watermark)?;
        self.register_compaction(compaction_result, version_seqno)
    }

    /// Runs and registers a compaction that drops a key range.
    ///
    /// # Errors
    ///
    /// Returns an error if compaction or persisting its version edit fails.
    pub fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        tree: &impl AbstractTree,
        range: R,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        let (bounds, is_empty) = range_bounds_to_owned_bounds(&range);

        if is_empty {
            return Ok(());
        }

        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(bounds));
        let compaction_result = tree.run_compaction(strategy, gc_watermark)?;
        self.register_compaction(compaction_result, version_seqno)
    }

    /// Clears the in-memory version history's latest visible version.
    pub fn clear(&self, tree: &impl AbstractTree, version_seqno: SeqNo) -> crate::Result<()> {
        self.inner.clear(tree, version_seqno)
    }

    /// Persists and then registers a compaction result.
    ///
    /// # Errors
    ///
    /// Returns an error if persistence or registering the compaction fails.
    pub fn register_compaction(
        &self,
        mut result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> crate::Result<()> {
        let mut edits = std::mem::take(&mut result.version_edit);
        edits.push(EditRecord::SetVersionSeqno(version_seqno));

        self.persister.persist(&edits)?;
        self.inner.register_compaction(result, version_seqno)
    }

    /// Persists and then registers a flush result.
    ///
    /// # Errors
    ///
    /// Returns an error if persistence fails.
    pub fn register_flush(
        &self,
        mut result: NewFlushToTablesResult,
        version_seqno: SeqNo,
    ) -> crate::Result<u64> {
        let mut edits = std::mem::take(&mut result.version_edit);
        edits.push(EditRecord::SetVersionSeqno(version_seqno));

        self.persister.persist(&edits)?;
        Ok(self.inner.register_flush(result, version_seqno))
    }

    /// Rotates the active memtable.
    pub fn rotate_active_memtable(&self, tree: &impl AbstractTree) -> Option<MemtableId> {
        self.inner.rotate_active_memtable(tree)
    }
}

impl VersionSetProvider for IoVersionSetProvider {
    fn version_history_len(&self) -> usize {
        self.inner.version_history_len()
    }

    fn latest_version(&self) -> SuperVersion {
        self.inner.latest_version()
    }

    fn get_version_for_seqno(&self, seqno: SeqNo) -> SuperVersion {
        self.inner.get_version_for_seqno(seqno)
    }
}

#[cfg(test)]
#[expect(clippy::significant_drop_tightening, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        blob_tree::FragmentationEntry, Config, EditRecord, KvSeparationOptions,
        SequenceNumberCounter,
    };
    use std::{
        fs::{File, OpenOptions},
        io::{BufWriter, Write},
        path::Path,
        sync::Mutex,
    };
    use test_log::test;

    /// Writes version edits as JSONL for testing
    struct JsonPersister(Mutex<BufWriter<File>>);

    impl JsonPersister {
        fn create(path: &Path) -> crate::Result<Self> {
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            Ok(Self(Mutex::new(BufWriter::new(file))))
        }
    }

    impl Persister for JsonPersister {
        fn persist(&self, edit: &VersionEdit) -> crate::Result<()> {
            log::trace!("Persisting {edit:#?}");
            let mut writer = self.0.lock().unwrap();
            writeln!(writer, "{}", serde_json::to_string(&edit).unwrap())?;
            writer.flush()?;
            writer.get_mut().sync_all()?;
            Ok(())
        }
    }

    #[test]
    fn io_provider_flush() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let persister = Arc::new(JsonPersister::create(&manifest_path)?);
        let provider = Arc::new(IoVersionSetProvider::new(persister));

        let tree = Config::new(dir.path(), provider.clone()).open()?;

        tree.insert("key", "value", 1);
        provider.flush_active_memtable(&tree, 2, 0)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            &[VersionEdit::new(vec![
                EditRecord::AddTables {
                    level: 0,
                    tables: vec![0],
                },
                EditRecord::SetVersionSeqno(2),
            ])],
            &*edits,
        );

        Ok(())
    }

    #[test]
    fn io_provider_flush_blob() -> crate::Result<()> {
        let dir: tempfile::TempDir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let persister = Arc::new(JsonPersister::create(&manifest_path)?);
        let provider = Arc::new(IoVersionSetProvider::new(persister));

        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        tree.insert("key", "value", 1);
        provider.flush_active_memtable(&tree, 2, 0)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            &[VersionEdit::new(vec![
                EditRecord::AddTables {
                    level: 0,
                    tables: vec![0],
                },
                EditRecord::AddBlobFiles {
                    blob_files: vec![0]
                },
                EditRecord::SetVersionSeqno(2),
            ])],
            &*edits,
        );

        Ok(())
    }

    #[test]
    fn io_provider_compaction_drops_stale_blob_files() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let persister = Arc::new(JsonPersister::create(&manifest_path)?);
        let provider = Arc::new(IoVersionSetProvider::new(persister));

        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        tree.insert("drop", "value", 1);
        provider.flush_active_memtable(&tree, 2, 0)?;

        tree.insert("keep", "value", 3);
        provider.flush_active_memtable(&tree, 4, 0)?;

        tree.remove("drop", 5);
        provider.flush_active_memtable(&tree, 6, 0)?;

        let major = Arc::new(crate::compaction::Major::default());
        provider.compact(&tree, major.clone(), 7, 1_000)?;
        provider.compact(&tree, major, 8, 1_000)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(5, edits.len());

        let [flush_drop, flush_keep, flush_delete, first_major_edit, second_major_edit] =
            edits.as_slice()
        else {
            unreachable!("the edit count was asserted above");
        };

        assert_eq!(
            &VersionEdit::new(vec![
                EditRecord::AddTables {
                    level: 0,
                    tables: vec![0],
                },
                EditRecord::AddBlobFiles {
                    blob_files: vec![0],
                },
                EditRecord::SetVersionSeqno(2),
            ]),
            flush_drop,
        );
        assert_eq!(
            &VersionEdit::new(vec![
                EditRecord::AddTables {
                    level: 0,
                    tables: vec![1],
                },
                EditRecord::AddBlobFiles {
                    blob_files: vec![1],
                },
                EditRecord::SetVersionSeqno(4),
            ]),
            flush_keep,
        );
        assert_eq!(
            &VersionEdit::new(vec![
                EditRecord::AddTables {
                    level: 0,
                    tables: vec![2],
                },
                EditRecord::SetVersionSeqno(6),
            ]),
            flush_delete,
        );

        assert_eq!(
            Some(&EditRecord::AddTables {
                level: 6,
                tables: vec![3],
            }),
            first_major_edit.iter().next(),
        );
        assert!(matches!(
            first_major_edit.iter().last(),
            Some(EditRecord::SetVersionSeqno(7)),
        ));
        let Some(mut dropped_tables) = first_major_edit.iter().find_map(|record| match record {
            EditRecord::DropTables(tables) => Some(tables.clone()),
            _ => None,
        }) else {
            unreachable!("first major compaction should drop its input tables");
        };
        dropped_tables.sort_unstable();
        assert_eq!(vec![0, 1, 2], dropped_tables);

        assert_eq!(
            &VersionEdit::new(vec![
                EditRecord::AddTables {
                    level: 6,
                    tables: vec![4],
                },
                EditRecord::DropTables(vec![3]),
                EditRecord::DropBlobFiles(vec![0]),
                EditRecord::SetVersionSeqno(8),
            ]),
            second_major_edit,
        );

        Ok(())
    }

    #[test]
    fn io_provider_compaction_persists_fragmentation_map() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let persister = Arc::new(JsonPersister::create(&manifest_path)?);
        let provider = Arc::new(IoVersionSetProvider::new(persister));

        let value = "value";
        let size = value.len() as u64;
        let seqno = SequenceNumberCounter::default();
        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(
                KvSeparationOptions::default()
                    .compression(crate::CompressionType::None)
                    .separation_threshold(2),
            ))
            .open()?;

        // Keep one non-blob value so the final compaction still has a table to compact.
        tree.insert("1", value, seqno.next());
        tree.insert("keep", "x", seqno.next());
        let flush_1_seqno = seqno.next();
        provider.flush_active_memtable(&tree, flush_1_seqno, 0)?;

        tree.insert("2", value, seqno.next());
        let flush_2_seqno = seqno.next();
        provider.flush_active_memtable(&tree, flush_2_seqno, 0)?;

        tree.insert("3", value, seqno.next());
        let flush_3_seqno = seqno.next();
        provider.flush_active_memtable(&tree, flush_3_seqno, 0)?;

        let major = Arc::new(crate::compaction::Major::default());

        tree.remove("1", seqno.next());
        let delete_1_flush_seqno = seqno.next();
        provider.flush_active_memtable(&tree, delete_1_flush_seqno, 0)?;
        let delete_1_compaction_seqno = seqno.next();
        provider.compact(&tree, major.clone(), delete_1_compaction_seqno, 1_000)?;

        tree.remove("2", seqno.next());
        let delete_2_flush_seqno = seqno.next();
        provider.flush_active_memtable(&tree, delete_2_flush_seqno, 0)?;
        let delete_2_compaction_seqno = seqno.next();
        provider.compact(&tree, major.clone(), delete_2_compaction_seqno, 1_000)?;

        tree.remove("3", seqno.next());
        let delete_3_flush_seqno = seqno.next();
        provider.flush_active_memtable(&tree, delete_3_flush_seqno, 0)?;
        let delete_3_compaction_seqno = seqno.next();
        provider.compact(&tree, major.clone(), delete_3_compaction_seqno, 1_000)?;

        let final_compaction_seqno = seqno.next();
        provider.compact(&tree, major, final_compaction_seqno, 1_000)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                // Insert 1
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![0],
                    },
                    EditRecord::AddBlobFiles {
                        blob_files: vec![0],
                    },
                    EditRecord::SetVersionSeqno(flush_1_seqno),
                ]),
                // Insert 2
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![1],
                    },
                    EditRecord::AddBlobFiles {
                        blob_files: vec![1],
                    },
                    EditRecord::SetVersionSeqno(flush_2_seqno),
                ]),
                // Insert 3
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![2],
                    },
                    EditRecord::AddBlobFiles {
                        blob_files: vec![2],
                    },
                    EditRecord::SetVersionSeqno(flush_3_seqno),
                ]),
                // Remove 1
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![3],
                    },
                    EditRecord::SetVersionSeqno(delete_1_flush_seqno),
                ]),
                // Major compaction
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 6,
                        tables: vec![4],
                    },
                    EditRecord::DropTables(vec![0, 3, 1, 2]),
                    EditRecord::UpdateFragMap({
                        let mut map = crate::blob_tree::FragmentationMap::default();
                        map.insert(0, FragmentationEntry::new(1, size, size));
                        map
                    }),
                    EditRecord::SetVersionSeqno(delete_1_compaction_seqno),
                ]),
                // Remove 2
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![5],
                    },
                    EditRecord::SetVersionSeqno(delete_2_flush_seqno),
                ]),
                // Major compaction
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 6,
                        tables: vec![6],
                    },
                    EditRecord::DropTables(vec![5, 4]),
                    EditRecord::UpdateFragMap({
                        let mut map = crate::blob_tree::FragmentationMap::default();
                        map.insert(1, FragmentationEntry::new(1, size, size));
                        map
                    }),
                    EditRecord::DropBlobFiles(vec![0]),
                    EditRecord::SetVersionSeqno(delete_2_compaction_seqno),
                ]),
                // Remove 3
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![7],
                    },
                    EditRecord::SetVersionSeqno(delete_3_flush_seqno),
                ]),
                // Major compaction
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 6,
                        tables: vec![8],
                    },
                    EditRecord::DropTables(vec![7, 6]),
                    EditRecord::UpdateFragMap({
                        let mut map = crate::blob_tree::FragmentationMap::default();
                        map.insert(2, FragmentationEntry::new(1, size, size));
                        map
                    }),
                    EditRecord::DropBlobFiles(vec![1]),
                    EditRecord::SetVersionSeqno(delete_3_compaction_seqno),
                ]),
                // Final major compaction
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 6,
                        tables: vec![9],
                    },
                    EditRecord::DropTables(vec![8]),
                    EditRecord::DropBlobFiles(vec![2]),
                    EditRecord::SetVersionSeqno(final_compaction_seqno),
                ]),
            ],
            edits,
        );

        assert_eq!(0, tree.current_version().blob_file_count());
        {
            let gc_stats = tree.current_version().gc_stats().clone();
            assert_eq!(&crate::HashMap::default(), &*gc_stats);
        }

        Ok(())
    }

    #[test]
    fn io_provider_flush_and_trivial_move() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let persister = Arc::new(JsonPersister::create(&manifest_path)?);
        let provider = Arc::new(IoVersionSetProvider::new(persister));

        let tree = Config::new(dir.path(), provider.clone()).open()?;

        tree.insert("key", "value", 1);
        provider.flush_active_memtable(&tree, 2, 0)?;
        provider.compact(&tree, Arc::new(crate::compaction::Leveled::default()), 3, 0)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![0],
                    },
                    EditRecord::SetVersionSeqno(2),
                ]),
                VersionEdit::new(vec![
                    EditRecord::MoveTables {
                        dst_lvl: 6,
                        tables: vec![0],
                    },
                    EditRecord::SetVersionSeqno(3),
                ]),
            ],
            edits,
        );

        Ok(())
    }

    #[test]
    fn io_provider_flush_and_major_compaction() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let persister = Arc::new(JsonPersister::create(&manifest_path)?);
        let provider = Arc::new(IoVersionSetProvider::new(persister));

        let tree = Config::new(dir.path(), provider.clone()).open()?;

        tree.insert("a", "value", 0);
        provider.flush_active_memtable(&tree, 1, 0)?;

        tree.insert("b", "value", 2);
        provider.flush_active_memtable(&tree, 3, 0)?;

        provider.compact(&tree, Arc::new(crate::compaction::Major::default()), 4, 0)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![0],
                    },
                    EditRecord::SetVersionSeqno(1),
                ]),
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 0,
                        tables: vec![1],
                    },
                    EditRecord::SetVersionSeqno(3),
                ]),
                VersionEdit::new(vec![
                    EditRecord::AddTables {
                        level: 6,
                        tables: vec![2],
                    },
                    EditRecord::DropTables(vec![0, 1]),
                    EditRecord::SetVersionSeqno(4),
                ]),
            ],
            edits,
        );

        Ok(())
    }
}
