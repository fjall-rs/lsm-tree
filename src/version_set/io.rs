// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::mem::MemorySuperVersionSetProvider;
use crate::{
    abstract_tree::FlushToTablesResult,
    compaction::worker::NewCompactionResult,
    version::{SuperVersion, SuperVersionSet, Version},
    version_set::mem::range_bounds_to_owned_bounds,
    AbstractTree, EditRecord, MemtableId, SeqNo, SuperVersionSetProvider, VersionEdit,
};
use std::{ops::RangeBounds, sync::Arc};

/// Persists changes to the super version set
///
/// The persister is passed into every call that persists an edit (instead of being
/// stored in the provider), so the caller decides which lock guards it.
pub trait Persister {
    /// Error type of the persister, lsm-tree errors are converted into it
    type Error: From<crate::Error>;

    /// Persists a version edit.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    fn persist(&mut self, edit: &VersionEdit) -> Result<(), Self::Error>;
}

/// A [`SuperVersionSetProvider`] that persists edits before applying them in memory
#[derive(Default)]
pub struct IoSuperVersionSetProvider {
    inner: MemorySuperVersionSetProvider,
}

impl IoSuperVersionSetProvider {
    /// Creates a provider with an empty initial [`SuperVersion`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a provider from an existing [`SuperVersionSetProvider`].
    #[must_use]
    pub fn with_version_set(vset: SuperVersionSet) -> Self {
        Self {
            inner: MemorySuperVersionSetProvider::new(vset),
        }
    }

    /// Creates a provider with an existing initial [`SuperVersion`].
    #[must_use]
    pub fn with_initial_version(version: Version) -> Self {
        Self {
            inner: MemorySuperVersionSetProvider::with_initial_version(version),
        }
    }

    /// Replaces the latest in-memory version.
    pub fn overwrite(&self, super_version: SuperVersion) {
        self.inner.overwrite(super_version);
    }

    /// Removes [`SuperVersion`] (and their memtables) that no snapshot at or above `gc_watermark`
    /// can read anymore, see [`crate::version::SuperVersionSet::maintenance`].
    pub fn prune(&self, gc_watermark: SeqNo) {
        self.inner.prune(gc_watermark);
    }

    /// Rotates and flushes the active memtable.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    #[expect(clippy::expect_used, clippy::missing_panics_doc)]
    pub fn flush_active_memtable<P: Persister + ?Sized>(
        &self,
        persister: &mut P,
        tree: &impl AbstractTree,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> Result<u64, P::Error> {
        if self.rotate_active_memtable(tree).is_none() {
            return Ok(0);
        }

        log::trace!("Flushing write buffer");

        let flush_result = tree
            .flush(gc_watermark)?
            .expect("should flush to table files");

        self.register_flush(persister, flush_result, version_seqno)
    }

    /// Runs and registers a compaction.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn compact<P: Persister + ?Sized>(
        &self,
        persister: &mut P,
        tree: &impl AbstractTree,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> Result<(), P::Error> {
        let compaction_result = tree.run_compaction(strategy, gc_watermark)?;
        self.register_compaction(persister, compaction_result, version_seqno)
    }

    /// Runs and registers a compaction that drops a key range.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn drop_range<P: Persister + ?Sized, K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        persister: &mut P,
        tree: &impl AbstractTree,
        range: R,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> Result<(), P::Error> {
        let (bounds, is_empty) = range_bounds_to_owned_bounds(&range);

        if is_empty {
            return Ok(());
        }

        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(bounds));
        let compaction_result = tree.run_compaction(strategy, gc_watermark)?;

        self.register_compaction(persister, compaction_result, version_seqno)
    }

    /// Drops all tables, blob files and memtables by appending a new, empty [`SuperVersion`].
    ///
    /// Returns the size of the dropped (active and sealed) memtables in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn clear<P: Persister + ?Sized>(
        &self,
        persister: &mut P,
        tree: &impl AbstractTree,
        version_seqno: SeqNo,
    ) -> Result<u64, P::Error> {
        let current = self.inner.latest_version().version;

        let table_ids: Vec<_> = current.iter_tables().map(crate::Table::id).collect();
        let blob_file_ids: Vec<_> = current.blob_files.list_ids().copied().collect();

        let mut edits = Vec::new();

        if !table_ids.is_empty() {
            edits.push(EditRecord::DropTables(table_ids));
        }
        if !blob_file_ids.is_empty() {
            edits.push(EditRecord::DropBlobFiles(blob_file_ids));
        }

        // NOTE: Everything up to the clear is gone, so it counts as flushed
        edits.push(EditRecord::SetFlushWatermark(version_seqno));
        edits.push(EditRecord::SetVersionSeqno(version_seqno));
        persister.persist(&VersionEdit::new(edits))?;

        Ok(self.inner.clear(tree, version_seqno)?)
    }

    /// Persists and then registers a compaction result.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn register_compaction<P: Persister + ?Sized>(
        &self,
        persister: &mut P,
        mut result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> Result<(), P::Error> {
        if matches!(
            result.inner,
            crate::compaction::worker::CompactionOperation::Noop,
        ) {
            return Ok(());
        }

        let mut edits = std::mem::take(&mut result.version_edit);
        edits.push(EditRecord::SetVersionSeqno(version_seqno));
        persister.persist(&edits)?;

        Ok(self.inner.register_compaction(result, version_seqno)?)
    }

    /// Persists and then registers a flush result.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn register_flush<P: Persister + ?Sized>(
        &self,
        persister: &mut P,
        mut result: FlushToTablesResult,
        version_seqno: SeqNo,
    ) -> Result<u64, P::Error> {
        // IMPORTANT: Check before persisting, otherwise the stale edit is replayed on recovery,
        // and the dropped data comes back
        //
        // NOTE: Sealed memtables are only removed by calls that persist an edit, which the
        // caller serializes (it holds the persister mutably), so the check stays valid until
        // the install (a concurrent rotation only adds sealed memtables)
        if !self.inner.latest_version().can_register_flush(&result) {
            log::debug!(
                "Not registering flush, because some of its sealed memtables do not exist anymore (e.g. cleared)",
            );
            result.discard();
            return Ok(0);
        }

        let mut edits = std::mem::take(&mut result.version_edit);

        if let Some(flush_watermark) = result.flush_watermark {
            edits.push(EditRecord::SetFlushWatermark(flush_watermark));
        }

        edits.push(EditRecord::SetVersionSeqno(version_seqno));

        persister.persist(&edits)?;

        Ok(self.inner.register_flush(result, version_seqno))
    }

    /// Rotates the active memtable.
    pub fn rotate_active_memtable(&self, tree: &impl AbstractTree) -> Option<MemtableId> {
        self.inner.rotate_active_memtable(tree)
    }

    /// Returns edit records that rebuild the latest [`Version`]
    /// when applied to an empty [`crate::RawVersionData`].
    ///
    /// See [`crate::version::Version::get_edit_records`].
    #[must_use]
    pub fn get_edit_records(&self) -> Vec<EditRecord> {
        self.inner.latest_version().version.get_edit_records()
    }

    /// Hydrates the current [`Version`] from a recovered state.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs.
    pub fn hydrate(
        &self,
        tree: &crate::AnyTree,
        raw_state: &crate::RawVersionData,
    ) -> crate::Result<()> {
        self.inner.hydrate(tree, raw_state)
    }
}

impl SuperVersionSetProvider for IoSuperVersionSetProvider {
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
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        blob_tree::{FragmentationEntry, FragmentationMap},
        BlobFileRef, Checksum, Config, EditRecord, KvSeparationOptions, RawVersionData,
        SequenceNumberCounter, TableRef,
    };
    use std::{
        fs::{File, OpenOptions},
        io::{BufWriter, Write},
        path::Path,
    };
    use test_log::test;

    /// Writes version edits as JSONL for testing
    struct JsonPersister(BufWriter<File>);

    impl JsonPersister {
        fn create(path: &Path) -> crate::Result<Self> {
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            Ok(Self(BufWriter::new(file)))
        }
    }

    impl Persister for JsonPersister {
        type Error = crate::Error;

        fn persist(&mut self, edit: &VersionEdit) -> crate::Result<()> {
            log::trace!("Persisting {edit:#?}");
            writeln!(self.0, "{}", serde_json::to_string(&edit).unwrap())?;
            self.0.flush()?;
            self.0.get_mut().sync_all()?;
            Ok(())
        }
    }

    /// Test wrapper that compares version edits by logical contents.
    ///
    /// Table and blob-file checksums are intentionally ignored by its equality
    /// implementation because the persistence tests generate files at runtime.
    #[derive(Clone, Debug)]
    struct NormalizedRecord(EditRecord);

    impl NormalizedRecord {
        /// Creates an add-tables record with placeholder checksums.
        fn add_tables(level: usize, tables: Vec<u64>) -> Self {
            Self(EditRecord::AddTables {
                level,
                tables: tables
                    .into_iter()
                    .map(|id| TableRef::new(id, Checksum::from_raw(0)))
                    .collect(),
            })
        }

        /// Creates an add-blob-files record with placeholder checksums.
        fn add_blob_files(blob_files: Vec<u64>) -> Self {
            Self(EditRecord::AddBlobFiles {
                blob_files: blob_files
                    .into_iter()
                    .map(|id| BlobFileRef::new(id, Checksum::from_raw(0)))
                    .collect(),
            })
        }

        /// Creates a drop-tables record.
        fn drop_tables(tables: Vec<u64>) -> Self {
            Self(EditRecord::DropTables(tables))
        }

        /// Creates a move-tables record.
        fn move_tables(dst_lvl: usize, tables: Vec<u64>) -> Self {
            Self(EditRecord::MoveTables { dst_lvl, tables })
        }

        /// Creates a drop-blob-files record.
        fn drop_blob_files(blob_files: Vec<u64>) -> Self {
            Self(EditRecord::DropBlobFiles(blob_files))
        }

        /// Creates a sequence-number update record.
        fn set_version_seqno(seqno: u64) -> Self {
            Self(EditRecord::SetVersionSeqno(seqno))
        }

        /// Creates a flush watermark record.
        fn set_flush_watermark(seqno: u64) -> Self {
            Self(EditRecord::SetFlushWatermark(seqno))
        }

        /// Creates a fragmentation-map update record.
        fn update_frag_map(map: FragmentationMap) -> Self {
            Self(EditRecord::UpdateFragMap(map))
        }
    }

    fn unordered_ids_equal(lhs: &[u64], rhs: &[u64]) -> bool {
        if lhs.len() != rhs.len() {
            return false;
        }

        let mut lhs = lhs.to_vec();
        let mut rhs = rhs.to_vec();
        lhs.sort_unstable();
        rhs.sort_unstable();
        lhs == rhs
    }

    impl PartialEq for NormalizedRecord {
        fn eq(&self, other: &Self) -> bool {
            match (&self.0, &other.0) {
                (
                    EditRecord::AddTables {
                        level: lhs_level,
                        tables: lhs_tables,
                    },
                    EditRecord::AddTables {
                        level: rhs_level,
                        tables: rhs_tables,
                    },
                ) => {
                    lhs_level == rhs_level
                        && lhs_tables
                            .iter()
                            .map(|table| (table.id(), table.global_seqno()))
                            .eq(rhs_tables
                                .iter()
                                .map(|table| (table.id(), table.global_seqno())))
                }
                (
                    EditRecord::AddBlobFiles {
                        blob_files: lhs_blob_files,
                    },
                    EditRecord::AddBlobFiles {
                        blob_files: rhs_blob_files,
                    },
                ) => lhs_blob_files
                    .iter()
                    .map(BlobFileRef::id)
                    .eq(rhs_blob_files.iter().map(BlobFileRef::id)),
                (EditRecord::DropTables(lhs), EditRecord::DropTables(rhs))
                | (EditRecord::DropBlobFiles(lhs), EditRecord::DropBlobFiles(rhs)) => {
                    unordered_ids_equal(lhs, rhs)
                }
                _ => self.0 == other.0,
            }
        }
    }

    impl Eq for NormalizedRecord {}

    fn normalize_record(record: &EditRecord) -> NormalizedRecord {
        NormalizedRecord(record.clone())
    }

    fn normalize_edits(edits: &[VersionEdit]) -> Vec<Vec<NormalizedRecord>> {
        edits
            .iter()
            .map(|edit| edit.iter().map(normalize_record).collect())
            .collect()
    }

    #[test]
    fn io_provider_flush_e2e_recovery() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");

        {
            let mut persister = JsonPersister::create(&manifest_path)?;
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone()).open()?;

            tree.insert("key", "value", 1);
            provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;

            assert_eq!(1, tree.table_count());
            assert_eq!(&*tree.get("key", 3)?.unwrap(), b"value");
        }

        {
            let edits = std::fs::read_to_string(&manifest_path)?
                .lines()
                .filter(|l| !l.is_empty())
                .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
                .collect::<Vec<_>>();

            let mut raw_state = RawVersionData::default();
            for edit in edits {
                raw_state.apply(&edit);
            }

            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone()).open()?;

            provider.hydrate(&tree, &raw_state)?;

            assert_eq!(1, tree.table_count());
            assert_eq!(&*tree.get("key", 3)?.unwrap(), b"value");
            assert_eq!(2, tree.current_version().seqno());
            assert_eq!(1, tree.next_table_id());
        }

        Ok(())
    }

    #[test]
    fn io_provider_overlapping_l0_runs_e2e_recovery() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let manifest_path = dir.path().join("version-edits.jsonl");

        let expected = {
            let mut persister = JsonPersister::create(&manifest_path)?;
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone()).open()?;

            tree.insert("a", "old_value", 1);
            provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;
            tree.insert("a", "new_value", 3);
            provider.flush_active_memtable(&mut persister, &tree, 4, 0)?;

            let version = tree.current_version();
            assert_eq!(2, version.l0().run_count());
            version
        };

        let edits = std::fs::read_to_string(&manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        let mut raw_state = RawVersionData::default();
        for edit in edits {
            raw_state.apply(&edit);
        }

        let provider = Arc::new(IoSuperVersionSetProvider::new());
        let tree = Config::new(dir.path(), provider.clone()).open()?;
        provider.hydrate(&tree, &raw_state)?;

        // NOTE: The newer run must stay on top, otherwise point reads find the old value first
        assert_eq!(run_layout(&expected), run_layout(&tree.current_version()));
        assert_eq!(&*tree.get("a", 5)?.unwrap(), b"new_value");

        Ok(())
    }

    /// Resolves table and blob file IDs to `custom_<id>` file names
    struct CustomNameResolver(std::path::PathBuf);

    impl crate::FileResolver for CustomNameResolver {
        fn resolve_table(&self, id: crate::TableId) -> std::path::PathBuf {
            self.0.join("tables").join(format!("custom_{id}"))
        }

        fn resolve_blob_file(&self, id: u64) -> std::path::PathBuf {
            self.0.join("blobs").join(format!("custom_{id}"))
        }
    }

    #[test]
    fn io_provider_file_resolver_recovery() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let manifest_path = dir.path().join("version-edits.jsonl");

        let kv_opts = KvSeparationOptions::default().separation_threshold(1);

        {
            let mut persister = JsonPersister::create(&manifest_path)?;
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone())
                .with_kv_separation(Some(kv_opts.clone()))
                .open()?;

            tree.insert("key", "value", 1);
            provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;

            assert_eq!(1, tree.table_count());
            assert_eq!(1, tree.blob_file_count());
        }

        // Move the files somewhere only the resolver knows about
        for folder in ["tables", "blobs"] {
            let folder = dir.path().join(folder);
            std::fs::rename(folder.join("0"), folder.join("custom_0"))?;
        }

        let edits = std::fs::read_to_string(&manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        let mut raw_state = RawVersionData::default();
        for edit in edits {
            raw_state.apply(&edit);
        }

        // Without a resolver, the files are not found
        {
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone())
                .with_kv_separation(Some(kv_opts.clone()))
                .open()?;

            assert!(matches!(
                provider.hydrate(&tree, &raw_state),
                Err(crate::Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound,
            ));
        }

        let provider = Arc::new(IoSuperVersionSetProvider::new());
        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(kv_opts))
            .with_file_resolver(Arc::new(CustomNameResolver(dir.path().to_path_buf())))
            .open()?;

        provider.hydrate(&tree, &raw_state)?;

        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        // NOTE: Reads the value from the resolved blob file
        assert_eq!(&*tree.get("key", 3)?.unwrap(), b"value");

        Ok(())
    }

    #[test]
    fn io_provider_flush_blob_e2e_recovery() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let manifest_path = dir.path().join("version-edits.jsonl");
        {
            let mut persister = JsonPersister::create(&manifest_path)?;
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone())
                .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
                .open()?;

            tree.insert("key", "value", 1);
            provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;
        }

        {
            let edits = std::fs::read_to_string(&manifest_path)?
                .lines()
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
                .collect::<Vec<_>>();

            let mut raw_state = RawVersionData::default();
            for edit in edits {
                raw_state.apply(&edit);
            }

            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone())
                .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
                .open()?;

            provider.hydrate(&tree, &raw_state)?;

            assert_eq!(1, tree.table_count());
            assert_eq!(&*tree.get("key", 3)?.unwrap(), b"value");
            assert_eq!(1, tree.next_table_id());
            assert_eq!(1, tree.inner().blob_file_id_counter.get());
            assert_eq!(2, tree.current_version().seqno());
        }

        Ok(())
    }

    /// Per level, the table IDs of each run, in order
    fn run_layout(version: &Version) -> Vec<Vec<Vec<u64>>> {
        version
            .iter_levels()
            .map(|lvl| {
                lvl.iter()
                    .map(|run| run.iter().map(crate::Table::id).collect())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn io_provider_get_edit_records_roundtrip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let kv_opts = KvSeparationOptions::default()
            .compression(crate::CompressionType::None)
            .separation_threshold(2);

        let seqno = SequenceNumberCounter::default();

        let (records, expected) = {
            let mut persister = JsonPersister::create(&dir.path().join("a.jsonl"))?;
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone())
                .with_kv_separation(Some(kv_opts.clone()))
                .open()?;

            // Major compaction after a delete creates a table in the last level
            // and fragmentation stats for the blob file
            tree.insert("1", "value", seqno.next());
            tree.insert("keep", "x", seqno.next());
            provider.flush_active_memtable(&mut persister, &tree, seqno.next(), 0)?;
            tree.remove("1", seqno.next());
            provider.flush_active_memtable(&mut persister, &tree, seqno.next(), 0)?;
            provider.compact(
                &mut persister,
                &tree,
                Arc::new(crate::compaction::Major::default()),
                seqno.next(),
                1_000,
            )?;

            // Two overlapping L0 runs, the newer one must stay on top
            tree.insert("a", "old_value", seqno.next());
            provider.flush_active_memtable(&mut persister, &tree, seqno.next(), 0)?;
            tree.insert("a", "new_value", seqno.next());
            provider.flush_active_memtable(&mut persister, &tree, seqno.next(), 0)?;

            let version = tree.current_version();
            assert_eq!(2, version.l0().run_count());
            assert!(!version.gc_stats().is_empty());
            assert!(version.blob_file_count() > 0);

            (provider.get_edit_records(), version)
        };

        let mut raw_state = RawVersionData::default();
        raw_state.apply(&VersionEdit::new(records));

        let provider = Arc::new(IoSuperVersionSetProvider::new());
        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(kv_opts))
            .open()?;
        provider.hydrate(&tree, &raw_state)?;

        let recovered = tree.current_version();

        assert_eq!(run_layout(&expected), run_layout(&recovered));
        assert_eq!(
            expected
                .blob_files
                .list_ids()
                .collect::<crate::HashSet<_>>(),
            recovered
                .blob_files
                .list_ids()
                .collect::<crate::HashSet<_>>(),
        );
        assert_eq!(expected.gc_stats(), recovered.gc_stats());
        assert_eq!(expected.seqno(), recovered.seqno());

        assert_eq!(&*tree.get("a", seqno.get())?.unwrap(), b"new_value");
        assert_eq!(&*tree.get("keep", seqno.get())?.unwrap(), b"x");
        assert!(tree.get("1", seqno.get())?.is_none());

        Ok(())
    }

    #[test]
    fn io_provider_noop_compaction_appends_no_edit() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());
        let tree = Config::new(dir.path(), provider.clone()).open()?;

        let version_id = tree.current_version().id();

        // NOTE: No tables, so there is nothing to compact
        provider.compact(
            &mut persister,
            &tree,
            Arc::new(crate::compaction::Major::default()),
            1,
            0,
        )?;

        assert_eq!(std::fs::read_to_string(&manifest_path)?, "");
        assert_eq!(version_id, tree.current_version().id());

        Ok(())
    }

    /// A tree that persists its version edits as JSON lines, so it can be recovered
    struct RecoverableTree {
        dir: tempfile::TempDir,
        manifest_path: std::path::PathBuf,
        persister: JsonPersister,
        provider: Arc<IoSuperVersionSetProvider>,
        tree: crate::AnyTree,
        kv_opts: Option<KvSeparationOptions>,
    }

    impl RecoverableTree {
        fn new(kv_opts: Option<KvSeparationOptions>) -> crate::Result<Self> {
            Self::with_filter(kv_opts, None)
        }

        fn with_filter(
            kv_opts: Option<KvSeparationOptions>,
            filter: Option<Arc<dyn crate::compaction::filter::Factory>>,
        ) -> crate::Result<Self> {
            let dir = tempfile::tempdir()?;
            let manifest_path = dir.path().join("version-edits.jsonl");

            let persister = JsonPersister::create(&manifest_path)?;
            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(dir.path(), provider.clone())
                .with_kv_separation(kv_opts.clone())
                .with_compaction_filter_factory(filter)
                .open()?;

            Ok(Self {
                dir,
                manifest_path,
                persister,
                provider,
                tree,
                kv_opts,
            })
        }

        fn insert(&self, key: &str, seqno: SeqNo) {
            self.tree.insert(key, "value", seqno);
        }

        fn flush(&mut self, version_seqno: SeqNo) -> crate::Result<()> {
            self.provider.flush_active_memtable(
                &mut self.persister,
                &self.tree,
                version_seqno,
                0,
            )?;
            Ok(())
        }

        fn major_compact(&mut self, version_seqno: SeqNo) -> crate::Result<()> {
            self.provider.compact(
                &mut self.persister,
                &self.tree,
                Arc::new(crate::compaction::Major::default()),
                version_seqno,
                0,
            )
        }

        /// Replays all persisted edits, and hydrates a fresh tree from them
        fn recover(&self) -> crate::Result<crate::AnyTree> {
            let mut raw_state = RawVersionData::default();

            for line in std::fs::read_to_string(&self.manifest_path)?.lines() {
                if line.is_empty() {
                    continue;
                }

                raw_state.apply(&serde_json::from_str::<VersionEdit>(line).unwrap());
            }

            let provider = Arc::new(IoSuperVersionSetProvider::new());
            let tree = Config::new(self.dir.path(), provider.clone())
                .with_kv_separation(self.kv_opts.clone())
                .open()?;

            provider.hydrate(&tree, &raw_state)?;

            Ok(tree)
        }
    }

    /// Recovery must rebuild L0 with the same (optimized) runs as the live tree,
    /// otherwise strategies that expect a disjoint L0 (FIFO) panic after a restart
    #[test]
    fn io_provider_recover_optimizes_runs() -> crate::Result<()> {
        let mut t = RecoverableTree::new(None)?;

        t.insert("a", 1);
        t.flush(2)?;

        t.insert("b", 3);
        t.flush(4)?;
        assert_eq!(1, t.tree.l0_run_count());

        let recovered = t.recover()?;
        assert_eq!(1, recovered.l0_run_count());

        // NOTE: Used to panic with "L0 needs to be disjoint"
        recovered.run_compaction(Arc::new(crate::compaction::Fifo::new(u64::MAX)), 0)?;

        Ok(())
    }

    /// A compaction result that is committed after a clear must not bring back the cleared data,
    /// neither in memory nor after recovery
    #[test]
    fn io_provider_compaction_after_clear() -> crate::Result<()> {
        let mut t = RecoverableTree::new(None)?;

        t.insert("a", 1);
        t.flush(2)?;

        t.insert("a", 3);
        t.flush(4)?;
        assert_eq!(2, t.tree.table_count());

        // Run compaction, but commit it only after the clear
        let result = t
            .tree
            .run_compaction(Arc::new(crate::compaction::Major::default()), 0)?;

        let crate::compaction::worker::CompactionOperation::Merge { created_tables, .. } =
            &result.inner
        else {
            panic!("expected merge, got {result:?}");
        };

        let created_table_ids = created_tables
            .iter()
            .map(|x| x.id().to_string())
            .collect::<Vec<_>>();

        assert_eq!(1, created_table_ids.len());

        // Do clear before compaction is registered
        t.provider.clear(&mut t.persister, &t.tree, 5)?;

        t.provider
            .register_compaction(&mut t.persister, result, 6)?;

        assert_eq!(0, t.tree.table_count());
        assert!(t.tree.get("a", SeqNo::MAX)?.is_none());

        let recovered = t.recover()?;
        assert_eq!(0, recovered.table_count());
        assert!(recovered.get("a", SeqNo::MAX)?.is_none());

        // NOTE: The discarded output table is deleted
        // (the cleared input tables are still referenced by older versions)
        let files = std::fs::read_dir(t.dir.path().join(crate::file::TABLES_FOLDER))?
            .map(|x| x.map(|x| x.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<_>, _>>()?;

        assert!(
            created_table_ids.iter().all(|id| !files.contains(id)),
            "discarded output tables {created_table_ids:?} still exist: {files:?}",
        );

        Ok(())
    }

    #[test]
    fn io_provider_flush() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());

        let tree = Config::new(dir.path(), provider.clone()).open()?;

        tree.insert("key", "value", 1);
        provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![vec![
                NormalizedRecord::add_tables(0, vec![0]),
                NormalizedRecord::set_flush_watermark(1),
                NormalizedRecord::set_version_seqno(2),
            ]],
            normalize_edits(&edits),
        );

        Ok(())
    }

    #[test]
    fn io_provider_flush_blob() -> crate::Result<()> {
        let dir: tempfile::TempDir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());

        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        tree.insert("key", "value", 1);
        provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![vec![
                NormalizedRecord::add_tables(0, vec![0]),
                NormalizedRecord::add_blob_files(vec![0]),
                NormalizedRecord::set_flush_watermark(1),
                NormalizedRecord::set_version_seqno(2),
            ]],
            normalize_edits(&edits),
        );

        Ok(())
    }

    #[test]
    fn io_provider_compaction_drops_stale_blob_files() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());

        let tree = Config::new(dir.path(), provider.clone())
            .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
            .open()?;

        tree.insert("drop", "value", 1);
        provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;

        tree.insert("keep", "value", 3);
        provider.flush_active_memtable(&mut persister, &tree, 4, 0)?;

        tree.remove("drop", 5);
        provider.flush_active_memtable(&mut persister, &tree, 6, 0)?;

        let major = Arc::new(crate::compaction::Major::default());
        provider.compact(&mut persister, &tree, major.clone(), 7, 1_000)?;
        provider.compact(&mut persister, &tree, major, 8, 1_000)?;

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
            vec![
                NormalizedRecord::add_tables(0, vec![0]),
                NormalizedRecord::add_blob_files(vec![0]),
                NormalizedRecord::set_flush_watermark(1),
                NormalizedRecord::set_version_seqno(2),
            ],
            flush_drop.iter().map(normalize_record).collect::<Vec<_>>(),
        );
        assert_eq!(
            vec![
                NormalizedRecord::add_tables(0, vec![1]),
                NormalizedRecord::add_blob_files(vec![1]),
                NormalizedRecord::set_flush_watermark(3),
                NormalizedRecord::set_version_seqno(4),
            ],
            flush_keep.iter().map(normalize_record).collect::<Vec<_>>(),
        );
        assert_eq!(
            vec![
                NormalizedRecord::add_tables(0, vec![2]),
                NormalizedRecord::set_flush_watermark(5),
                NormalizedRecord::set_version_seqno(6),
            ],
            flush_delete
                .iter()
                .map(normalize_record)
                .collect::<Vec<_>>(),
        );

        assert_eq!(
            Some(NormalizedRecord::add_tables(6, vec![3])),
            first_major_edit.iter().next().map(normalize_record),
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
            vec![
                NormalizedRecord::add_tables(6, vec![4]),
                NormalizedRecord::drop_tables(vec![3]),
                NormalizedRecord::drop_blob_files(vec![0]),
                NormalizedRecord::set_version_seqno(8),
            ],
            second_major_edit
                .iter()
                .map(normalize_record)
                .collect::<Vec<_>>(),
        );

        Ok(())
    }

    /// Dropping tables can make blob files dead, which are then deleted,
    /// so the persisted edit must drop them (and update the fragmentation map) too
    #[test]
    fn io_provider_drop_range_drops_dead_blob_files() -> crate::Result<()> {
        let mut t =
            RecoverableTree::new(Some(KvSeparationOptions::default().separation_threshold(1)))?;

        // Each flush writes one table and one blob file
        t.insert("a", 0);
        t.flush(1)?;
        t.insert("b", 2);
        t.flush(3)?;
        assert_eq!(2, t.tree.table_count());
        assert_eq!(2, t.tree.blob_file_count());

        fn assert_same_as_recovered(
            t: &RecoverableTree,
            table_count: usize,
            blob_file_count: usize,
        ) -> crate::Result<crate::AnyTree> {
            let recovered = t.recover()?;

            assert_eq!(table_count, t.tree.table_count(), "in memory");
            assert_eq!(blob_file_count, t.tree.blob_file_count(), "in memory");
            assert_eq!(table_count, recovered.table_count(), "recovered");
            assert_eq!(blob_file_count, recovered.blob_file_count(), "recovered");
            assert_eq!(
                t.tree.current_version().gc_stats(),
                recovered.current_version().gc_stats(),
            );

            Ok(recovered)
        }

        // The blob file of `a` is only referenced by the dropped table, so it is dead
        t.provider
            .drop_range(&mut t.persister, &t.tree, "a".."b", 4, 0)?;
        let recovered = assert_same_as_recovered(&t, 1, 1)?;
        assert!(recovered.get("a", SeqNo::MAX)?.is_none());
        assert!(recovered.get("b", SeqNo::MAX)?.is_some());

        t.provider
            .drop_range::<_, &str, _>(&mut t.persister, &t.tree, .., 5, 0)?;
        let recovered = assert_same_as_recovered(&t, 0, 0)?;
        assert!(recovered.get("b", SeqNo::MAX)?.is_none());

        Ok(())
    }

    /// A trivial move puts the moved tables into one run in the order they were found
    /// (newest run first), which may not be key order; recovery builds the run from the
    /// edits, so it must still end up sorted
    #[test]
    fn io_provider_trivial_move_recovers_sorted_run() -> crate::Result<()> {
        let mut t = RecoverableTree::new(None)?;

        // Two disjoint L0 runs, the newest (`c`) first
        t.insert("a", 0);
        t.flush(1)?;
        t.insert("c", 2);
        t.flush(3)?;

        // L0 is disjoint and the other levels are empty, so both tables move to the last level
        t.provider.compact(
            &mut t.persister,
            &t.tree,
            Arc::new(crate::compaction::Leveled::default()),
            4,
            0,
        )?;
        assert_eq!(0, t.tree.l0_run_count());

        let scan = |tree: &crate::AnyTree| {
            use crate::Guard;

            tree.iter(SeqNo::MAX, None)
                .map(|guard| guard.key().map(|k| k.to_vec()))
                .collect::<crate::Result<Vec<_>>>()
        };

        let expected = vec![b"a".to_vec(), b"c".to_vec()];
        assert_eq!(expected, scan(&t.tree)?, "in memory");
        assert_eq!(expected, scan(&t.recover()?)?, "recovered");

        Ok(())
    }

    struct ReplaceEverything;

    impl crate::compaction::filter::CompactionFilter for ReplaceEverything {
        fn filter_item(
            &mut self,
            _item: crate::compaction::filter::ItemAccessor<'_>,
            _ctx: &crate::compaction::filter::Context,
        ) -> crate::Result<crate::compaction::filter::Verdict> {
            Ok(crate::compaction::filter::Verdict::ReplaceValue(
                "replaced".into(),
            ))
        }
    }

    struct ReplaceEverythingFactory;

    impl crate::compaction::filter::Factory for ReplaceEverythingFactory {
        fn name(&self) -> &str {
            "ReplaceEverything"
        }

        fn make_filter(
            &self,
            _ctx: &crate::compaction::filter::Context,
        ) -> Box<dyn crate::compaction::filter::CompactionFilter> {
            Box::new(ReplaceEverything)
        }
    }

    /// A compaction filter that replaces values writes them into new blob files,
    /// so the persisted edit must add them
    #[test]
    fn io_provider_compaction_filter_adds_new_blob_files() -> crate::Result<()> {
        let mut t = RecoverableTree::with_filter(
            Some(KvSeparationOptions::default().separation_threshold(1)),
            Some(Arc::new(ReplaceEverythingFactory)),
        )?;

        t.tree.insert("a", "old", 0);
        t.flush(1)?;
        t.major_compact(2)?;

        assert_eq!(
            Some(b"replaced".as_slice()),
            t.tree.get("a", SeqNo::MAX)?.as_deref(),
            "in memory",
        );

        let recovered = t.recover()?;

        assert_eq!(t.tree.blob_file_count(), recovered.blob_file_count());
        assert_eq!(
            Some(b"replaced".as_slice()),
            recovered.get("a", SeqNo::MAX)?.as_deref(),
            "recovered",
        );

        Ok(())
    }

    /// Blob GC relocates live blobs into new blob files,
    /// so the persisted edit must add them
    #[test]
    fn io_provider_relocation_adds_new_blob_files() -> crate::Result<()> {
        let mut t = RecoverableTree::new(Some(
            KvSeparationOptions::default()
                .separation_threshold(1)
                .staleness_threshold(0.1)
                .age_cutoff(1.0),
        ))?;

        // Blob file 0 has `a` and `b`
        t.tree.insert("a", "old", 0);
        t.tree.insert("b", "b", 1);
        t.flush(2)?;

        // Blob file 1 has the new `a`
        t.tree.insert("a", "new", 3);
        t.flush(4)?;

        let major = Arc::new(crate::compaction::Major::default());

        // Drops the old `a`, so blob file 0 is partially stale
        t.provider
            .compact(&mut t.persister, &t.tree, major.clone(), 5, 1_000)?;

        // Relocates `b` out of blob file 0, into a new blob file
        let blob_files_before = t
            .tree
            .current_version()
            .blob_files
            .list_ids()
            .copied()
            .collect::<Vec<_>>();
        t.provider
            .compact(&mut t.persister, &t.tree, major, 6, 1_000)?;

        let blob_files = t
            .tree
            .current_version()
            .blob_files
            .list_ids()
            .copied()
            .collect::<Vec<_>>();
        assert!(
            blob_files.iter().any(|id| !blob_files_before.contains(id)),
            "compaction should have relocated into a new blob file: {blob_files_before:?} -> {blob_files:?}",
        );

        let recovered = t.recover()?;

        let mut recovered_blob_files = recovered
            .current_version()
            .blob_files
            .list_ids()
            .copied()
            .collect::<Vec<_>>();
        let mut blob_files = blob_files;
        recovered_blob_files.sort_unstable();
        blob_files.sort_unstable();
        assert_eq!(blob_files, recovered_blob_files);

        assert_eq!(
            Some(b"new".as_slice()),
            recovered.get("a", SeqNo::MAX)?.as_deref()
        );
        assert_eq!(
            Some(b"b".as_slice()),
            recovered.get("b", SeqNo::MAX)?.as_deref()
        );

        Ok(())
    }

    #[test]
    fn io_provider_compaction_persists_fragmentation_map() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());

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
        provider.flush_active_memtable(&mut persister, &tree, flush_1_seqno, 0)?;

        tree.insert("2", value, seqno.next());
        let flush_2_seqno = seqno.next();
        provider.flush_active_memtable(&mut persister, &tree, flush_2_seqno, 0)?;

        tree.insert("3", value, seqno.next());
        let flush_3_seqno = seqno.next();
        provider.flush_active_memtable(&mut persister, &tree, flush_3_seqno, 0)?;

        let major = Arc::new(crate::compaction::Major::default());

        tree.remove("1", seqno.next());
        let delete_1_flush_seqno = seqno.next();
        provider.flush_active_memtable(&mut persister, &tree, delete_1_flush_seqno, 0)?;
        let delete_1_compaction_seqno = seqno.next();
        provider.compact(
            &mut persister,
            &tree,
            major.clone(),
            delete_1_compaction_seqno,
            1_000,
        )?;

        tree.remove("2", seqno.next());
        let delete_2_flush_seqno = seqno.next();
        provider.flush_active_memtable(&mut persister, &tree, delete_2_flush_seqno, 0)?;
        let delete_2_compaction_seqno = seqno.next();
        provider.compact(
            &mut persister,
            &tree,
            major.clone(),
            delete_2_compaction_seqno,
            1_000,
        )?;

        tree.remove("3", seqno.next());
        let delete_3_flush_seqno = seqno.next();
        provider.flush_active_memtable(&mut persister, &tree, delete_3_flush_seqno, 0)?;
        let delete_3_compaction_seqno = seqno.next();
        provider.compact(
            &mut persister,
            &tree,
            major.clone(),
            delete_3_compaction_seqno,
            1_000,
        )?;

        let final_compaction_seqno = seqno.next();
        provider.compact(&mut persister, &tree, major, final_compaction_seqno, 1_000)?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        // NOTE: Each flush's last item is written right before the flush's seqno,
        // so the flush watermark (highest flushed item) is `flush seqno - 1`;
        // compactions don't write a flush watermark
        assert_eq!(
            vec![
                // Insert 1
                vec![
                    NormalizedRecord::add_tables(0, vec![0]),
                    NormalizedRecord::add_blob_files(vec![0]),
                    NormalizedRecord::set_flush_watermark(flush_1_seqno - 1),
                    NormalizedRecord::set_version_seqno(flush_1_seqno),
                ],
                // Insert 2
                vec![
                    NormalizedRecord::add_tables(0, vec![1]),
                    NormalizedRecord::add_blob_files(vec![1]),
                    NormalizedRecord::set_flush_watermark(flush_2_seqno - 1),
                    NormalizedRecord::set_version_seqno(flush_2_seqno),
                ],
                // Insert 3
                vec![
                    NormalizedRecord::add_tables(0, vec![2]),
                    NormalizedRecord::add_blob_files(vec![2]),
                    NormalizedRecord::set_flush_watermark(flush_3_seqno - 1),
                    NormalizedRecord::set_version_seqno(flush_3_seqno),
                ],
                // Remove 1
                vec![
                    NormalizedRecord::add_tables(0, vec![3]),
                    NormalizedRecord::set_flush_watermark(delete_1_flush_seqno - 1),
                    NormalizedRecord::set_version_seqno(delete_1_flush_seqno),
                ],
                // Major compaction
                vec![
                    NormalizedRecord::add_tables(6, vec![4]),
                    NormalizedRecord::drop_tables(vec![0, 1, 2, 3]),
                    NormalizedRecord::update_frag_map({
                        let mut map = FragmentationMap::default();
                        map.insert(0, FragmentationEntry::new(1, size, size));
                        map
                    }),
                    NormalizedRecord::set_version_seqno(delete_1_compaction_seqno),
                ],
                // Remove 2
                vec![
                    NormalizedRecord::add_tables(0, vec![5]),
                    NormalizedRecord::set_flush_watermark(delete_2_flush_seqno - 1),
                    NormalizedRecord::set_version_seqno(delete_2_flush_seqno),
                ],
                // Major compaction
                vec![
                    NormalizedRecord::add_tables(6, vec![6]),
                    NormalizedRecord::drop_tables(vec![5, 4]),
                    NormalizedRecord::update_frag_map({
                        let mut map = FragmentationMap::default();
                        map.insert(1, FragmentationEntry::new(1, size, size));
                        map
                    }),
                    NormalizedRecord::drop_blob_files(vec![0]),
                    NormalizedRecord::set_version_seqno(delete_2_compaction_seqno),
                ],
                // Remove 3
                vec![
                    NormalizedRecord::add_tables(0, vec![7]),
                    NormalizedRecord::set_flush_watermark(delete_3_flush_seqno - 1),
                    NormalizedRecord::set_version_seqno(delete_3_flush_seqno),
                ],
                // Major compaction
                vec![
                    NormalizedRecord::add_tables(6, vec![8]),
                    NormalizedRecord::drop_tables(vec![6, 7]),
                    NormalizedRecord::update_frag_map({
                        let mut map = FragmentationMap::default();
                        map.insert(2, FragmentationEntry::new(1, size, size));
                        map
                    }),
                    NormalizedRecord::drop_blob_files(vec![1]),
                    NormalizedRecord::set_version_seqno(delete_3_compaction_seqno),
                ],
                // Final major compaction
                vec![
                    NormalizedRecord::add_tables(6, vec![9]),
                    NormalizedRecord::drop_tables(vec![8]),
                    NormalizedRecord::drop_blob_files(vec![2]),
                    NormalizedRecord::set_version_seqno(final_compaction_seqno),
                ],
            ],
            normalize_edits(&edits),
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
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());

        let tree = Config::new(dir.path(), provider.clone()).open()?;

        tree.insert("key", "value", 1);
        provider.flush_active_memtable(&mut persister, &tree, 2, 0)?;
        provider.compact(
            &mut persister,
            &tree,
            Arc::new(crate::compaction::Leveled::default()),
            3,
            0,
        )?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                vec![
                    NormalizedRecord::add_tables(0, vec![0]),
                    NormalizedRecord::set_flush_watermark(1),
                    NormalizedRecord::set_version_seqno(2),
                ],
                vec![
                    NormalizedRecord::move_tables(6, vec![0]),
                    NormalizedRecord::set_version_seqno(3),
                ],
            ],
            normalize_edits(&edits),
        );

        Ok(())
    }

    #[test]
    fn io_provider_flush_and_major_compaction() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let manifest_path = dir.path().join("version-edits.jsonl");
        let mut persister = JsonPersister::create(&manifest_path)?;
        let provider = Arc::new(IoSuperVersionSetProvider::new());

        let tree = Config::new(dir.path(), provider.clone()).open()?;

        tree.insert("a", "value", 0);
        provider.flush_active_memtable(&mut persister, &tree, 1, 0)?;

        tree.insert("b", "value", 2);
        provider.flush_active_memtable(&mut persister, &tree, 3, 0)?;

        provider.compact(
            &mut persister,
            &tree,
            Arc::new(crate::compaction::Major::default()),
            4,
            0,
        )?;

        let edits = std::fs::read_to_string(manifest_path)?
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<VersionEdit>(line).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            vec![
                vec![
                    NormalizedRecord::add_tables(0, vec![0]),
                    NormalizedRecord::set_flush_watermark(0),
                    NormalizedRecord::set_version_seqno(1),
                ],
                vec![
                    NormalizedRecord::add_tables(0, vec![1]),
                    NormalizedRecord::set_flush_watermark(2),
                    NormalizedRecord::set_version_seqno(3),
                ],
                vec![
                    NormalizedRecord::add_tables(6, vec![2]),
                    NormalizedRecord::drop_tables(vec![0, 1]),
                    NormalizedRecord::set_version_seqno(4),
                ],
            ],
            normalize_edits(&edits),
        );

        Ok(())
    }
}
