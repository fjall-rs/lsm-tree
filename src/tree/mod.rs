// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod ingest;
pub mod inner;
pub mod sealed;

use crate::{
    compaction::{drop_range::OwnedBounds, state::CompactionState, CompactionStrategy},
    config::Config,
    file::CURRENT_VERSION_FILE,
    format_version::FormatVersion,
    iter_guard::{IterGuard, IterGuardImpl},
    manifest::Manifest,
    memtable::Memtable,
    slice::Slice,
    table::Table,
    value::InternalValue,
    version::{recovery::recover, SuperVersion, SuperVersions, Version},
    vlog::BlobFile,
    AbstractTree, Checksum, KvPair, SeqNo, SequenceNumberCounter, TableId, UserKey, UserValue,
    ValueType,
};
use inner::{TreeId, TreeInner};
use std::{
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Iterator value guard
pub struct Guard(crate::Result<(UserKey, UserValue)>);

impl IterGuard for Guard {
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)> {
        let (k, v) = self.0?;

        if pred(&k) {
            Ok((k, Some(v)))
        } else {
            Ok((k, None))
        }
    }

    fn key(self) -> crate::Result<UserKey> {
        self.0.map(|(k, _)| k)
    }

    fn size(self) -> crate::Result<u32> {
        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        self.into_inner().map(|(_, v)| v.len() as u32)
    }

    fn into_inner(self) -> crate::Result<(UserKey, UserValue)> {
        self.0
    }
}

fn ignore_tombstone_value(item: InternalValue) -> Option<InternalValue> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

/// A log-structured merge tree (LSM-tree/LSMT)
#[derive(Clone)]
pub struct Tree(#[doc(hidden)] pub Arc<TreeInner>);

impl std::ops::Deref for Tree {
    type Target = TreeInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AbstractTree for Tree {
    fn table_file_cache_size(&self) -> usize {
        self.config.descriptor_table.len()
    }

    fn get_version_history_lock(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, crate::version::SuperVersions> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history.write().expect("lock is poisoned")
    }

    fn next_table_id(&self) -> TableId {
        self.0.table_id_counter.get()
    }

    fn id(&self) -> TreeId {
        self.id
    }

    fn blob_file_count(&self) -> usize {
        0
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let super_version = self
            .version_history
            .read()
            .expect("lock is poisoned")
            .get_version_for_snapshot(seqno);

        Self::get_internal_entry_from_version(&super_version, key, seqno)
    }

    fn current_version(&self) -> Version {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history
            .read()
            .expect("poisoned")
            .latest_version()
            .version
    }

    fn get_flush_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.flush_lock.lock().expect("lock is poisoned")
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        &self.0.metrics
    }

    fn version_free_list_len(&self) -> usize {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history
            .read()
            .expect("lock is poisoned")
            .free_list_len()
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        Box::new(
            self.create_prefix(&prefix, seqno, index)
                .map(|kv| IterGuardImpl::Standard(Guard(kv))),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        Box::new(
            self.create_range(&range, seqno, index)
                .map(|kv| IterGuardImpl::Standard(Guard(kv))),
        )
    }

    /// Returns the number of tombstones in the tree.
    fn tombstone_count(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::tombstone_count)
            .sum()
    }

    /// Returns the number of weak tombstones (single deletes) in the tree.
    fn weak_tombstone_count(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::weak_tombstone_count)
            .sum()
    }

    /// Returns the number of value entries that become reclaimable once weak tombstones can be GC'd.
    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::weak_tombstone_reclaimable)
            .sum()
    }

    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        let (bounds, is_empty) = Self::range_bounds_to_owned_bounds(&range);

        if is_empty {
            return Ok(());
        }

        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(bounds));

        // IMPORTANT: Write lock so we can be the only compaction going on
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let _lock = self
            .0
            .major_compaction_lock
            .write()
            .expect("lock is poisoned");

        log::info!("Starting drop_range compaction");
        self.inner_compact(strategy, 0)
    }

    fn clear(&self) -> crate::Result<()> {
        let mut versions = self.get_version_history_lock();

        versions.upgrade_version(
            &self.config.path,
            |v| {
                let mut copy = v.clone();
                copy.active_memtable = Arc::new(Memtable::new(self.memtable_id_counter.next()));
                copy.sealed_memtables = Arc::default();
                copy.version = Version::new(v.version.id() + 1, self.tree_type());
                Ok(copy)
            },
            &self.config.seqno,
            &self.config.visible_seqno,
        )
    }

    #[doc(hidden)]
    fn major_compact(&self, target_size: u64, seqno_threshold: SeqNo) -> crate::Result<()> {
        let strategy = Arc::new(crate::compaction::major::Strategy::new(target_size));

        // IMPORTANT: Write lock so we can be the only compaction going on
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let _lock = self
            .0
            .major_compaction_lock
            .write()
            .expect("lock is poisoned");

        log::info!("Starting major compaction");
        self.inner_compact(strategy, seqno_threshold)
    }

    fn l0_run_count(&self) -> usize {
        self.current_version()
            .level(0)
            .map(|x| x.run_count())
            .unwrap_or_default()
    }

    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
        Ok(self.get(key, seqno)?.map(|x| x.len() as u32))
    }

    fn filter_size(&self) -> u64 {
        self.current_version()
            .iter_tables()
            .map(Table::filter_size)
            .map(u64::from)
            .sum()
    }

    fn pinned_filter_size(&self) -> usize {
        self.current_version()
            .iter_tables()
            .map(Table::pinned_filter_size)
            .sum()
    }

    fn pinned_block_index_size(&self) -> usize {
        self.current_version()
            .iter_tables()
            .map(Table::pinned_block_index_size)
            .sum()
    }

    fn sealed_memtable_count(&self) -> usize {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history
            .read()
            .expect("lock is poisoned")
            .latest_version()
            .sealed_memtables
            .len()
    }

    fn flush_to_tables(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<(Vec<Table>, Option<Vec<BlobFile>>)>> {
        use crate::{file::TABLES_FOLDER, table::multi_writer::MultiWriter};
        use std::time::Instant;

        let start = Instant::now();

        let folder = self.config.path.join(TABLES_FOLDER);

        let data_block_size = self.config.data_block_size_policy.get(0);

        let data_block_restart_interval = self.config.data_block_restart_interval_policy.get(0);
        let index_block_restart_interval = self.config.index_block_restart_interval_policy.get(0);

        let data_block_compression = self.config.data_block_compression_policy.get(0);
        let index_block_compression = self.config.index_block_compression_policy.get(0);

        let data_block_hash_ratio = self.config.data_block_hash_ratio_policy.get(0);

        let index_partitioning = self.config.index_block_partitioning_policy.get(0);
        let filter_partitioning = self.config.filter_block_partitioning_policy.get(0);

        log::debug!(
            "Flushing memtable(s) to {}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression}, index_block_compression={index_block_compression}",
            folder.display(),
        );

        let mut table_writer = MultiWriter::new(
            folder.clone(),
            self.table_id_counter.clone(),
            64 * 1_024 * 1_024,
            0,
        )?
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_index_block_compression(index_block_compression)
        .use_data_block_size(data_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::table::filter::BloomConstructionPolicy;

            match self.config.filter_policy.get(0) {
                Bloom(policy) => policy,
                None => BloomConstructionPolicy::BitsPerKey(0.0),
            }
        });

        if index_partitioning {
            table_writer = table_writer.use_partitioned_index();
        }
        if filter_partitioning {
            table_writer = table_writer.use_partitioned_filter();
        }

        for item in stream {
            table_writer.write(item?)?;
        }

        let result = table_writer.finish()?;

        log::debug!("Flushed memtable(s) in {:?}", start.elapsed());

        let pin_filter = self.config.filter_block_pinning_policy.get(0);
        let pin_index = self.config.index_block_pinning_policy.get(0);

        // Load tables
        let tables = result
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    folder.join(table_id.to_string()),
                    checksum,
                    0,
                    self.id,
                    self.config.cache.clone(),
                    self.config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    self.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Some((tables, None)))
    }

    #[expect(clippy::significant_drop_tightening)]
    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<crate::blob_tree::FragmentationMap>,
        sealed_memtables_to_delete: &[crate::tree::inner::MemtableId],
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        log::trace!(
            "Registering {} tables, {} blob files",
            tables.len(),
            blob_files.map(<[BlobFile]>::len).unwrap_or_default(),
        );

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut _compaction_state = self.compaction_state.lock().expect("lock is poisoned");
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut version_lock = self.version_history.write().expect("lock is poisoned");

        version_lock.upgrade_version(
            &self.config.path,
            |current| {
                let mut copy = current.clone();

                copy.version = copy.version.with_new_l0_run(
                    tables,
                    blob_files,
                    frag_map.filter(|x| !x.is_empty()),
                );

                for &table_id in sealed_memtables_to_delete {
                    log::trace!("releasing sealed memtable #{table_id}");
                    copy.sealed_memtables = Arc::new(copy.sealed_memtables.remove(table_id));
                }

                Ok(copy)
            },
            &self.config.seqno,
            &self.config.visible_seqno,
        )?;

        if let Err(e) = version_lock.maintenance(&self.config.path, gc_watermark) {
            log::warn!("Version GC failed: {e:?}");
        }

        Ok(())
    }

    fn clear_active_memtable(&self) {
        use crate::tree::sealed::SealedMemtables;

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut version_history_lock = self.version_history.write().expect("lock is poisoned");
        let super_version = version_history_lock.latest_version();

        if super_version.active_memtable.is_empty() {
            return;
        }

        let mut copy = version_history_lock.latest_version();
        copy.active_memtable = Arc::new(Memtable::new(self.memtable_id_counter.next()));
        copy.sealed_memtables = Arc::new(SealedMemtables::default());

        // Rotate does not modify the memtable, so it cannot break snapshots
        copy.seqno = super_version.seqno;

        version_history_lock.replace_latest_version(copy);

        log::trace!("cleared active memtable");
    }

    fn compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        // NOTE: Read lock major compaction lock
        // That way, if a major compaction is running, we cannot proceed
        // But in general, parallel (non-major) compactions can occur
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let _lock = self
            .0
            .major_compaction_lock
            .read()
            .expect("lock is poisoned");

        self.inner_compact(strategy, seqno_threshold)
    }

    fn get_next_table_id(&self) -> TableId {
        self.0.get_next_table_id()
    }

    fn tree_config(&self) -> &Config {
        &self.config
    }

    fn active_memtable(&self) -> Arc<Memtable> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history
            .read()
            .expect("lock is poisoned")
            .latest_version()
            .active_memtable
    }

    fn tree_type(&self) -> crate::TreeType {
        crate::TreeType::Standard
    }

    #[expect(clippy::significant_drop_tightening)]
    fn rotate_memtable(&self) -> Option<Arc<Memtable>> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut version_history_lock = self.version_history.write().expect("lock is poisoned");
        let super_version = version_history_lock.latest_version();

        if super_version.active_memtable.is_empty() {
            return None;
        }

        let yanked_memtable = super_version.active_memtable;

        let mut copy = version_history_lock.latest_version();
        copy.active_memtable = Arc::new(Memtable::new(self.memtable_id_counter.next()));
        copy.sealed_memtables =
            Arc::new(super_version.sealed_memtables.add(yanked_memtable.clone()));

        // Rotate does not modify the memtable so it cannot break snapshots
        copy.seqno = super_version.seqno;

        version_history_lock.replace_latest_version(copy);

        log::trace!(
            "rotate: added memtable id={} to sealed memtables",
            yanked_memtable.id,
        );

        Some(yanked_memtable)
    }

    fn table_count(&self) -> usize {
        self.current_version().table_count()
    }

    fn level_table_count(&self, idx: usize) -> Option<usize> {
        self.current_version().level(idx).map(|x| x.table_count())
    }

    fn approximate_len(&self) -> usize {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let super_version = self
            .version_history
            .read()
            .expect("lock is poisoned")
            .latest_version();

        let tables_item_count = self
            .current_version()
            .iter_tables()
            .map(|x| x.metadata.item_count)
            .sum::<u64>();

        let memtable_count = super_version.active_memtable.len() as u64;
        let sealed_count = super_version
            .sealed_memtables
            .iter()
            .map(|mt| mt.len())
            .sum::<usize>() as u64;

        #[expect(clippy::expect_used, reason = "result should fit into usize")]
        (memtable_count + sealed_count + tables_item_count)
            .try_into()
            .expect("approximate_len too large for usize")
    }

    fn disk_space(&self) -> u64 {
        self.current_version()
            .iter_levels()
            .map(super::version::Level::size)
            .sum()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let version = self
            .version_history
            .read()
            .expect("lock is poisoned")
            .latest_version();

        let active = version.active_memtable.get_highest_seqno();

        let sealed = version
            .sealed_memtables
            .iter()
            .map(|mt| mt.get_highest_seqno())
            .max()
            .flatten();

        active.max(sealed)
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.current_version()
            .iter_tables()
            .map(Table::get_highest_seqno)
            .max()
    }

    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<UserValue>> {
        Ok(self
            .get_internal_entry(key.as_ref(), seqno)?
            .map(|x| x.value))
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        let value = InternalValue::from_components(key, value, seqno, ValueType::Value);
        self.append_entry(value)
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        let value = InternalValue::new_tombstone(key, seqno);
        self.append_entry(value)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        let value = InternalValue::new_weak_tombstone(key, seqno);
        self.append_entry(value)
    }
}

impl Tree {
    #[doc(hidden)]
    pub fn create_internal_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        version: SuperVersion,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'static {
        use crate::range::{IterState, TreeIter};
        use std::ops::Bound::{self, Excluded, Included, Unbounded};

        let lo: Bound<UserKey> = match range.start_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let hi: Bound<UserKey> = match range.end_bound() {
            Included(x) => Included(x.as_ref().into()),
            Excluded(x) => Excluded(x.as_ref().into()),
            Unbounded => Unbounded,
        };

        let bounds: (Bound<UserKey>, Bound<UserKey>) = (lo, hi);

        let iter_state = { IterState { version, ephemeral } };

        TreeIter::create_range(iter_state, bounds, seqno)
    }

    pub(crate) fn get_internal_entry_from_version(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        if let Some(entry) = super_version.active_memtable.get(key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in sealed memtables
        if let Some(entry) =
            Self::get_internal_entry_from_sealed_memtables(super_version, key, seqno)
        {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in tables... this may involve disk I/O
        Self::get_internal_entry_from_tables(&super_version.version, key, seqno)
    }

    fn get_internal_entry_from_tables(
        version: &Version,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        // NOTE: Create key hash for hash sharing
        // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
        let key_hash = crate::table::filter::standard_bloom::Builder::get_hash(key);

        for table in version
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .filter_map(|run| run.get_for_key(key))
        {
            if let Some(item) = table.get(key, seqno, key_hash)? {
                return Ok(ignore_tombstone_value(item));
            }
        }

        Ok(None)
    }

    fn get_internal_entry_from_sealed_memtables(
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
    ) -> Option<InternalValue> {
        for mt in super_version.sealed_memtables.iter().rev() {
            if let Some(entry) = mt.get(key, seqno) {
                return Some(entry);
            }
        }

        None
    }

    pub(crate) fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history
            .read()
            .expect("lock is poisoned")
            .get_version_for_snapshot(seqno)
    }

    /// Normalizes a user-provided range into owned `Bound<Slice>` values.
    ///
    /// Returns a tuple containing:
    /// - the `OwnedBounds` that mirror the original bounds semantics (including
    ///   inclusive/exclusive markers and unbounded endpoints), and
    /// - a `bool` flag indicating whether the normalized range is logically
    ///   empty (e.g., when the lower bound is greater than the upper bound).
    ///
    /// Callers can use the flag to detect empty ranges and skip further work
    /// while still having access to the normalized bounds for non-empty cases.
    fn range_bounds_to_owned_bounds<K: AsRef<[u8]>, R: RangeBounds<K>>(
        range: &R,
    ) -> (OwnedBounds, bool) {
        use Bound::{Excluded, Included, Unbounded};

        let start = match range.start_bound() {
            Included(key) => Included(Slice::from(key.as_ref())),
            Excluded(key) => Excluded(Slice::from(key.as_ref())),
            Unbounded => Unbounded,
        };

        let end = match range.end_bound() {
            Included(key) => Included(Slice::from(key.as_ref())),
            Excluded(key) => Excluded(Slice::from(key.as_ref())),
            Unbounded => Unbounded,
        };

        let is_empty =
            if let (Included(lo) | Excluded(lo), Included(hi) | Excluded(hi)) = (&start, &end) {
                lo.as_ref() > hi.as_ref()
            } else {
                false
            };

        (OwnedBounds { start, end }, is_empty)
    }

    /// Opens an LSM-tree in the given directory.
    ///
    /// Will recover previous state if the folder was previously
    /// occupied by an LSM-tree, including the previous configuration.
    /// If not, a new tree will be initialized with the given config.
    ///
    /// After recovering a previous state, use [`Tree::set_active_memtable`]
    /// to fill the memtable with data from a write-ahead log for full durability.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    pub(crate) fn open(config: Config) -> crate::Result<Self> {
        log::debug!("Opening LSM-tree at {}", config.path.display());

        // Check for old version
        if config.path.join("version").try_exists()? {
            log::error!("It looks like you are trying to open a V1 database - the database needs a manual migration, however a migration tool is not provided, as V1 is extremely outdated.");
            return Err(crate::Error::InvalidVersion(FormatVersion::V1.into()));
        }

        let tree = if config.path.join(CURRENT_VERSION_FILE).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }?;

        Ok(tree)
    }

    /// Returns `true` if there are some tables that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        !self
            .compaction_state
            .lock()
            .expect("lock is poisoned")
            .hidden_set()
            .is_empty()
    }

    fn inner_compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        mvcc_gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        use crate::compaction::worker::{do_compaction, Options};

        let mut opts = Options::from_tree(self, strategy);
        opts.mvcc_gc_watermark = mvcc_gc_watermark;

        do_compaction(&opts)?;

        log::debug!("Compaction run over");

        Ok(())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn create_iter(
        &self,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.create_range::<UserKey, _>(&.., seqno, ephemeral)
    }

    #[doc(hidden)]
    pub fn create_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &self,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let super_version = self
            .version_history
            .read()
            .expect("lock is poisoned")
            .get_version_for_snapshot(seqno);

        Self::create_internal_range(super_version, range, seqno, ephemeral).map(|item| match item {
            Ok(kv) => Ok((kv.key.user_key, kv.value)),
            Err(e) => Err(e),
        })
    }

    #[doc(hidden)]
    pub fn create_prefix<'a, K: AsRef<[u8]> + 'a>(
        &self,
        prefix: K,
        seqno: SeqNo,
        ephemeral: Option<(Arc<Memtable>, SeqNo)>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        use crate::range::prefix_to_range;

        let range = prefix_to_range(prefix.as_ref());
        self.create_range(&range, seqno, ephemeral)
    }

    /// Adds an item to the active memtable.
    ///
    /// Returns the added item's size and new size of the memtable.
    #[doc(hidden)]
    #[must_use]
    pub fn append_entry(&self, value: InternalValue) -> (u64, u64) {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.version_history
            .read()
            .expect("lock is poisoned")
            .latest_version()
            .active_memtable
            .insert(value)
    }

    /// Recovers previous state, by loading the level manifest, tables and blob files.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    fn recover(mut config: Config) -> crate::Result<Self> {
        use crate::stop_signal::StopSignal;
        use inner::get_next_tree_id;

        log::info!("Recovering LSM-tree at {}", config.path.display());

        // let manifest = {
        //     let manifest_path = config.path.join(MANIFEST_FILE);
        //     let reader = sfa::Reader::new(&manifest_path)?;
        //     Manifest::decode_from(&manifest_path, &reader)?
        // };

        // if manifest.version != FormatVersion::V3 {
        //     return Err(crate::Error::InvalidVersion(manifest.version.into()));
        // }

        let tree_id = get_next_tree_id();

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let version = Self::recover_levels(
            &config.path,
            tree_id,
            &config,
            #[cfg(feature = "metrics")]
            &metrics,
        )?;

        {
            let manifest_path = config.path.join(format!("v{}", version.id()));
            let reader = sfa::Reader::new(&manifest_path)?;
            let manifest = Manifest::decode_from(&manifest_path, &reader)?;

            if manifest.version != FormatVersion::V3 {
                return Err(crate::Error::InvalidVersion(manifest.version.into()));
            }

            let requested_tree_type = match config.kv_separation_opts {
                Some(_) => crate::TreeType::Blob,
                None => crate::TreeType::Standard,
            };

            if version.tree_type() != requested_tree_type {
                log::error!(
                    "Tried to open a {requested_tree_type:?}Tree, but the existing tree is of type {:?}Tree. This indicates a misconfiguration or corruption.",
                    version.tree_type(),
                );
                return Err(crate::Error::Unrecoverable);
            }

            // IMPORTANT: Restore persisted config
            config.level_count = manifest.level_count;
        }

        let highest_table_id = version
            .iter_tables()
            .map(Table::id)
            .max()
            .unwrap_or_default();

        let inner = TreeInner {
            id: tree_id,
            memtable_id_counter: SequenceNumberCounter::default(),
            table_id_counter: SequenceNumberCounter::new(highest_table_id + 1),
            blob_file_id_counter: SequenceNumberCounter::default(),
            version_history: Arc::new(RwLock::new(SuperVersions::new(version))),
            stop_signal: StopSignal::default(),
            config: Arc::new(config),
            major_compaction_lock: RwLock::default(),
            flush_lock: Mutex::default(),
            compaction_state: Arc::new(Mutex::new(CompactionState::default())),

            #[cfg(feature = "metrics")]
            metrics,
        };

        Ok(Self(Arc::new(inner)))
    }

    /// Creates a new LSM-tree in a directory.
    fn create_new(config: Config) -> crate::Result<Self> {
        use crate::file::{fsync_directory, TABLES_FOLDER};
        use std::fs::create_dir_all;

        let path = config.path.clone();
        log::trace!("Creating LSM-tree at {}", path.display());

        create_dir_all(&path)?;

        let table_folder_path = path.join(TABLES_FOLDER);
        create_dir_all(&table_folder_path)?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&table_folder_path)?;
        fsync_directory(&path)?;

        let inner = TreeInner::create_new(config)?;
        Ok(Self(Arc::new(inner)))
    }

    /// Recovers the level manifest, loading all tables from disk.
    fn recover_levels<P: AsRef<Path>>(
        tree_path: P,
        tree_id: TreeId,
        config: &Config,
        #[cfg(feature = "metrics")] metrics: &Arc<Metrics>,
    ) -> crate::Result<Version> {
        use crate::{file::fsync_directory, file::TABLES_FOLDER, TableId};

        let tree_path = tree_path.as_ref();

        let recovery = recover(tree_path)?;

        let table_map = {
            let mut result: crate::HashMap<TableId, (u8 /* Level index */, Checksum, SeqNo)> =
                crate::HashMap::default();

            for (level_idx, table_ids) in recovery.table_ids.iter().enumerate() {
                for run in table_ids {
                    for table in run {
                        #[expect(
                            clippy::expect_used,
                            reason = "there are always less than 256 levels"
                        )]
                        result.insert(
                            table.id,
                            (
                                level_idx
                                    .try_into()
                                    .expect("there are less than 256 levels"),
                                table.checksum,
                                table.global_seqno,
                            ),
                        );
                    }
                }
            }

            result
        };

        let cnt = table_map.len();

        log::debug!("Recovering {cnt} tables from {}", tree_path.display());

        let progress_mod = match cnt {
            _ if cnt <= 20 => 1,
            _ if cnt <= 100 => 10,
            _ => 100,
        };

        let mut tables = vec![];

        let table_base_folder = tree_path.join(TABLES_FOLDER);

        if !table_base_folder.try_exists()? {
            std::fs::create_dir_all(&table_base_folder)?;
            fsync_directory(&table_base_folder)?;
        }

        let mut orphaned_tables = vec![];

        for (idx, dirent) in std::fs::read_dir(&table_base_folder)?.enumerate() {
            let dirent = dirent?;
            let file_name = dirent.file_name();

            // https://en.wikipedia.org/wiki/.DS_Store
            if file_name == ".DS_Store" {
                continue;
            }

            // https://en.wikipedia.org/wiki/AppleSingle_and_AppleDouble_formats
            if file_name.to_string_lossy().starts_with("._") {
                continue;
            }

            let table_file_name = file_name.to_str().ok_or_else(|| {
                log::error!("invalid table file name {}", file_name.display());
                crate::Error::Unrecoverable
            })?;

            let table_file_path = dirent.path();
            assert!(!table_file_path.is_dir());

            log::debug!("Recovering table from {}", table_file_path.display());

            let table_id = table_file_name.parse::<TableId>().map_err(|e| {
                log::error!("invalid table file name {table_file_name:?}: {e:?}");
                crate::Error::Unrecoverable
            })?;

            if let Some(&(level_idx, checksum, global_seqno)) = table_map.get(&table_id) {
                let pin_filter = config.filter_block_pinning_policy.get(level_idx.into());
                let pin_index = config.index_block_pinning_policy.get(level_idx.into());

                let table = Table::recover(
                    table_file_path,
                    checksum,
                    global_seqno,
                    tree_id,
                    config.cache.clone(),
                    config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    metrics.clone(),
                )?;

                log::debug!("Recovered table from {:?}", table.path);

                tables.push(table);

                if idx % progress_mod == 0 {
                    log::debug!("Recovered {idx}/{cnt} tables");
                }
            } else {
                orphaned_tables.push(table_file_path);
            }
        }

        if tables.len() < cnt {
            log::error!(
                "Recovered less tables than expected: {:?}",
                table_map.keys(),
            );
            return Err(crate::Error::Unrecoverable);
        }

        log::debug!("Successfully recovered {} tables", tables.len());

        let (blob_files, orphaned_blob_files) = crate::vlog::recover_blob_files(
            &tree_path.join(crate::file::BLOBS_FOLDER),
            &recovery.blob_file_ids,
        )?;

        let version = Version::from_recovery(recovery, &tables, &blob_files)?;

        // NOTE: Cleanup old versions
        // But only after we definitely recovered the latest version
        Self::cleanup_orphaned_version(tree_path, version.id())?;

        for table_path in orphaned_tables {
            log::debug!("Deleting orphaned table {}", table_path.display());
            std::fs::remove_file(&table_path)?;
        }

        for blob_file_path in orphaned_blob_files {
            log::debug!("Deleting orphaned blob file {}", blob_file_path.display());
            std::fs::remove_file(&blob_file_path)?;
        }

        Ok(version)
    }

    fn cleanup_orphaned_version(
        path: &Path,
        latest_version_id: crate::version::VersionId,
    ) -> crate::Result<()> {
        let version_str = format!("v{latest_version_id}");

        for file in std::fs::read_dir(path)? {
            let dirent = file?;

            if dirent.file_type()?.is_dir() {
                continue;
            }

            let name = dirent.file_name();

            if name.to_string_lossy().starts_with('v') && *name != *version_str {
                log::trace!("Cleanup orphaned version {}", name.display());
                std::fs::remove_file(dirent.path())?;
            }
        }

        Ok(())
    }
}
