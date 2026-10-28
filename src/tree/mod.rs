// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod ingest;
pub mod inner;
pub mod sealed;

#[cfg(test)]
pub mod test;

use crate::{
    compaction::{state::CompactionState, worker::NewCompactionResult, CompactionStrategy},
    config::Config,
    file::BLOBS_FOLDER,
    format_version::FormatVersion,
    iter_guard::{IterGuard, IterGuardImpl},
    key::InternalKey,
    memtable::Memtable,
    slice::Slice,
    table::Table,
    value::InternalValue,
    version::{SuperVersion, Version},
    vlog::BlobFile,
    AbstractTree, KvPair, SeqNo, SequenceNumberCounter, TableId, UserKey, UserValue, ValueType,
};
use inner::{TreeId, TreeInner};
use std::{
    ops::RangeBounds,
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
    fn inner(&self) -> &crate::tree::inner::TreeInner {
        &self.0
    }

    fn table_file_cache_size(&self) -> usize {
        self.config
            .descriptor_table
            .as_ref()
            .map_or(0, |dt| dt.len())
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

    fn print_trace(&self, key: &[u8]) -> crate::Result<()> {
        let super_version = self.tree_config().version_set_provider.latest_version();

        let key = Slice::from(key);

        for kv in super_version
            .active_memtable
            .range(InternalKey::new(key.clone(), SeqNo::MAX, ValueType::Value)..)
        {
            log::info!("[Active] {kv:?}");
        }

        for mt in super_version.sealed_memtables.iter().rev() {
            for kv in mt.range(InternalKey::new(key.clone(), SeqNo::MAX, ValueType::Value)..) {
                log::info!("[Sealed #{}] {kv:?}", mt.id());
            }
        }

        for table in super_version
            .version
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .filter_map(|run| run.get_for_key(&key))
        {
            for kv in table.range(..) {
                let kv = kv?;

                if kv.key.user_key != key {
                    break;
                }

                log::info!("[Table #{}] {kv:?}", table.id());
            }
        }

        Ok(())
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        let super_version = self.get_version_for_snapshot(seqno);
        Self::get_internal_entry_from_version(&super_version, key, seqno)
    }

    fn get_flush_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        self.flush_lock.lock().expect("lock is poisoned")
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        &self.0.metrics
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

    /// TODO: WIP
    fn run_compaction(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        gc_watermark: SeqNo,
    ) -> crate::Result<NewCompactionResult> {
        let mut opts = crate::compaction::worker::Options::from_tree(self, strategy);
        opts.mvcc_gc_watermark = gc_watermark;
        crate::compaction::worker::do_compaction(&opts)
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
            "Flushing memtable(s) to {}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression:?}, index_block_compression={index_block_compression:?}",
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

    fn get_next_table_id(&self) -> TableId {
        self.0.get_next_table_id()
    }

    fn tree_config(&self) -> &Config {
        &self.config
    }

    fn table_count(&self) -> usize {
        self.current_version().table_count()
    }

    fn level_table_count(&self, idx: usize) -> Option<usize> {
        self.current_version().level(idx).map(|x| x.table_count())
    }

    fn approximate_len(&self) -> usize {
        let super_version = self.tree_config().version_set_provider.latest_version();

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
        let version = self.tree_config().version_set_provider.latest_version();

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
        self.tree_config()
            .version_set_provider
            .get_version_for_seqno(seqno)
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

    #[doc(hidden)]
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
        let super_version = self.get_version_for_snapshot(seqno);

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
        self.tree_config()
            .version_set_provider
            .latest_version()
            .active_memtable
            .insert(value)
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
        use crate::file::{fsync_directory, TABLES_FOLDER};
        use crate::stop_signal::StopSignal;
        use inner::get_next_tree_id;

        log::info!("Opening LSM-tree at {}", config.path.display());

        // Check for old version
        if config.path.join("version").try_exists()? {
            log::error!("It looks like you are trying to open a V1 database - the database needs a manual migration, however a migration tool is not provided, as V1 is extremely outdated.");
            return Err(crate::Error::InvalidVersion(FormatVersion::V1.into()));
        }

        let tree_id = get_next_tree_id();

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let next_table_id: TableId = config
            .version_set_provider
            .latest_version()
            .version
            .iter_tables()
            .map(Table::id)
            .max()
            .map(|max| max + 1)
            .unwrap_or_default();

        let mut need_sync = false;

        let table_base_folder = config.path.join(TABLES_FOLDER);
        if !table_base_folder.try_exists()? {
            std::fs::create_dir_all(&table_base_folder)?;
            fsync_directory(&table_base_folder)?;
            need_sync = true;
        }

        let blobs_base_folder = config.path.join(BLOBS_FOLDER);
        if !blobs_base_folder.try_exists()? {
            std::fs::create_dir_all(&blobs_base_folder)?;
            fsync_directory(&blobs_base_folder)?;
            need_sync = true;
        }

        if need_sync {
            fsync_directory(&config.path)?;
        }

        // Check for < 1 MB shards, which may not be able to
        // hold filter blocks, causing high read latencies
        if config.cache.shard_capacity() < 1_000_000 {
            // TODO: we probably really need a documentation page that explains stuff like this
            // TODO: and documents how to tune this better... "the fjall book"...
            log::warn!(
                "Cache shard capacity is less than 1 MB, which may cause non-partitioned filter blocks to be uncacheable, greatly read latencies. Consider increasing the cache size.",
            );
        }

        let inner = TreeInner {
            id: tree_id,
            memtable_id_counter: SequenceNumberCounter::new(1),
            table_id_counter: SequenceNumberCounter::new(next_table_id),
            blob_file_id_counter: SequenceNumberCounter::default(),
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
}
