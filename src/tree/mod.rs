// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod ingest;
pub mod inner;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use crate::{
    coding::{Decode, Encode},
    compaction::CompactionStrategy,
    config::Config,
    format_version::FormatVersion,
    iter_guard::{IterGuard, IterGuardImpl},
    level_manifest::LevelManifest,
    manifest::Manifest,
    memtable::Memtable,
    segment::Segment,
    value::InternalValue,
    AbstractTree, Cache, DescriptorTable, KvPair, SegmentId, SeqNo, UserKey, UserValue, ValueType,
};
use inner::{MemtableId, SealedMemtables, TreeId, TreeInner};
use std::{
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

pub struct Guard(crate::Result<(UserKey, UserValue)>);

impl IterGuard for Guard {
    fn key(self) -> crate::Result<UserKey> {
        self.0.map(|(k, _)| k)
    }

    fn size(self) -> crate::Result<u32> {
        // NOTE: We know LSM-tree values are 32 bits in length max
        #[allow(clippy::cast_possible_truncation)]
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
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_> {
        Box::new(
            self.create_prefix(&prefix, seqno, index)
                .map(|kv| IterGuardImpl::Standard(Guard(kv))),
        )
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_> {
        Box::new(
            self.create_range(&range, seqno, index)
                .map(|kv| IterGuardImpl::Standard(Guard(kv))),
        )
    }

    // TODO: doctest
    fn tombstone_count(&self) -> u64 {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .iter_segments()
            .map(Segment::tombstone_count)
            .sum()
    }

    fn ingest(&self, iter: impl Iterator<Item = (UserKey, UserValue)>) -> crate::Result<()> {
        use crate::tree::ingest::Ingestion;
        use std::time::Instant;

        // NOTE: Lock active memtable so nothing else can be going on while we are bulk loading
        let lock = self.lock_active_memtable();
        assert!(
            lock.is_empty(),
            "can only perform bulk_ingest on empty trees",
        );

        let mut writer = Ingestion::new(self)?;

        let start = Instant::now();
        let mut count = 0;
        let mut last_key = None;

        for (key, value) in iter {
            if let Some(last_key) = &last_key {
                assert!(
                    key > last_key,
                    "next key in bulk ingest was not greater than last key",
                );
            }
            last_key = Some(key.clone());

            writer.write(key, value)?;

            count += 1;
        }

        writer.finish()?;

        log::info!("Ingested {count} items in {:?}", start.elapsed());

        Ok(())
    }

    // TODO: change API to RangeBounds<K>
    fn drop_range(&self, key_range: crate::KeyRange) -> crate::Result<()> {
        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(key_range));

        // IMPORTANT: Write lock so we can be the only compaction going on
        let _lock = self
            .0
            .major_compaction_lock
            .write()
            .expect("lock is poisoned");

        log::info!("Starting drop_range compaction");
        self.inner_compact(strategy, 0)
    }

    #[doc(hidden)]
    fn major_compact(&self, target_size: u64, seqno_threshold: SeqNo) -> crate::Result<()> {
        let strategy = Arc::new(crate::compaction::major::Strategy::new(target_size));

        // IMPORTANT: Write lock so we can be the only compaction going on
        let _lock = self
            .0
            .major_compaction_lock
            .write()
            .expect("lock is poisoned");

        log::info!("Starting major compaction");
        self.inner_compact(strategy, seqno_threshold)
    }

    fn l0_run_count(&self) -> usize {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .level(0)
            .map(|x| x.run_count())
            .unwrap_or_default()
    }

    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        // NOTE: We know that values are u32 max
        #[allow(clippy::cast_possible_truncation)]
        Ok(self.get(key, seqno)?.map(|x| x.len() as u32))
    }

    fn pinned_filter_size(&self) -> usize {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .iter_segments()
            .map(Segment::pinned_filter_size)
            .sum()
    }

    fn pinned_block_index_size(&self) -> usize {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .iter_segments()
            .map(Segment::pinned_block_index_size)
            .sum()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.sealed_memtables
            .read()
            .expect("lock is poisoned")
            .len()
    }

    /* fn verify(&self) -> crate::Result<usize> {
        // NOTE: Lock memtable to prevent any tampering with disk segments
        let _lock = self.lock_active_memtable();

        let mut sum = 0;

        let level_manifest = self.levels.read().expect("lock is poisoned");

        for level in &level_manifest.levels {
            for segment in &level.segments {
                sum += segment.verify()?;
            }
        }

        Ok(sum)
    } */

    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<Memtable>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<Option<Segment>> {
        use crate::{compaction::stream::CompactionStream, file::SEGMENTS_FOLDER, segment::Writer};
        use std::time::Instant;

        let start = Instant::now();

        let folder = self.config.path.join(SEGMENTS_FOLDER);
        let segment_file_path = folder.join(segment_id.to_string());
        log::debug!("writing segment to {}", segment_file_path.display());

        let mut segment_writer = Writer::new(segment_file_path, segment_id)?
            .use_data_block_compression(self.config.compression)
            .use_data_block_size(self.config.data_block_size)
            .use_data_block_hash_ratio(self.config.data_block_hash_ratio)
            .use_bloom_policy({
                use crate::segment::filter::BloomConstructionPolicy;

                if self.config.bloom_bits_per_key >= 0 {
                    // TODO: enable monkey later on
                    // BloomConstructionPolicy::FpRate(0.00001)
                    BloomConstructionPolicy::BitsPerKey(
                        self.config.bloom_bits_per_key.unsigned_abs(),
                    )
                } else {
                    BloomConstructionPolicy::BitsPerKey(0)
                }
            });

        if let Some(ref extractor) = self.config.prefix_extractor {
            segment_writer = segment_writer.use_prefix_extractor(extractor.clone());
        }

        let iter = memtable.iter().map(Ok);
        let compaction_filter = CompactionStream::new(iter, seqno_threshold);

        for item in compaction_filter {
            segment_writer.write(item?)?;
        }

        let result = self.consume_writer(segment_writer)?;

        log::debug!("Flushed memtable {segment_id:?} in {:?}", start.elapsed());

        Ok(result)
    }

    fn register_segments(&self, segments: &[Segment], seqno_threshold: SeqNo) -> crate::Result<()> {
        log::trace!("Registering {} segments", segments.len());

        // NOTE: Mind lock order L -> M -> S
        log::trace!("register: Acquiring levels manifest write lock");
        let mut manifest = self.manifest.write().expect("lock is poisoned");
        log::trace!("register: Acquired levels manifest write lock");

        // NOTE: Mind lock order L -> M -> S
        log::trace!("register: Acquiring sealed memtables write lock");
        let mut sealed_memtables = self.sealed_memtables.write().expect("lock is poisoned");
        log::trace!("register: Acquired sealed memtables write lock");

        manifest.atomic_swap(|version| version.with_new_l0_run(segments), seqno_threshold)?;

        // eprintln!("{manifest}");

        for segment in segments {
            log::trace!("releasing sealed memtable {}", segment.id());
            sealed_memtables.remove(segment.id());
        }

        Ok(())
    }

    fn lock_active_memtable(&self) -> RwLockWriteGuard<'_, Arc<Memtable>> {
        self.active_memtable.write().expect("lock is poisoned")
    }

    fn clear_active_memtable(&self) {
        *self.active_memtable.write().expect("lock is poisoned") = Arc::new(Memtable::default());
    }

    fn set_active_memtable(&self, memtable: Memtable) {
        let mut memtable_lock = self.active_memtable.write().expect("lock is poisoned");
        *memtable_lock = Arc::new(memtable);
    }

    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<Memtable>) {
        let mut memtable_lock = self.sealed_memtables.write().expect("lock is poisoned");
        memtable_lock.add(id, memtable);
    }

    fn compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        // NOTE: Read lock major compaction lock
        // That way, if a major compaction is running, we cannot proceed
        // But in general, parallel (non-major) compactions can occur
        let _lock = self
            .0
            .major_compaction_lock
            .read()
            .expect("lock is poisoned");

        self.inner_compact(strategy, seqno_threshold)
    }

    fn get_next_segment_id(&self) -> SegmentId {
        self.0.get_next_segment_id()
    }

    fn tree_config(&self) -> &Config {
        &self.config
    }

    fn active_memtable_size(&self) -> u64 {
        use std::sync::atomic::Ordering::Acquire;

        self.active_memtable
            .read()
            .expect("lock is poisoned")
            .approximate_size
            .load(Acquire)
    }

    fn tree_type(&self) -> crate::TreeType {
        crate::TreeType::Standard
    }

    fn rotate_memtable(&self) -> Option<(MemtableId, Arc<Memtable>)> {
        log::trace!("rotate: acquiring active memtable write lock");
        let mut active_memtable = self.lock_active_memtable();

        log::trace!("rotate: acquiring sealed memtables write lock");
        let mut sealed_memtables = self.lock_sealed_memtables();

        if active_memtable.is_empty() {
            return None;
        }

        let yanked_memtable = std::mem::take(&mut *active_memtable);
        let yanked_memtable = yanked_memtable;

        let tmp_memtable_id = self.get_next_segment_id();
        sealed_memtables.add(tmp_memtable_id, yanked_memtable.clone());

        log::trace!("rotate: added memtable id={tmp_memtable_id} to sealed memtables");

        Some((tmp_memtable_id, yanked_memtable))
    }

    fn segment_count(&self) -> usize {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .segment_count()
    }

    fn level_segment_count(&self, idx: usize) -> Option<usize> {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .level(idx)
            .map(|x| x.segment_count())
    }

    #[allow(clippy::significant_drop_tightening)]
    fn approximate_len(&self) -> usize {
        // NOTE: Mind lock order L -> M -> S
        let manifest = self.manifest.read().expect("lock is poisoned");
        let memtable = self.active_memtable.read().expect("lock is poisoned");
        let sealed = self.sealed_memtables.read().expect("lock is poisoned");

        let segments_item_count = manifest
            .current_version()
            .iter_segments()
            .map(|x| x.metadata.item_count)
            .sum::<u64>();

        let memtable_count = memtable.len() as u64;
        let sealed_count = sealed.iter().map(|(_, mt)| mt.len()).sum::<usize>() as u64;

        (memtable_count + sealed_count + segments_item_count)
            .try_into()
            .expect("should not be too large")
    }

    fn disk_space(&self) -> u64 {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .iter_levels()
            .map(|x| x.size())
            .sum()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        let active = self
            .active_memtable
            .read()
            .expect("lock is poisoned")
            .get_highest_seqno();

        let sealed = self
            .sealed_memtables
            .read()
            .expect("Lock is poisoned")
            .iter()
            .map(|(_, table)| table.get_highest_seqno())
            .max()
            .flatten();

        active.max(sealed)
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .current_version()
            .iter_segments()
            .map(Segment::get_highest_seqno)
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
        use crate::file::MANIFEST_FILE;

        log::debug!("Opening LSM-tree at {}", config.path.display());

        // Check for old version
        if config.path.join("version").try_exists()? {
            return Err(crate::Error::InvalidVersion(FormatVersion::V1));
        }

        let tree = if config.path.join(MANIFEST_FILE).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }?;

        Ok(tree)
    }

    pub(crate) fn read_lock_active_memtable(&self) -> RwLockReadGuard<'_, Arc<Memtable>> {
        self.active_memtable.read().expect("lock is poisoned")
    }

    pub(crate) fn consume_writer(
        &self,
        writer: crate::segment::Writer,
    ) -> crate::Result<Option<Segment>> {
        let segment_file_path = writer.path.to_path_buf();

        let Some(_) = writer.finish()? else {
            return Ok(None);
        };

        log::debug!("Finalized segment write at {}", segment_file_path.display());

        /* let block_index =
            FullBlockIndex::from_file(&segment_file_path, &trailer.metadata, &trailer.offsets)?;
        let block_index = Arc::new(BlockIndexImpl::Full(block_index));

        let created_segment: Segment = SegmentInner {
            path: segment_file_path.clone(),

            tree_id: self.id,

            metadata: trailer.metadata,
            offsets: trailer.offsets,

            descriptor_table: self.config.descriptor_table.clone(),
            block_index,
            cache: self.config.cache.clone(),

            bloom_filter: Segment::load_bloom(&segment_file_path, trailer.offsets.bloom_ptr)?,

            is_deleted: AtomicBool::default(),
        }
        .into(); */

        /* self.config
        .descriptor_table
        .insert(segment_file_path, created_segment.global_id()); */

        let created_segment = Segment::recover(
            segment_file_path,
            self.id,
            self.config.cache.clone(),
            self.config.descriptor_table.clone(),
            self.config.prefix_extractor.clone(),
            true, // TODO: look at configuration
            true, // TODO: look at configuration
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        )?;

        log::debug!("Flushed segment to {:?}", created_segment.path);

        Ok(Some(created_segment))
    }

    /// Synchronously flushes the active memtable to a disk segment.
    ///
    /// The function may not return a result, if, during concurrent workloads, the memtable
    /// ends up being empty before the flush is set up.
    ///
    /// The result will contain the [`Segment`].
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn flush_active_memtable(&self, seqno_threshold: SeqNo) -> crate::Result<Option<Segment>> {
        log::debug!("Flushing active memtable");

        let Some((segment_id, yanked_memtable)) = self.rotate_memtable() else {
            return Ok(None);
        };

        let Some(segment) = self.flush_memtable(segment_id, &yanked_memtable, seqno_threshold)?
        else {
            return Ok(None);
        };
        self.register_segments(std::slice::from_ref(&segment), seqno_threshold)?;

        Ok(Some(segment))
    }

    /// Returns `true` if there are some segments that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        self.manifest
            .read()
            .expect("lock is poisoned")
            .is_compacting()
    }

    /// Write-locks the sealed memtables for exclusive access
    fn lock_sealed_memtables(&self) -> RwLockWriteGuard<'_, SealedMemtables> {
        self.sealed_memtables.write().expect("lock is poisoned")
    }

    // TODO: maybe not needed anyway
    /// Used for [`BlobTree`] lookup
    pub(crate) fn get_internal_entry_with_memtable(
        &self,
        memtable_lock: &Memtable,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        if let Some(entry) = memtable_lock.get(key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in sealed memtables
        if let Some(entry) = self.get_internal_entry_from_sealed_memtables(key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        self.get_internal_entry_from_segments(key, seqno)
    }

    fn get_internal_entry_from_sealed_memtables(
        &self,
        key: &[u8],
        seqno: SeqNo,
    ) -> Option<InternalValue> {
        let memtable_lock = self.sealed_memtables.read().expect("lock is poisoned");

        for (_, memtable) in memtable_lock.iter().rev() {
            if let Some(entry) = memtable.get(key, seqno) {
                return Some(entry);
            }
        }

        None
    }

    fn get_internal_entry_from_segments(
        &self,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        // NOTE: Create key hash for hash sharing
        // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
        let key_hash = crate::segment::filter::standard_bloom::Builder::get_hash(key);

        let manifest = self.manifest.read().expect("lock is poisoned");

        for level in manifest.current_version().iter_levels() {
            for run in level.iter() {
                // NOTE: Based on benchmarking, binary search is only worth it with ~4 segments
                if run.len() >= 4 {
                    if let Some(segment) = run.get_for_key(key) {
                        if let Some(item) = segment.get(key, seqno, key_hash)? {
                            return Ok(ignore_tombstone_value(item));
                        }
                    }
                } else {
                    // NOTE: Fallback to linear search
                    for segment in run.iter() {
                        if !segment.is_key_in_key_range(key) {
                            continue;
                        }

                        if let Some(item) = segment.get(key, seqno, key_hash)? {
                            return Ok(ignore_tombstone_value(item));
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    #[doc(hidden)]
    pub fn get_internal_entry(
        &self,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        // TODO: consolidate memtable & sealed behind single RwLock

        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");

        if let Some(entry) = memtable_lock.get(key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        drop(memtable_lock);

        // Now look in sealed memtables
        if let Some(entry) = self.get_internal_entry_from_sealed_memtables(key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in segments... this may involve disk I/O
        self.get_internal_entry_from_segments(key, seqno)
    }

    fn inner_compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        use crate::compaction::worker::{do_compaction, Options};

        let mut opts = Options::from_tree(self, strategy);
        opts.eviction_seqno = seqno_threshold;

        do_compaction(&opts)?;

        log::debug!("Compaction run over");

        Ok(())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn create_iter(
        &self,
        seqno: SeqNo,
        ephemeral: Option<Arc<Memtable>>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.create_range::<UserKey, _>(&.., seqno, ephemeral)
    }

    #[doc(hidden)]
    pub fn create_internal_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<Arc<Memtable>>,
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

        // NOTE: Mind lock order L -> M -> S
        log::trace!("range read: acquiring read locks");

        let manifest = self.manifest.read().expect("lock is poisoned");

        let iter_state = {
            let active = self.active_memtable.read().expect("lock is poisoned");
            let sealed = &self.sealed_memtables.read().expect("lock is poisoned");

            IterState {
                active: active.clone(),
                sealed: sealed.iter().map(|(_, mt)| mt.clone()).collect(),
                ephemeral,
                version: manifest.current_version().clone(),
            }
        };

        TreeIter::create_range(iter_state, bounds, seqno, &manifest)
    }

    #[doc(hidden)]
    pub fn create_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &self,
        range: &'a R,
        seqno: SeqNo,
        ephemeral: Option<Arc<Memtable>>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.create_internal_range(range, seqno, ephemeral)
            .map(|item| match item {
                Ok(kv) => Ok((kv.key.user_key, kv.value)),
                Err(e) => Err(e),
            })
    }

    #[doc(hidden)]
    pub fn create_prefix<'a, K: AsRef<[u8]> + 'a>(
        &self,
        prefix: K,
        seqno: SeqNo,
        ephemeral: Option<Arc<Memtable>>,
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
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");
        memtable_lock.insert(value)
    }

    /// Recovers previous state, by loading the level manifest and segments.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    fn recover(mut config: Config) -> crate::Result<Self> {
        use crate::{file::MANIFEST_FILE, stop_signal::StopSignal};
        use inner::get_next_tree_id;

        log::info!("Recovering LSM-tree at {}", config.path.display());

        let bytes = std::fs::read(config.path.join(MANIFEST_FILE))?;
        let mut bytes = Cursor::new(bytes);
        let manifest = Manifest::decode_from(&mut bytes)?;

        if manifest.version != FormatVersion::V3 {
            return Err(crate::Error::InvalidVersion(manifest.version));
        }

        // IMPORTANT: Restore persisted config
        config.level_count = manifest.level_count;
        // config.table_type = manifest.table_type;
        config.tree_type = manifest.tree_type;

        let tree_id = get_next_tree_id();

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let levels = Self::recover_levels(
            &config.path,
            tree_id,
            &config.cache,
            &config.descriptor_table,
            &config.prefix_extractor,
            #[cfg(feature = "metrics")]
            &metrics,
        )?;

        let highest_segment_id = levels.iter().map(Segment::id).max().unwrap_or_default();

        let inner = TreeInner {
            id: tree_id,
            segment_id_counter: Arc::new(AtomicU64::new(highest_segment_id + 1)),
            active_memtable: Arc::default(),
            sealed_memtables: Arc::default(),
            manifest: Arc::new(RwLock::new(levels)),
            stop_signal: StopSignal::default(),
            config,
            major_compaction_lock: RwLock::default(),
            #[cfg(feature = "metrics")]
            metrics,
        };

        Ok(Self(Arc::new(inner)))
    }

    /// Creates a new LSM-tree in a directory.
    fn create_new(config: Config) -> crate::Result<Self> {
        use crate::file::{fsync_directory, MANIFEST_FILE, SEGMENTS_FOLDER};
        use std::fs::{create_dir_all, File};

        let path = config.path.clone();
        log::trace!("Creating LSM-tree at {}", path.display());

        create_dir_all(&path)?;

        let manifest_path = path.join(MANIFEST_FILE);
        assert!(!manifest_path.try_exists()?);

        let segment_folder_path = path.join(SEGMENTS_FOLDER);
        create_dir_all(&segment_folder_path)?;

        // NOTE: Lastly, fsync version marker, which contains the version
        // -> the LSM is fully initialized
        let mut file = File::create_new(manifest_path)?;
        Manifest {
            version: FormatVersion::V3,
            level_count: config.level_count,
            tree_type: config.tree_type,
            // table_type: TableType::Block,
        }
        .encode_into(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&segment_folder_path)?;
        fsync_directory(&path)?;

        let inner = TreeInner::create_new(config)?;
        Ok(Self(Arc::new(inner)))
    }

    /// Recovers the level manifest, loading all segments from disk.
    fn recover_levels<P: AsRef<Path>>(
        tree_path: P,
        tree_id: TreeId,
        cache: &Arc<Cache>,
        descriptor_table: &Arc<DescriptorTable>,
        prefix_extractor: &Option<crate::prefix::SharedPrefixExtractor>,
        #[cfg(feature = "metrics")] metrics: &Arc<Metrics>,
    ) -> crate::Result<LevelManifest> {
        use crate::{file::fsync_directory, file::SEGMENTS_FOLDER, SegmentId};

        let tree_path = tree_path.as_ref();

        log::info!("Recovering manifest at {}", tree_path.display());

        let segment_id_map = LevelManifest::recover_ids(tree_path)?;
        let cnt = segment_id_map.len();

        log::debug!(
            "Recovering {cnt} disk segments from {}",
            tree_path.display(),
        );

        let progress_mod = match cnt {
            _ if cnt <= 20 => 1,
            _ if cnt <= 100 => 10,
            _ => 100,
        };

        let mut segments = vec![];

        let segment_base_folder = tree_path.join(SEGMENTS_FOLDER);

        if !segment_base_folder.try_exists()? {
            std::fs::create_dir_all(&segment_base_folder)?;
            fsync_directory(&segment_base_folder)?;
        }

        for (idx, dirent) in std::fs::read_dir(&segment_base_folder)?.enumerate() {
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

            let segment_file_name = file_name.to_str().ok_or_else(|| {
                log::error!("invalid segment file name {file_name:?}");
                crate::Error::Unrecoverable
            })?;

            let segment_file_path = dirent.path();
            assert!(!segment_file_path.is_dir());

            log::debug!("Recovering segment from {}", segment_file_path.display());

            let segment_id = segment_file_name.parse::<SegmentId>().map_err(|e| {
                log::error!("invalid segment file name {segment_file_name:?}: {e:?}");
                crate::Error::Unrecoverable
            })?;

            if let Some(&level_idx) = segment_id_map.get(&segment_id) {
                let segment = Segment::recover(
                    segment_file_path,
                    tree_id,
                    cache.clone(),
                    descriptor_table.clone(),
                    prefix_extractor.clone(),
                    level_idx <= 2, // TODO: look at configuration
                    level_idx <= 2, // TODO: look at configuration
                    #[cfg(feature = "metrics")]
                    metrics.clone(),
                )?;

                log::debug!("Recovered segment from {:?}", segment.path);

                segments.push(segment);

                if idx % progress_mod == 0 {
                    log::debug!("Recovered {idx}/{cnt} disk segments");
                }
            } else {
                log::debug!(
                    "Deleting unfinished segment: {}",
                    segment_file_path.display(),
                );
                std::fs::remove_file(&segment_file_path)?;
            }
        }

        if segments.len() < cnt {
            log::error!(
                "Recovered less segments than expected: {:?}",
                segment_id_map.keys(),
            );
            return Err(crate::Error::Unrecoverable);
        }

        log::debug!("Successfully recovered {} segments", segments.len());

        LevelManifest::recover(tree_path, &segments)
    }
}
