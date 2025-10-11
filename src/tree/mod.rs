// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod ingest;
pub mod inner;
mod sealed;

use crate::{
    blob_tree::FragmentationMap,
    coding::{Decode, Encode},
    compaction::{drop_range::OwnedBounds, state::CompactionState, CompactionStrategy},
    config::Config,
    file::BLOBS_FOLDER,
    format_version::FormatVersion,
    iter_guard::{IterGuard, IterGuardImpl},
    manifest::Manifest,
    memtable::Memtable,
    segment::Segment,
    slice::Slice,
    tree::inner::SuperVersion,
    value::InternalValue,
    version::{recovery::recover_ids, Version, VersionId},
    vlog::BlobFile,
    AbstractTree, Cache, DescriptorTable, KvPair, SegmentId, SeqNo, SequenceNumberCounter,
    TreeType, UserKey, UserValue, ValueType,
};
use inner::{MemtableId, TreeId, TreeInner};
use std::{
    io::Cursor,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

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
    fn next_table_id(&self) -> SegmentId {
        self.0
            .segment_id_counter
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn id(&self) -> TreeId {
        self.id
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        #[allow(clippy::significant_drop_tightening)]
        let version_lock = self.super_version.read().expect("lock is poisoned");

        if let Some(entry) = version_lock.active_memtable.get(key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in sealed memtables
        if let Some(entry) =
            self.get_internal_entry_from_sealed_memtables(&version_lock, key, seqno)
        {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in segments... this may involve disk I/O
        self.get_internal_entry_from_segments(&version_lock, key, seqno)
    }

    fn current_version(&self) -> Version {
        self.super_version.read().expect("poisoned").version.clone()
    }

    fn flush_active_memtable(&self, seqno_threshold: SeqNo) -> crate::Result<Option<Segment>> {
        log::debug!("Flushing active memtable");

        let Some((segment_id, yanked_memtable)) = self.rotate_memtable() else {
            return Ok(None);
        };

        let Some((segment, _)) =
            self.flush_memtable(segment_id, &yanked_memtable, seqno_threshold)?
        else {
            return Ok(None);
        };
        self.register_segments(std::slice::from_ref(&segment), None, None, seqno_threshold)?;

        Ok(Some(segment))
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        &self.0.metrics
    }

    fn version_free_list_len(&self) -> usize {
        self.compaction_state
            .lock()
            .expect("lock is poisoned")
            .version_free_list_len()
    }

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
        self.current_version()
            .iter_segments()
            .map(Segment::tombstone_count)
            .sum()
    }

    fn ingest(
        &self,
        iter: impl Iterator<Item = (UserKey, UserValue)>,
        seqno_generator: &SequenceNumberCounter,
        visible_seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        use crate::tree::ingest::Ingestion;
        use std::time::Instant;

        // // TODO: 3.0.0 ... hmmmm
        // let global_lock = self.super_version.write().expect("lock is poisoned");

        let seqno = seqno_generator.next();

        // TODO: allow ingestion always, by flushing memtable
        // assert!(
        //     global_lock.active_memtable.is_empty(),
        //     "can only perform bulk ingestion with empty memtable(s)",
        // );
        // assert!(
        //     global_lock.sealed_memtables.len() == 0,
        //     "can only perform bulk ingestion with empty memtable(s)",
        // );

        let mut writer = Ingestion::new(self)?.with_seqno(seqno);

        let start = Instant::now();
        let mut count = 0;
        let mut last_key = None;

        #[allow(clippy::explicit_counter_loop)]
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

        visible_seqno.fetch_max(seqno + 1);

        log::info!("Ingested {count} items in {:?}", start.elapsed());

        Ok(())
    }

    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()> {
        let (bounds, is_empty) = Self::range_bounds_to_owned_bounds(&range);

        if is_empty {
            return Ok(());
        }

        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(bounds));

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
        self.current_version()
            .level(0)
            .map(|x| x.run_count())
            .unwrap_or_default()
    }

    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        // NOTE: We know that values are u32 max
        #[allow(clippy::cast_possible_truncation)]
        Ok(self.get(key, seqno)?.map(|x| x.len() as u32))
    }

    fn filter_size(&self) -> usize {
        self.current_version()
            .iter_segments()
            .map(Segment::filter_size)
            .sum()
    }

    fn pinned_filter_size(&self) -> usize {
        self.current_version()
            .iter_segments()
            .map(Segment::pinned_filter_size)
            .sum()
    }

    fn pinned_block_index_size(&self) -> usize {
        self.current_version()
            .iter_segments()
            .map(Segment::pinned_block_index_size)
            .sum()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.super_version
            .read()
            .expect("lock is poisoned")
            .sealed_memtables
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
    ) -> crate::Result<Option<(Segment, Option<BlobFile>)>> {
        use crate::{compaction::stream::CompactionStream, file::SEGMENTS_FOLDER, segment::Writer};
        use std::time::Instant;

        let start = Instant::now();

        let folder = self.config.path.join(SEGMENTS_FOLDER);
        let segment_file_path = folder.join(segment_id.to_string());

        let data_block_size = self.config.data_block_size_policy.get(0);
        let index_block_size = self.config.index_block_size_policy.get(0);

        let data_block_restart_interval = self.config.data_block_restart_interval_policy.get(0);
        let index_block_restart_interval = self.config.index_block_restart_interval_policy.get(0);

        let data_block_compression = self.config.data_block_compression_policy.get(0);
        let index_block_compression = self.config.index_block_compression_policy.get(0);

        let data_block_hash_ratio = self.config.data_block_hash_ratio_policy.get(0);

        log::debug!(
            "Flushing segment to {}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, index_block_size={index_block_size}, data_block_compression={data_block_compression}, index_block_compression={index_block_compression}",
            segment_file_path.display(),
        );

        let mut segment_writer = Writer::new(segment_file_path, segment_id)?
            .use_data_block_restart_interval(data_block_restart_interval)
            .use_index_block_restart_interval(index_block_restart_interval)
            .use_data_block_compression(data_block_compression)
            .use_index_block_compression(index_block_compression)
            .use_data_block_size(data_block_size)
            .use_index_block_size(index_block_size)
            .use_data_block_hash_ratio(data_block_hash_ratio)
            .use_bloom_policy({
                use crate::config::FilterPolicyEntry::{Bloom, None};
                use crate::segment::filter::BloomConstructionPolicy;

                match self.config.filter_policy.get(0) {
                    Bloom(policy) => policy,
                    None => BloomConstructionPolicy::BitsPerKey(0.0),
                }
            });

        let iter = memtable.iter().map(Ok);
        let compaction_filter = CompactionStream::new(iter, seqno_threshold);

        for item in compaction_filter {
            segment_writer.write(item?)?;
        }

        let result = self.consume_writer(segment_writer)?;

        log::debug!("Flushed memtable {segment_id:?} in {:?}", start.elapsed());

        Ok(result.map(|segment| (segment, None)))
    }

    fn register_segments(
        &self,
        segments: &[Segment],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<FragmentationMap>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()> {
        log::trace!(
            "Registering {} segments, {} blob files",
            segments.len(),
            blob_files.map(<[BlobFile]>::len).unwrap_or_default(),
        );

        let mut compaction_state = self.compaction_state.lock().expect("lock is poisoned");
        let mut super_version = self.super_version.write().expect("lock is poisoned");

        compaction_state.upgrade_version(
            &mut super_version,
            |version| {
                Ok(version.with_new_l0_run(
                    segments,
                    blob_files,
                    frag_map.filter(|x| !x.is_empty()),
                ))
            },
            seqno_threshold,
        )?;

        for segment in segments {
            log::trace!("releasing sealed memtable {}", segment.id());

            super_version.sealed_memtables =
                Arc::new(super_version.sealed_memtables.remove(segment.id()));
        }

        Ok(())
    }

    fn clear_active_memtable(&self) {
        self.super_version
            .write()
            .expect("lock is poisoned")
            .active_memtable = Arc::new(Memtable::default());
    }

    fn set_active_memtable(&self, memtable: Memtable) {
        let mut version_lock = self.super_version.write().expect("lock is poisoned");
        version_lock.active_memtable = Arc::new(memtable);
    }

    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<Memtable>) {
        let mut version_lock = self.super_version.write().expect("lock is poisoned");
        version_lock.sealed_memtables = Arc::new(version_lock.sealed_memtables.add(id, memtable));
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

        self.super_version
            .read()
            .expect("lock is poisoned")
            .active_memtable
            .approximate_size
            .load(Acquire)
    }

    fn tree_type(&self) -> crate::TreeType {
        crate::TreeType::Standard
    }

    fn rotate_memtable(&self) -> Option<(MemtableId, Arc<Memtable>)> {
        let mut version_lock = self.super_version.write().expect("lock is poisoned");

        if version_lock.active_memtable.is_empty() {
            return None;
        }

        let yanked_memtable = std::mem::take(&mut version_lock.active_memtable);
        let yanked_memtable = yanked_memtable;

        let tmp_memtable_id = self.get_next_segment_id();

        version_lock.sealed_memtables = Arc::new(
            version_lock
                .sealed_memtables
                .add(tmp_memtable_id, yanked_memtable.clone()),
        );

        log::trace!("rotate: added memtable id={tmp_memtable_id} to sealed memtables");

        Some((tmp_memtable_id, yanked_memtable))
    }

    fn segment_count(&self) -> usize {
        self.current_version().segment_count()
    }

    fn level_segment_count(&self, idx: usize) -> Option<usize> {
        self.current_version().level(idx).map(|x| x.segment_count())
    }

    #[allow(clippy::significant_drop_tightening)]
    fn approximate_len(&self) -> usize {
        let version = self.super_version.read().expect("lock is poisoned");

        let segments_item_count = self
            .current_version()
            .iter_segments()
            .map(|x| x.metadata.item_count)
            .sum::<u64>();

        let memtable_count = version.active_memtable.len() as u64;
        let sealed_count = version
            .sealed_memtables
            .iter()
            .map(|(_, mt)| mt.len())
            .sum::<usize>() as u64;

        (memtable_count + sealed_count + segments_item_count)
            .try_into()
            .expect("should not be too large")
    }

    fn disk_space(&self) -> u64 {
        self.current_version()
            .iter_levels()
            .map(super::version::Level::size)
            .sum()
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        let version = self.super_version.read().expect("lock is poisoned");

        let active = version.active_memtable.get_highest_seqno();

        let sealed = version
            .sealed_memtables
            .iter()
            .map(|(_, table)| table.get_highest_seqno())
            .max()
            .flatten();

        active.max(sealed)
    }

    fn get_highest_persisted_seqno(&self) -> Option<SeqNo> {
        self.current_version()
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

    pub(crate) fn consume_writer(
        &self,
        writer: crate::segment::Writer,
    ) -> crate::Result<Option<Segment>> {
        let segment_file_path = writer.path.clone();

        let Some(_) = writer.finish()? else {
            return Ok(None);
        };

        log::debug!("Finalized segment write at {}", segment_file_path.display());

        let pin_filter = self.config.filter_block_pinning_policy.get(0);
        let pin_index = self.config.filter_block_pinning_policy.get(0);

        let created_segment = Segment::recover(
            segment_file_path,
            self.id,
            self.config.cache.clone(),
            self.config.descriptor_table.clone(),
            pin_filter,
            pin_index,
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        )?;

        log::debug!("Flushed segment to {:?}", created_segment.path);

        Ok(Some(created_segment))
    }

    /// Returns `true` if there are some segments that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        !self
            .compaction_state
            .lock()
            .expect("lock is poisoned")
            .hidden_set()
            .is_empty()
    }

    fn get_internal_entry_from_sealed_memtables(
        &self,
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
    ) -> Option<InternalValue> {
        for (_, memtable) in super_version.sealed_memtables.iter().rev() {
            if let Some(entry) = memtable.get(key, seqno) {
                return Some(entry);
            }
        }

        None
    }

    fn get_internal_entry_from_segments(
        &self,
        super_version: &SuperVersion,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        // NOTE: Create key hash for hash sharing
        // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
        let key_hash = crate::segment::filter::standard_bloom::Builder::get_hash(key);

        for level in super_version.version.iter_levels() {
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

        let super_version = self.super_version.write().expect("lock is poisoned");

        let iter_state = {
            let active = &super_version.active_memtable;
            let sealed = &super_version.sealed_memtables;

            IterState {
                active: active.clone(),
                sealed: sealed.iter().map(|(_, mt)| mt.clone()).collect(),
                ephemeral,
                version: super_version.version.clone(),
            }
        };

        TreeIter::create_range(iter_state, bounds, seqno, &super_version.version)
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
        self.super_version
            .read()
            .expect("lock is poisoned")
            .active_memtable
            .insert(value)
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

        let tree_id = get_next_tree_id();

        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let version = Self::recover_levels(
            &config.path,
            tree_id,
            &config.cache,
            &config.descriptor_table,
            #[cfg(feature = "metrics")]
            &metrics,
        )?;

        let highest_segment_id = version
            .iter_segments()
            .map(Segment::id)
            .max()
            .unwrap_or_default();

        let path = config.path.clone();

        let inner = TreeInner {
            id: tree_id,
            segment_id_counter: Arc::new(AtomicU64::new(highest_segment_id + 1)),
            blob_file_id_generator: SequenceNumberCounter::default(),
            super_version: Arc::new(RwLock::new(SuperVersion {
                active_memtable: Arc::default(),
                sealed_memtables: Arc::default(),
                version,
            })),
            stop_signal: StopSignal::default(),
            config,
            major_compaction_lock: RwLock::default(),
            compaction_state: Arc::new(Mutex::new(CompactionState::new(path))),

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
            tree_type: if config.kv_separation_opts.is_some() {
                TreeType::Blob
            } else {
                TreeType::Standard
            },
        }
        .encode_into(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&segment_folder_path)?;
        fsync_directory(&path)?;

        let inner = TreeInner::create_new(config)?;
        Ok(Self(Arc::new(inner)))
    }

    /// Recovers the level manifest, loading all tables from disk.
    fn recover_levels<P: AsRef<Path>>(
        tree_path: P,
        tree_id: TreeId,
        cache: &Arc<Cache>,
        descriptor_table: &Arc<DescriptorTable>,
        #[cfg(feature = "metrics")] metrics: &Arc<Metrics>,
    ) -> crate::Result<Version> {
        use crate::{file::fsync_directory, file::SEGMENTS_FOLDER, SegmentId};

        let tree_path = tree_path.as_ref();

        let recovery = recover_ids(tree_path)?;

        let table_id_map = {
            let mut result: crate::HashMap<SegmentId, u8 /* Level index */> =
                crate::HashMap::default();

            for (level_idx, table_ids) in recovery.segment_ids.iter().enumerate() {
                for run in table_ids {
                    for table_id in run {
                        // NOTE: We know there are always less than 256 levels
                        #[allow(clippy::expect_used)]
                        result.insert(
                            *table_id,
                            level_idx
                                .try_into()
                                .expect("there are less than 256 levels"),
                        );
                    }
                }
            }

            result
        };

        let cnt = table_id_map.len();

        log::debug!("Recovering {cnt} tables from {}", tree_path.display());

        let progress_mod = match cnt {
            _ if cnt <= 20 => 1,
            _ if cnt <= 100 => 10,
            _ => 100,
        };

        let mut tables = vec![];

        let table_base_folder = tree_path.join(SEGMENTS_FOLDER);

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

            let table_id = table_file_name.parse::<SegmentId>().map_err(|e| {
                log::error!("invalid table file name {table_file_name:?}: {e:?}");
                crate::Error::Unrecoverable
            })?;

            if let Some(&level_idx) = table_id_map.get(&table_id) {
                let table = Segment::recover(
                    table_file_path,
                    tree_id,
                    cache.clone(),
                    descriptor_table.clone(),
                    level_idx <= 1, // TODO: look at configuration
                    level_idx <= 2, // TODO: look at configuration
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
                table_id_map.keys(),
            );
            return Err(crate::Error::Unrecoverable);
        }

        log::debug!("Successfully recovered {} tables", tables.len());

        let blob_files = crate::vlog::recover_blob_files(
            &tree_path.join(BLOBS_FOLDER),
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

        // TODO: remove orphaned blob files as well -> unit test

        Ok(version)
    }

    fn cleanup_orphaned_version(path: &Path, latest_version_id: VersionId) -> crate::Result<()> {
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
