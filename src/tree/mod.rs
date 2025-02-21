// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod inner;

use crate::{
    coding::{Decode, Encode},
    compaction::{stream::CompactionStream, CompactionStrategy},
    config::Config,
    descriptor_table::FileDescriptorTable,
    level_manifest::LevelManifest,
    manifest::Manifest,
    memtable::Memtable,
    range::{prefix_to_range, MemtableLockGuard, TreeIter},
    segment::{
        block_index::{full_index::FullBlockIndex, BlockIndexImpl},
        meta::TableType,
        Segment, SegmentInner,
    },
    stop_signal::StopSignal,
    value::InternalValue,
    version::Version,
    AbstractTree, BlockCache, KvPair, SegmentId, SeqNo, Snapshot, UserKey, UserValue, ValueType,
};
use inner::{MemtableId, SealedMemtables, TreeId, TreeInner};
use std::{
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

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
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: Option<SeqNo>) -> crate::Result<Option<u32>> {
        Ok(self.get(key, seqno)?.map(|x| x.len() as u32))
    }

    fn bloom_filter_size(&self) -> usize {
        self.levels
            .read()
            .expect("lock is poisoned")
            .iter()
            .map(|x| x.bloom_filter_size())
            .sum()
    }

    fn sealed_memtable_count(&self) -> usize {
        self.sealed_memtables
            .read()
            .expect("lock is poisoned")
            .len()
    }

    fn is_first_level_disjoint(&self) -> bool {
        self.levels
            .read()
            .expect("lock is poisoned")
            .levels
            .first()
            .expect("first level should exist")
            .is_disjoint
    }

    fn verify(&self) -> crate::Result<usize> {
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
    }

    fn keys(
        &self,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static> {
        Box::new(self.create_iter(seqno, index).map(|x| x.map(|(k, _)| k)))
    }

    fn values(
        &self,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static> {
        Box::new(self.create_iter(seqno, index).map(|x| x.map(|(_, v)| v)))
    }

    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<Memtable>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<Option<Segment>> {
        use crate::{
            file::SEGMENTS_FOLDER,
            segment::writer::{Options, Writer},
        };

        let start = std::time::Instant::now();

        let folder = self.config.path.join(SEGMENTS_FOLDER);
        log::debug!("writing segment to {folder:?}");

        let mut segment_writer = Writer::new(Options {
            segment_id,
            folder,
            data_block_size: self.config.data_block_size,
            index_block_size: self.config.index_block_size,
        })?
        .use_compression(self.config.compression);

        {
            use crate::segment::writer::BloomConstructionPolicy;

            if self.config.bloom_bits_per_key >= 0 {
                segment_writer =
                    segment_writer.use_bloom_policy(BloomConstructionPolicy::FpRate(0.00001));
            } else {
                segment_writer =
                    segment_writer.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0));
            }
        }

        let iter = memtable.iter().map(Ok);
        let compaction_filter = CompactionStream::new(iter, seqno_threshold);

        for item in compaction_filter {
            segment_writer.write(item?)?;
        }

        let result = self.consume_writer(segment_id, segment_writer)?;

        log::debug!("Flushed memtable {segment_id:?} in {:?}", start.elapsed());

        Ok(result)
    }

    fn register_segments(&self, segments: &[Segment]) -> crate::Result<()> {
        // NOTE: Mind lock order L -> M -> S
        log::trace!("Acquiring levels manifest write lock");
        let mut original_levels = self.levels.write().expect("lock is poisoned");

        // NOTE: Mind lock order L -> M -> S
        log::trace!("Acquiring sealed memtables write lock");
        let mut sealed_memtables = self.sealed_memtables.write().expect("lock is poisoned");

        original_levels.atomic_swap(|recipe| {
            for segment in segments.iter().cloned() {
                recipe
                    .first_mut()
                    .expect("first level should exist")
                    .insert(segment);
            }
        })?;

        // eprintln!("{original_levels}");

        for segment in segments {
            log::trace!("releasing sealed memtable {}", segment.id());
            sealed_memtables.remove(segment.id());
        }

        Ok(())
    }

    fn lock_active_memtable(&self) -> RwLockWriteGuard<'_, Memtable> {
        self.active_memtable.write().expect("lock is poisoned")
    }

    fn set_active_memtable(&self, memtable: Memtable) {
        let mut memtable_lock = self.active_memtable.write().expect("lock is poisoned");
        *memtable_lock = memtable;
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
        use crate::compaction::worker::{do_compaction, Options};

        let mut opts = Options::from_tree(self, strategy);
        opts.eviction_seqno = seqno_threshold;
        do_compaction(&opts)?;

        log::debug!("lsm-tree: compaction run over");

        Ok(())
    }

    fn get_next_segment_id(&self) -> SegmentId {
        self.0.get_next_segment_id()
    }

    fn tree_config(&self) -> &Config {
        &self.config
    }

    fn active_memtable_size(&self) -> u32 {
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
        let yanked_memtable = Arc::new(yanked_memtable);

        let tmp_memtable_id = self.get_next_segment_id();
        sealed_memtables.add(tmp_memtable_id, yanked_memtable.clone());

        log::trace!("rotate: added memtable id={tmp_memtable_id} to sealed memtables");

        Some((tmp_memtable_id, yanked_memtable))
    }

    fn segment_count(&self) -> usize {
        self.levels.read().expect("lock is poisoned").len()
    }

    fn first_level_segment_count(&self) -> usize {
        self.levels
            .read()
            .expect("lock is poisoned")
            .first_level_segment_count()
    }

    #[allow(clippy::significant_drop_tightening)]
    fn approximate_len(&self) -> usize {
        // NOTE: Mind lock order L -> M -> S
        let levels = self.levels.read().expect("lock is poisoned");
        let memtable = self.active_memtable.read().expect("lock is poisoned");
        let sealed = self.sealed_memtables.read().expect("lock is poisoned");

        let segments_item_count = levels.iter().map(|x| x.metadata.item_count).sum::<u64>();
        let memtable_count = memtable.len() as u64;
        let sealed_count = sealed.iter().map(|(_, mt)| mt.len()).sum::<usize>() as u64;

        (memtable_count + sealed_count + segments_item_count)
            .try_into()
            .expect("should not be too large")
    }

    fn disk_space(&self) -> u64 {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.iter().map(|x| x.metadata.file_size).sum()
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
        let levels = self.levels.read().expect("lock is poisoned");
        levels
            .iter()
            .map(super::segment::Segment::get_highest_seqno)
            .max()
    }

    fn snapshot(&self, seqno: SeqNo) -> Snapshot {
        use crate::AnyTree::Standard;

        Snapshot::new(Standard(self.clone()), seqno)
    }

    fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<UserValue>> {
        Ok(self.get_internal_entry(key, seqno)?.map(|x| x.value))
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        Box::new(self.create_range(&range, seqno, index))
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: Option<SeqNo>,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        Box::new(self.create_prefix(prefix, seqno, index))
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u32, u32) {
        let value = InternalValue::from_components(key, value, seqno, ValueType::Value);
        self.append_entry(value)
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        let value = InternalValue::new_tombstone(key, seqno);
        self.append_entry(value)
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
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

        log::debug!("Opening LSM-tree at {:?}", config.path);

        // Check for old version
        if config.path.join("version").try_exists()? {
            return Err(crate::Error::InvalidVersion(Version::V1));
        }

        let tree = if config.path.join(MANIFEST_FILE).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }?;

        Ok(tree)
    }

    pub(crate) fn read_lock_active_memtable(&self) -> RwLockReadGuard<'_, Memtable> {
        self.active_memtable.read().expect("lock is poisoned")
    }

    // TODO: Expose as public function, however:
    // TODO: Right now this is somewhat unsafe to expose as
    // major compaction needs ALL segments, right now it just takes as many
    // as it can, which may make the LSM inconsistent.
    // TODO: There should also be a function to partially compact levels and individual segments

    /// Performs major compaction, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn major_compact(&self, target_size: u64, seqno_threshold: SeqNo) -> crate::Result<()> {
        log::info!("Starting major compaction");
        let strategy = Arc::new(crate::compaction::major::Strategy::new(target_size));
        self.compact(strategy, seqno_threshold)
    }

    pub(crate) fn consume_writer(
        &self,
        segment_id: SegmentId,
        mut writer: crate::segment::writer::Writer,
    ) -> crate::Result<Option<Segment>> {
        let segment_folder = writer.opts.folder.clone();
        let segment_file_path = segment_folder.join(segment_id.to_string());

        let Some(trailer) = writer.finish()? else {
            return Ok(None);
        };

        log::debug!("Finalized segment write at {segment_folder:?}");

        let block_index =
            FullBlockIndex::from_file(&segment_file_path, &trailer.metadata, &trailer.offsets)?;
        let block_index = Arc::new(BlockIndexImpl::Full(block_index));

        let created_segment: Segment = SegmentInner {
            tree_id: self.id,

            metadata: trailer.metadata,
            offsets: trailer.offsets,

            descriptor_table: self.config.descriptor_table.clone(),
            block_index,
            block_cache: self.config.block_cache.clone(),

            bloom_filter: Segment::load_bloom(&segment_file_path, trailer.offsets.bloom_ptr)?,
        }
        .into();

        self.config
            .descriptor_table
            .insert(segment_file_path, created_segment.global_id());

        log::debug!("Flushed segment to {segment_folder:?}");

        Ok(Some(created_segment))
    }

    /// Synchronously flushes the active memtable to a disk segment.
    ///
    /// The function may not return a result, if, during concurrent workloads, the memtable
    /// ends up being empty before the flush thread is set up.
    ///
    /// The result will contain the disk segment's path, relative to the tree's base path.
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
        self.register_segments(&[segment.clone()])?;

        Ok(Some(segment))
    }

    /// Returns `true` if there are some segments that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.is_compacting()
    }

    /// Write-locks the sealed memtables for exclusive access
    fn lock_sealed_memtables(&self) -> RwLockWriteGuard<'_, SealedMemtables> {
        self.sealed_memtables.write().expect("lock is poisoned")
    }

    /// Used for [`BlobTree`] lookup
    pub(crate) fn get_internal_entry_with_lock<K: AsRef<[u8]>>(
        &self,
        memtable_lock: &RwLockWriteGuard<'_, Memtable>,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<InternalValue>> {
        if let Some(entry) = memtable_lock.get(&key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        };

        // Now look in sealed memtables
        if let Some(entry) = self.get_internal_entry_from_sealed_memtables(&key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        self.get_internal_entry_from_segments(key, seqno)
    }

    fn get_internal_entry_from_sealed_memtables<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> Option<InternalValue> {
        let memtable_lock = self.sealed_memtables.read().expect("lock is poisoned");

        for (_, memtable) in memtable_lock.iter().rev() {
            if let Some(entry) = memtable.get(&key, seqno) {
                return Some(entry);
            }
        }

        None
    }

    fn get_internal_entry_from_segments<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<InternalValue>> {
        // NOTE: Create key hash for hash sharing
        // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
        let key_hash = crate::bloom::BloomFilter::get_hash(key.as_ref());

        let level_manifest = self.levels.read().expect("lock is poisoned");

        for level in &level_manifest.levels {
            // NOTE: Based on benchmarking, binary search is only worth it with ~4 segments
            if level.len() >= 4 {
                if let Some(level) = level.as_disjoint() {
                    // TODO: unit test in disjoint level:
                    // [a:5, a:4] [a:3, b:5]
                    // ^
                    // snapshot read a:3!!!

                    if let Some(segment) = level.get_segment_containing_key(&key) {
                        let maybe_item = segment.get(&key, seqno, key_hash)?;

                        if let Some(item) = maybe_item {
                            return Ok(ignore_tombstone_value(item));
                        }
                    }

                    // NOTE: Go to next level
                    continue;
                }
            }

            // NOTE: Fallback to linear search
            for segment in &level.segments {
                let maybe_item = segment.get(&key, seqno, key_hash)?;

                if let Some(item) = maybe_item {
                    return Ok(ignore_tombstone_value(item));
                }
            }
        }

        Ok(None)
    }

    #[doc(hidden)]
    pub fn get_internal_entry<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<InternalValue>> {
        // TODO: consolidate memtable & sealed behind single RwLock

        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");

        if let Some(entry) = memtable_lock.get(&key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        };

        drop(memtable_lock);

        // Now look in sealed memtables
        if let Some(entry) = self.get_internal_entry_from_sealed_memtables(&key, seqno) {
            return Ok(ignore_tombstone_value(entry));
        }

        // Now look in segments... this may involve disk I/O
        self.get_internal_entry_from_segments(key, seqno)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn create_iter(
        &self,
        seqno: Option<SeqNo>,
        ephemeral: Option<Arc<Memtable>>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.create_range::<UserKey, _>(&.., seqno, ephemeral)
    }

    #[doc(hidden)]
    pub fn create_internal_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: &'a R,
        seqno: Option<SeqNo>,
        ephemeral: Option<Arc<Memtable>>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'static {
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
        let level_manifest_lock =
            guardian::ArcRwLockReadGuardian::take(self.levels.clone()).expect("lock is poisoned");

        let active = guardian::ArcRwLockReadGuardian::take(self.active_memtable.clone())
            .expect("lock is poisoned");

        let sealed = guardian::ArcRwLockReadGuardian::take(self.sealed_memtables.clone())
            .expect("lock is poisoned");

        TreeIter::create_range(
            MemtableLockGuard {
                active,
                sealed,
                ephemeral,
            },
            bounds,
            seqno,
            level_manifest_lock,
        )
    }

    #[doc(hidden)]
    pub fn create_range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: &'a R,
        seqno: Option<SeqNo>,
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
        &'a self,
        prefix: K,
        seqno: Option<SeqNo>,
        ephemeral: Option<Arc<Memtable>>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        let range = prefix_to_range(prefix.as_ref());
        self.create_range(&range, seqno, ephemeral)
    }

    /// Adds an item to the active memtable.
    ///
    /// Returns the added item's size and new size of the memtable.
    #[doc(hidden)]
    #[must_use]
    pub fn append_entry(&self, value: InternalValue) -> (u32, u32) {
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");
        memtable_lock.insert(value)
    }

    /// Recovers previous state, by loading the level manifest and segments.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occurred.
    fn recover(mut config: Config) -> crate::Result<Self> {
        use crate::file::MANIFEST_FILE;
        use inner::get_next_tree_id;

        log::info!("Recovering LSM-tree at {:?}", config.path);

        let bytes = std::fs::read(config.path.join(MANIFEST_FILE))?;
        let mut bytes = Cursor::new(bytes);
        let manifest = Manifest::decode_from(&mut bytes)?;

        if manifest.version != Version::V2 {
            return Err(crate::Error::InvalidVersion(manifest.version));
        }

        // IMPORTANT: Restore persisted config
        config.level_count = manifest.level_count;
        config.table_type = manifest.table_type;
        config.tree_type = manifest.tree_type;

        let tree_id = get_next_tree_id();

        let mut levels = Self::recover_levels(
            &config.path,
            tree_id,
            &config.block_cache,
            &config.descriptor_table,
        )?;
        levels.update_metadata();

        let highest_segment_id = levels.iter().map(Segment::id).max().unwrap_or_default();

        let inner = TreeInner {
            id: tree_id,
            segment_id_counter: Arc::new(AtomicU64::new(highest_segment_id + 1)),
            active_memtable: Arc::default(),
            sealed_memtables: Arc::default(),
            levels: Arc::new(RwLock::new(levels)),
            stop_signal: StopSignal::default(),
            config,
        };

        Ok(Self(Arc::new(inner)))
    }

    /// Creates a new LSM-tree in a directory.
    fn create_new(config: Config) -> crate::Result<Self> {
        use crate::file::{fsync_directory, MANIFEST_FILE, SEGMENTS_FOLDER};
        use std::fs::{create_dir_all, File};

        let path = config.path.clone();
        log::trace!("Creating LSM-tree at {path:?}");

        create_dir_all(&path)?;

        let manifest_path = path.join(MANIFEST_FILE);
        assert!(!manifest_path.try_exists()?);

        let segment_folder_path = path.join(SEGMENTS_FOLDER);
        create_dir_all(&segment_folder_path)?;

        // NOTE: Lastly, fsync version marker, which contains the version
        // -> the LSM is fully initialized
        let mut file = File::create(manifest_path)?;
        Manifest {
            version: Version::V2,
            level_count: config.level_count,
            tree_type: config.tree_type,
            table_type: TableType::Block,
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
        block_cache: &Arc<BlockCache>,
        descriptor_table: &Arc<FileDescriptorTable>,
    ) -> crate::Result<LevelManifest> {
        use crate::{
            file::fsync_directory,
            file::{LEVELS_MANIFEST_FILE, SEGMENTS_FOLDER},
            SegmentId,
        };

        let tree_path = tree_path.as_ref();

        let level_manifest_path = tree_path.join(LEVELS_MANIFEST_FILE);
        log::info!("Recovering manifest at {level_manifest_path:?}");

        let segment_id_map = LevelManifest::recover_ids(&level_manifest_path)?;
        let cnt = segment_id_map.len();

        log::debug!("Recovering {cnt} disk segments from {tree_path:?}");

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

            if file_name == ".DS_Store" {
                continue;
            }

            let segment_file_name = file_name.to_str().ok_or_else(|| {
                log::error!("invalid segment file name {file_name:?}");
                crate::Error::Unrecoverable
            })?;

            let segment_file_path = dirent.path();
            assert!(!segment_file_path.is_dir());

            if segment_file_name.starts_with("tmp_") {
                log::debug!("Deleting unfinished segment: {segment_file_path:?}",);
                std::fs::remove_file(&segment_file_path)?;
                continue;
            }

            log::debug!("Recovering segment from {segment_file_path:?}");

            let segment_id = segment_file_name.parse::<SegmentId>().map_err(|e| {
                log::error!("invalid segment file name {segment_file_name:?}: {e:?}");
                crate::Error::Unrecoverable
            })?;

            if let Some(&level_idx) = segment_id_map.get(&segment_id) {
                let segment = Segment::recover(
                    &segment_file_path,
                    tree_id,
                    block_cache.clone(),
                    descriptor_table.clone(),
                    level_idx == 0 || level_idx == 1,
                )?;

                descriptor_table.insert(&segment_file_path, segment.global_id());

                segments.push(segment);
                log::debug!("Recovered segment from {segment_file_path:?}");

                if idx % progress_mod == 0 {
                    log::debug!("Recovered {idx}/{cnt} disk segments");
                }
            } else {
                log::debug!("Deleting unfinished segment: {segment_file_path:?}",);
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

        LevelManifest::recover(&level_manifest_path, segments)
    }
}
