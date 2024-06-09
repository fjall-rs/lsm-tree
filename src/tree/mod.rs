pub mod inner;

use crate::{
    compaction::CompactionStrategy,
    config::{Config, PersistedConfig},
    descriptor_table::FileDescriptorTable,
    file::fsync_directory,
    levels::LevelManifest,
    memtable::MemTable,
    range::{MemtableLockGuard, TreeIter},
    segment::{block_index::BlockIndex, Segment},
    serde::{Deserializable, Serializable},
    stop_signal::StopSignal,
    version::Version,
    AbstractTree, BlockCache, KvPair, SegmentId, SeqNo, Snapshot, UserKey, UserValue, Value,
    ValueType,
};
use inner::{MemtableId, SealedMemtables, TreeId, TreeInner};
use std::{
    io::Cursor,
    ops::RangeBounds,
    path::Path,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockWriteGuard},
};

fn ignore_tombstone_value(item: Value) -> Option<Value> {
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
    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<MemTable>,
    ) -> crate::Result<Arc<Segment>> {
        use crate::{
            file::SEGMENTS_FOLDER,
            segment::writer::{Options, Writer},
        };

        let folder = self.config.path.join(SEGMENTS_FOLDER);
        log::debug!("writing segment to {folder:?}");

        let mut writer = Writer::new(Options {
            segment_id,
            folder,
            evict_tombstones: false,
            block_size: self.config.inner.block_size,
            compression: self.config.inner.compression,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.0001,
        })?;

        for entry in &memtable.items {
            let key = entry.key();
            let value = entry.value();
            writer.write(crate::Value::from(((key.clone()), value.clone())))?;
        }

        self.consume_writer(segment_id, writer)
    }

    fn register_segments(&self, segments: &[Arc<Segment>]) -> crate::Result<()> {
        // NOTE: Mind lock order L -> M -> S
        log::trace!("flush: acquiring levels manifest write lock");
        let mut original_levels = self.levels.write().expect("lock is poisoned");

        // NOTE: Mind lock order L -> M -> S
        log::trace!("flush: acquiring sealed memtables write lock");
        let mut sealed_memtables = self.sealed_memtables.write().expect("lock is poisoned");

        original_levels.atomic_swap(|recipe| {
            for segment in segments.iter().cloned() {
                recipe
                    .first_mut()
                    .expect("first level should exist")
                    .insert(segment);
            }
        })?;

        for segment in segments {
            sealed_memtables.remove(segment.metadata.id);
        }

        Ok(())
    }

    fn lock_active_memtable(&self) -> RwLockWriteGuard<'_, MemTable> {
        self.active_memtable.write().expect("lock is poisoned")
    }

    fn set_active_memtable(&self, memtable: MemTable) {
        let mut memtable_lock = self.active_memtable.write().expect("lock is poisoned");
        *memtable_lock = memtable;
    }

    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<MemTable>) {
        let mut memtable_lock = self.sealed_memtables.write().expect("lock is poisoned");
        memtable_lock.add(id, memtable);
    }

    fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> crate::Result<()> {
        use crate::compaction::worker::{do_compaction, Options};

        let opts = Options::from_tree(self, strategy);
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

    fn get_lsn(&self) -> Option<SeqNo> {
        let memtable_lsn = self
            .active_memtable
            .read()
            .expect("lock is poisoned")
            .get_lsn();

        let segment_lsn = self.get_segment_lsn();

        match (memtable_lsn, segment_lsn) {
            (Some(x), Some(y)) => Some(x.max(y)),
            (Some(x), None) | (None, Some(x)) => Some(x),
            (None, None) => None,
        }
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

    fn rotate_memtable(&self) -> Option<(MemtableId, Arc<MemTable>)> {
        log::trace!("rotate: acquiring active memtable write lock");
        let mut active_memtable = self.lock_active_memtable();

        if active_memtable.items.is_empty() {
            return None;
        }

        log::trace!("rotate: acquiring sealed memtables write lock");
        let mut sealed_memtables = self.lock_sealed_memtables();

        let yanked_memtable = std::mem::take(&mut *active_memtable);
        let yanked_memtable = Arc::new(yanked_memtable);

        let tmp_memtable_id = self.get_next_segment_id();
        sealed_memtables.add(tmp_memtable_id, yanked_memtable.clone());

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

    fn approximate_len(&self) -> u64 {
        // NOTE: Mind lock order L -> M -> S
        let levels = self.levels.read().expect("lock is poisoned");

        let level_iter = crate::levels::iter::LevelManifestIterator::new(&levels);
        let segments_item_count = level_iter.map(|x| x.metadata.item_count).sum::<u64>();
        drop(levels);

        let sealed_count = self
            .sealed_memtables
            .read()
            .expect("lock is poisoned")
            .iter()
            .map(|(_, mt)| mt.len())
            .sum::<usize>() as u64;

        self.active_memtable.read().expect("lock is poisoned").len() as u64
            + sealed_count
            + segments_item_count
    }

    fn disk_space(&self) -> u64 {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.iter().map(|x| x.metadata.file_size).sum()
    }

    fn get_memtable_lsn(&self) -> Option<SeqNo> {
        self.active_memtable
            .read()
            .expect("lock is poisoned")
            .get_lsn()
    }

    fn get_segment_lsn(&self) -> Option<SeqNo> {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.iter().map(|s| s.get_lsn()).max()
    }

    fn register_snapshot(&self) {
        self.open_snapshots.increment();
    }

    fn deregister_snapshot(&self) {
        self.open_snapshots.decrement();
    }

    fn snapshot(&self, seqno: SeqNo) -> Snapshot {
        use crate::AnyTree::Standard;

        Snapshot::new(Standard(self.clone()), seqno)
    }

    fn get_with_seqno<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: SeqNo,
    ) -> crate::Result<Option<UserValue>> {
        Ok(self
            .get_internal_entry(key, true, Some(seqno))?
            .map(|x| x.value))
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserValue>> {
        Ok(self.get_internal_entry(key, true, None)?.map(|x| x.value))
    }

    fn iter_with_seqno<'a>(
        &'a self,
        seqno: SeqNo,
        index: Option<&'a MemTable>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + '_> {
        self.range_with_seqno::<UserKey, _>(.., seqno, index)
    }

    fn range_with_seqno<'a, K: AsRef<[u8]>, R: RangeBounds<K>>(
        &'a self,
        range: R,
        seqno: SeqNo,
        index: Option<&'a MemTable>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + '_> {
        Box::new(self.create_range(range, Some(seqno), index))
    }

    fn prefix_with_seqno<'a, K: AsRef<[u8]>>(
        &'a self,
        prefix: K,
        seqno: SeqNo,
        index: Option<&'a MemTable>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + '_> {
        Box::new(self.create_prefix(prefix, Some(seqno), index))
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + '_> {
        Box::new(self.create_range(range, None, None))
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + '_> {
        Box::new(self.create_prefix(prefix, None, None))
    }

    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32) {
        let value = Value::new(key.as_ref(), value.as_ref(), seqno, ValueType::Value);
        self.append_entry(value)
    }

    fn raw_insert_with_lock<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        lock: &RwLockWriteGuard<'_, MemTable>,
        key: K,
        value: V,
        seqno: SeqNo,
        r#type: ValueType,
    ) -> (u32, u32) {
        let value = Value::new(key.as_ref(), value.as_ref(), seqno, r#type);
        lock.insert(value)
    }

    fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        let value = Value::new_tombstone(key.as_ref(), seqno);
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
    /// Returns error, if an IO error occured.
    pub fn open(config: Config) -> crate::Result<Self> {
        use crate::file::LSM_MARKER;

        log::debug!("Opening LSM-tree at {:?}", config.path);

        let tree = if config.path.join(LSM_MARKER).try_exists()? {
            Self::recover(config)
        } else {
            Self::create_new(config)
        }?;

        Ok(tree)
    }

    #[doc(hidden)]
    pub fn verify(&self) -> crate::Result<usize> {
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
    pub fn major_compact(&self, target_size: u64) -> crate::Result<()> {
        log::info!("Starting major compaction");
        let strategy = Arc::new(crate::compaction::major::Strategy::new(target_size));
        self.compact(strategy)
    }

    pub(crate) fn consume_writer(
        &self,
        segment_id: SegmentId,
        mut writer: crate::segment::writer::Writer,
    ) -> crate::Result<Arc<Segment>> {
        #[cfg(feature = "bloom")]
        use crate::bloom::BloomFilter;

        let segment_folder = writer.opts.folder.clone();
        let segment_file_path = segment_folder.join(segment_id.to_string());

        let trailer = writer.finish()?.expect("memtable should not be empty");

        log::debug!("Finalized segment write at {segment_folder:?}");

        // TODO: if L0, L1, preload block index (non-partitioned)
        let block_index = Arc::new(BlockIndex::from_file(
            &segment_file_path,
            trailer.offsets.tli_ptr,
            (self.id, segment_id).into(),
            self.config.descriptor_table.clone(),
            self.config.block_cache.clone(),
        )?);

        #[cfg(feature = "bloom")]
        let bloom_ptr = trailer.offsets.bloom_ptr;

        let created_segment: Arc<_> = Segment {
            tree_id: self.id,

            metadata: trailer.metadata,
            offsets: trailer.offsets,

            descriptor_table: self.config.descriptor_table.clone(),
            block_index,
            block_cache: self.config.block_cache.clone(),

            // TODO: as Bloom method
            #[cfg(feature = "bloom")]
            bloom_filter: {
                use crate::serde::Deserializable;
                use std::io::Seek;

                assert!(bloom_ptr > 0, "can not find bloom filter block");

                let mut reader = std::fs::File::open(&segment_file_path)?;
                reader.seek(std::io::SeekFrom::Start(bloom_ptr))?;
                BloomFilter::deserialize(&mut reader)?
            },
        }
        .into();

        self.config.descriptor_table.insert(
            segment_file_path,
            (self.id, created_segment.metadata.id).into(),
        );

        log::debug!("Flushed segment to {segment_folder:?}");

        Ok(created_segment)
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
    pub fn flush_active_memtable(&self) -> crate::Result<Option<Arc<Segment>>> {
        log::debug!("flush: flushing active memtable");

        let Some((segment_id, yanked_memtable)) = self.rotate_memtable() else {
            return Ok(None);
        };

        let segment = self.flush_memtable(segment_id, &yanked_memtable)?;
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
        memtable_lock: &RwLockWriteGuard<'_, MemTable>,
        key: K,
        evict_tombstone: bool,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        if let Some(entry) = memtable_lock.get(&key, seqno) {
            if evict_tombstone {
                return Ok(ignore_tombstone_value(entry));
            }
            return Ok(Some(entry));
        };

        // Now look in sealed memtables
        if let Some(entry) = self.get_internal_entry_from_sealed_memtables(&key, seqno) {
            if evict_tombstone {
                return Ok(ignore_tombstone_value(entry));
            }
            return Ok(Some(entry));
        }

        self.get_internal_entry_from_segments(key, evict_tombstone, seqno)
    }

    fn get_internal_entry_from_sealed_memtables<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> Option<Value> {
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
        evict_tombstone: bool,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        // NOTE: Create key hash for hash sharing
        // https://fjall-rs.github.io/post/bloom-filter-hash-sharing/
        #[cfg(feature = "bloom")]
        let key_hash = crate::bloom::BloomFilter::get_hash(key.as_ref());

        let level_manifest = self.levels.read().expect("lock is poisoned");

        for level in &level_manifest.levels {
            // NOTE: Based on benchmarking, binary search is only worth it after ~4 segments
            if level.is_disjoint && level.len() >= 5 {
                if let Some(segment) = level.get_segment_containing_key(&key) {
                    #[cfg(not(feature = "bloom"))]
                    let maybe_item = segment.get(&key, seqno)?;
                    #[cfg(feature = "bloom")]
                    let maybe_item = segment.get_with_hash(&key, seqno, key_hash)?;

                    if let Some(item) = maybe_item {
                        if evict_tombstone {
                            return Ok(ignore_tombstone_value(item));
                        }
                        return Ok(Some(item));
                    }
                }
            } else {
                // NOTE: Fallback to linear search
                for segment in &level.segments {
                    #[cfg(not(feature = "bloom"))]
                    let maybe_item = segment.get(&key, seqno)?;
                    #[cfg(feature = "bloom")]
                    let maybe_item = segment.get_with_hash(&key, seqno, key_hash)?;

                    if let Some(item) = maybe_item {
                        if evict_tombstone {
                            return Ok(ignore_tombstone_value(item));
                        }
                        return Ok(Some(item));
                    }
                }
            }
        }

        Ok(None)
    }

    #[doc(hidden)]
    pub fn get_internal_entry<K: AsRef<[u8]>>(
        &self,
        key: K,
        evict_tombstone: bool,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");

        if let Some(entry) = memtable_lock.get(&key, seqno) {
            if evict_tombstone {
                return Ok(ignore_tombstone_value(entry));
            }
            return Ok(Some(entry));
        };
        drop(memtable_lock);

        // Now look in sealed memtables
        if let Some(entry) = self.get_internal_entry_from_sealed_memtables(&key, seqno) {
            if evict_tombstone {
                return Ok(ignore_tombstone_value(entry));
            }
            return Ok(Some(entry));
        }

        // Now look in segments... this may involve disk I/O
        self.get_internal_entry_from_segments(key, evict_tombstone, seqno)
    }

    #[doc(hidden)]
    #[must_use]
    pub fn create_iter<'a>(
        &'a self,
        seqno: Option<SeqNo>,
        index: Option<&'a MemTable>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'a {
        self.create_range::<UserKey, _>(.., seqno, index)
    }

    #[doc(hidden)]
    pub fn create_range<'a, K: AsRef<[u8]>, R: RangeBounds<K>>(
        &'a self,
        range: R,
        seqno: Option<SeqNo>,
        add_index: Option<&'a MemTable>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'a {
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
        let level_manifest_lock = self.levels.read().expect("lock is poisoned");
        let active = self.active_memtable.read().expect("lock is poisoned");
        let sealed = self.sealed_memtables.read().expect("lock is poisoned");

        TreeIter::create_range(
            MemtableLockGuard { active, sealed },
            bounds,
            seqno,
            level_manifest_lock,
            add_index,
        )
    }

    #[doc(hidden)]
    pub fn create_prefix<'a, K: AsRef<[u8]>>(
        &'a self,
        prefix: K,
        seqno: Option<SeqNo>,
        add_index: Option<&'a MemTable>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'a {
        let prefix = prefix.as_ref();

        // NOTE: Mind lock order L -> M -> S
        let level_manifest_lock = self.levels.read().expect("lock is poisoned");
        let active = self.active_memtable.read().expect("lock is poisoned");
        let sealed = self.sealed_memtables.read().expect("lock is poisoned");

        TreeIter::create_prefix(
            MemtableLockGuard { active, sealed },
            &prefix.into(),
            seqno,
            level_manifest_lock,
            add_index,
        )
    }

    /// Adds an item to the active memtable.
    ///
    /// Returns the added item's size and new size of the memtable.
    #[doc(hidden)]
    #[must_use]
    pub fn append_entry(&self, value: Value) -> (u32, u32) {
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");
        memtable_lock.insert(value)
    }

    /// Recovers previous state, by loading the level manifest and segments.
    ///
    /// # Errors
    ///
    /// Returns error, if an IO error occured.
    fn recover(mut config: Config) -> crate::Result<Self> {
        use crate::{
            file::{CONFIG_FILE, LSM_MARKER},
            snapshot::Counter as SnapshotCounter,
        };
        use inner::get_next_tree_id;

        log::info!("Recovering LSM-tree at {:?}", config.path);

        {
            let bytes = std::fs::read(config.path.join(LSM_MARKER))?;

            if let Some(version) = Version::parse_file_header(&bytes) {
                if version != Version::V2 {
                    return Err(crate::Error::InvalidVersion(Some(version)));
                }
            } else {
                return Err(crate::Error::InvalidVersion(None));
            }
        }

        let tree_id = get_next_tree_id();

        let mut levels = Self::recover_levels(
            &config.path,
            tree_id,
            &config.block_cache,
            &config.descriptor_table,
        )?;
        levels.sort_levels();

        let config_from_disk = std::fs::read(config.path.join(CONFIG_FILE))?;
        let config_from_disk = PersistedConfig::deserialize(&mut Cursor::new(config_from_disk))?;
        config.inner = config_from_disk;

        let highest_segment_id = levels
            .iter()
            .map(|x| x.metadata.id)
            .max()
            .unwrap_or_default();

        let inner = TreeInner {
            id: tree_id,
            segment_id_counter: Arc::new(AtomicU64::new(highest_segment_id + 1)),
            active_memtable: Arc::default(),
            sealed_memtables: Arc::default(),
            levels: Arc::new(RwLock::new(levels)),
            open_snapshots: SnapshotCounter::default(),
            stop_signal: StopSignal::default(),
            config,
        };

        Ok(Self(Arc::new(inner)))
    }

    /// Creates a new LSM-tree in a directory.
    fn create_new(config: Config) -> crate::Result<Self> {
        use crate::file::{fsync_directory, CONFIG_FILE, LSM_MARKER, SEGMENTS_FOLDER};
        use std::fs::{create_dir_all, File};

        let path = config.path.clone();
        log::trace!("Creating LSM-tree at {path:?}");

        create_dir_all(&path)?;

        let marker_path = path.join(LSM_MARKER);
        assert!(!marker_path.try_exists()?);

        let segment_folder_path = path.join(SEGMENTS_FOLDER);
        create_dir_all(&segment_folder_path)?;

        let mut file = File::create(path.join(CONFIG_FILE))?;
        config.inner.serialize(&mut file)?;
        file.sync_all()?;

        let inner = TreeInner::create_new(config)?;

        // NOTE: Lastly, fsync version marker, which contains the version
        // -> the LSM is fully initialized
        let mut file = File::create(marker_path)?;
        Version::V2.write_file_header(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&segment_folder_path)?;
        fsync_directory(&path)?;

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
            file::{LEVELS_MANIFEST_FILE, SEGMENTS_FOLDER},
            SegmentId,
        };

        let tree_path = tree_path.as_ref();
        log::debug!("Recovering disk segments from {tree_path:?}");

        let manifest_path = tree_path.join(LEVELS_MANIFEST_FILE);

        let segment_ids_to_recover = LevelManifest::recover_ids(&manifest_path)?;

        let mut segments = vec![];

        let segment_base_folder = tree_path.join(SEGMENTS_FOLDER);

        if !segment_base_folder.try_exists()? {
            std::fs::create_dir_all(&segment_base_folder)?;
            fsync_directory(&segment_base_folder)?;
        }

        for dirent in std::fs::read_dir(&segment_base_folder)? {
            let dirent = dirent?;

            let file_name = dirent.file_name();
            let segment_file_name = file_name.to_str().expect("invalid segment folder name");
            let segment_file_path = dirent.path();

            assert!(!segment_file_path.is_dir());

            if segment_file_name.starts_with("tmp_") {
                log::debug!("Deleting unfinished segment: {segment_file_path:?}",);
                std::fs::remove_file(&segment_file_path)?;
                continue;
            }

            log::debug!("Recovering segment from {segment_file_path:?}");

            let segment_id = segment_file_name
                .parse::<SegmentId>()
                .expect("should be valid segment ID");

            if segment_ids_to_recover.contains(&segment_id) {
                let segment = Segment::recover(
                    &segment_file_path,
                    tree_id,
                    block_cache.clone(),
                    descriptor_table.clone(),
                )?;

                descriptor_table.insert(&segment_file_path, (tree_id, segment.metadata.id).into());

                segments.push(Arc::new(segment));
                log::debug!("Recovered segment from {segment_file_path:?}");
            } else {
                log::debug!("Deleting unfinished segment: {segment_file_path:?}",);
                std::fs::remove_file(&segment_file_path)?;
            }
        }

        if segments.len() < segment_ids_to_recover.len() {
            log::error!("Expected segments: {segment_ids_to_recover:?}");

            // TODO: no panic here
            panic!("Some segments were not recovered")
        }

        log::debug!("Recovered {} segments", segments.len());

        LevelManifest::recover(&manifest_path, segments)
    }
}
