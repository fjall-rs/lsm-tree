use crate::{
    compaction::CompactionStrategy,
    config::{Config, PersistedConfig},
    descriptor_table::FileDescriptorTable,
    levels::LevelManifest,
    memtable::MemTable,
    range::{MemtableLockGuard, TreeIter},
    segment::Segment,
    serde::{Deserializable, Serializable},
    stop_signal::StopSignal,
    tree_inner::{MemtableId, SealedMemtables, TreeId, TreeInner},
    version::Version,
    BlockCache, SeqNo, Snapshot, UserKey, UserValue, Value, ValueType,
};
use std::{
    io::Cursor,
    ops::RangeBounds,
    path::{Path, PathBuf},
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
pub struct Tree(pub(crate) Arc<TreeInner>);

impl std::ops::Deref for Tree {
    type Target = TreeInner;

    fn deref(&self) -> &Self::Target {
        &self.0
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
        let mut sum = 0;

        let level_manifest = self.levels.read().expect("lock is poisoned");

        for level in &level_manifest.levels {
            for segment in &level.segments {
                sum += segment.verify()?;
            }
        }

        Ok(sum)
    }

    /// Run compaction, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> crate::Result<()> {
        use crate::compaction::worker::{do_compaction, Options};

        let opts = Options::from_tree(self, strategy);
        do_compaction(&opts)?;

        log::debug!("lsm-tree: compaction run over");

        Ok(())
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

    /// Opens a read-only point-in-time snapshot of the tree
    ///
    /// Dropping the snapshot will close the snapshot
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    ///
    /// let snapshot = tree.snapshot(1);
    /// assert_eq!(snapshot.len()?, tree.len()?);
    ///
    /// tree.insert("b", "abc", 1);
    ///
    /// assert_eq!(2, tree.len()?);
    /// assert_eq!(1, snapshot.len()?);
    ///
    /// assert!(snapshot.contains_key("a")?);
    /// assert!(!snapshot.contains_key("b")?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    pub fn snapshot(&self, seqno: SeqNo) -> Snapshot {
        Snapshot::new(self.clone(), seqno)
    }

    /// Atomically registers flushed disk segments into the tree, removing their associated sealed memtables
    pub fn register_segments(&self, segments: &[Arc<Segment>]) -> crate::Result<()> {
        log::trace!("flush: acquiring levels manifest write lock");
        let mut levels = self.levels.write().expect("lock is poisoned");

        for segment in segments {
            levels.add(segment.clone());
        }

        log::trace!("flush: acquiring sealed memtables write lock");
        let mut memtable_lock = self.sealed_memtables.write().expect("lock is poisoned");

        for segment in segments {
            memtable_lock.remove(segment.metadata.id);
        }

        // NOTE: Segments are registered, we can unlock the memtable(s) safely
        drop(memtable_lock);

        levels.write_to_disk()?;

        Ok(())
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
    pub fn flush_active_memtable(&self) -> crate::Result<Option<PathBuf>> {
        use crate::{
            file::SEGMENTS_FOLDER,
            flush::{flush_to_segment, Options},
        };

        log::debug!("flush: flushing active memtable");

        let Some((segment_id, yanked_memtable)) = self.rotate_memtable() else {
            return Ok(None);
        };

        let segment_folder = self.config.path.join(SEGMENTS_FOLDER);
        log::debug!("flush: writing segment to {segment_folder:?}");

        let segment = flush_to_segment(Options {
            memtable: yanked_memtable,
            block_cache: self.config.block_cache.clone(),
            block_size: self.config.inner.block_size,
            folder: segment_folder.clone(),
            tree_id: self.id,
            segment_id,
            descriptor_table: self.config.descriptor_table.clone(),
        })?;
        let segment = Arc::new(segment);

        // Once we have written the segment, we need to add it to the level manifest
        // and remove it from the sealed memtables
        self.register_segments(&[segment])?;

        log::debug!("flush: thread done");
        Ok(Some(segment_folder))
    }

    /// Returns `true` if there are some segments that are being compacted.
    #[doc(hidden)]
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.is_compacting()
    }

    /// Returns the amount of disk segments in the first level.
    #[must_use]
    pub fn first_level_segment_count(&self) -> usize {
        self.levels
            .read()
            .expect("lock is poisoned")
            .first_level_segment_count()
    }

    /// Returns the amount of disk segments currently in the tree.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.levels.read().expect("lock is poisoned").len()
    }

    /// Approximates the amount of items in the tree.
    #[must_use]
    pub fn approximate_len(&self) -> u64 {
        // IMPORTANT: Mind lock order L -> M -> S
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

    /// Returns the approximate size of the active memtable in bytes.
    ///
    /// May be used to flush the memtable if it grows too large.
    #[must_use]
    pub fn active_memtable_size(&self) -> u32 {
        use std::sync::atomic::Ordering::Acquire;

        self.active_memtable
            .read()
            .expect("lock is poisoned")
            .approximate_size
            .load(Acquire)
    }

    /// Write-locks the active memtable for exclusive access
    pub fn lock_active_memtable(&self) -> RwLockWriteGuard<'_, MemTable> {
        self.active_memtable.write().expect("lock is poisoned")
    }

    /// Write-locks the sealed memtables for exclusive access
    fn lock_sealed_memtables(&self) -> RwLockWriteGuard<'_, SealedMemtables> {
        self.sealed_memtables.write().expect("lock is poisoned")
    }

    /// Seals the active memtable, and returns a reference to it
    #[must_use]
    pub fn rotate_memtable(&self) -> Option<(MemtableId, Arc<MemTable>)> {
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

    /// Sets the active memtable.
    ///
    /// May be used to restore the LSM-tree's in-memory state from a write-ahead log
    /// after tree recovery.
    pub fn set_active_memtable(&self, memtable: MemTable) {
        let mut memtable_lock = self.active_memtable.write().expect("lock is poisoned");
        *memtable_lock = memtable;
    }

    /// Adds a sealed memtables.
    ///
    /// May be used to restore the LSM-tree's in-memory state from some journals.
    pub fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<MemTable>) {
        let mut memtable_lock = self.sealed_memtables.write().expect("lock is poisoned");
        memtable_lock.add(id, memtable);
    }

    /// Scans the entire tree, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire tree: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the tree is empty, use [`Tree::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// use lsm_tree::{Tree, Config};
    ///
    /// let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// assert_eq!(tree.len()?, 0);
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    /// assert_eq!(tree.len()?, 3);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

        // TODO: shouldn't thrash block cache
        for item in self.iter() {
            let _ = item?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the tree is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// assert!(tree.is_empty()?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(!tree.is_empty()?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    #[doc(hidden)]
    pub fn get_internal_entry<K: AsRef<[u8]>>(
        &self,
        key: K,
        evict_tombstone: bool,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        let memtable_lock = self.active_memtable.read().expect("lock is poisoned");

        if let Some(item) = memtable_lock.get(&key, seqno) {
            if evict_tombstone {
                return Ok(ignore_tombstone_value(item));
            }
            return Ok(Some(item));
        };
        drop(memtable_lock);

        // Now look in sealed memtables
        let memtable_lock = self.sealed_memtables.read().expect("lock is poisoned");
        for (_, memtable) in memtable_lock.iter().rev() {
            if let Some(item) = memtable.get(&key, seqno) {
                if evict_tombstone {
                    return Ok(ignore_tombstone_value(item));
                }
                return Ok(Some(item));
            }
        }
        drop(memtable_lock);

        // Now look in segments... this may involve disk I/O
        let level_manifest = self.levels.read().expect("lock is poisoned");

        for level in &level_manifest.levels {
            // NOTE: Based on benchmarking, binary search is only worth it after ~5 segments
            if level.is_disjoint && level.len() > 5 {
                if let Some(segment) = level.get_segment_containing_key(&key) {
                    if let Some(item) = segment.get(&key, seqno)? {
                        if evict_tombstone {
                            return Ok(ignore_tombstone_value(item));
                        }
                        return Ok(Some(item));
                    }
                }
            } else {
                for segment in &level.segments {
                    if let Some(item) = segment.get(&key, seqno)? {
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

    /// Retrieves an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", "my_value", 0);
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserValue>> {
        Ok(self.get_internal_entry(key, true, None)?.map(|x| x.value))
    }

    /// Inserts a key-value pair into the tree.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// tree.insert("a", "abc", 0);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u32, u32) {
        let value = Value::new(key.as_ref(), value.as_ref(), seqno, ValueType::Value);
        self.append_entry(value)
    }

    /// Removes an item from the tree.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("a", "abc", 0);
    ///
    /// let item = tree.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove("a", 1);
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32) {
        let value = Value::new_tombstone(key.as_ref(), seqno);
        self.append_entry(value)
    }

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    /// assert!(!tree.contains_key("a")?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(tree.contains_key("a")?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
    }

    pub(crate) fn create_iter(
        &self,
        seqno: Option<SeqNo>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
        self.create_range::<UserKey, _>(.., seqno)
    }

    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    /// assert_eq!(3, tree.iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::iter_not_returning_iterator)]
    #[must_use]
    pub fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
        self.create_iter(None)
    }

    pub(crate) fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: Option<SeqNo>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
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

        // IMPORTANT: Mind lock order L -> M -> S
        let level_manifest_lock = self.levels.read().expect("lock is poisoned");
        let active = self.active_memtable.read().expect("lock is poisoned");
        let sealed = self.sealed_memtables.read().expect("lock is poisoned");

        TreeIter::create_range(
            MemtableLockGuard { active, sealed },
            bounds,
            seqno,
            level_manifest_lock,
        )
    }

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    /// assert_eq!(2, tree.range("a"..="f").count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
        self.create_range(range, None)
    }

    pub(crate) fn create_prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: Option<SeqNo>,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
        let prefix = prefix.as_ref();

        // IMPORTANT: Mind lock order L -> M -> S
        let level_manifest_lock = self.levels.read().expect("lock is poisoned");
        let active = self.active_memtable.read().expect("lock is poisoned");
        let sealed = self.sealed_memtables.read().expect("lock is poisoned");

        TreeIter::create_prefix(
            MemtableLockGuard { active, sealed },
            prefix.into(),
            seqno,
            level_manifest_lock,
        )
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("ab", "abc", 1);
    /// tree.insert("abc", "abc", 2);
    /// assert_eq!(2, tree.prefix("ab").count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + '_ {
        self.create_prefix(prefix, None)
    }

    /// Returns the first key-value pair in the tree.
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let (key, _) = tree.first_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter().next().transpose()
    }

    /// Returns the last key-value pair in the tree.
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{Tree, Config};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder).open()?;
    /// #
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let (key, _) = tree.last_key_value()?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter().next_back().transpose()
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
            tree_inner::get_next_tree_id,
        };

        log::info!("Recovering LSM-tree at {:?}", config.path);

        {
            let bytes = std::fs::read(config.path.join(LSM_MARKER))?;

            if let Some(version) = Version::parse_file_header(&bytes) {
                if version != Version::V1 {
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

        // NOTE: Lastly, fsync .lsm marker, which contains the version
        // -> the LSM is fully initialized
        let mut file = File::create(marker_path)?;
        Version::V1.write_file_header(&mut file)?;
        file.sync_all()?;

        // IMPORTANT: fsync folders on Unix
        fsync_directory(&segment_folder_path)?;
        fsync_directory(&path)?;

        Ok(Self(Arc::new(inner)))
    }

    /// Returns the disk space usage
    #[must_use]
    pub fn disk_space(&self) -> u64 {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.iter().map(|x| x.metadata.file_size).sum()
    }

    /// Returns the highest sequence number that is flushed to disk
    #[must_use]
    pub fn get_segment_lsn(&self) -> Option<SeqNo> {
        let levels = self.levels.read().expect("lock is poisoned");
        levels.iter().map(|s| s.get_lsn()).max()
    }

    /// Returns the highest sequence number
    #[must_use]
    pub fn get_lsn(&self) -> Option<SeqNo> {
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

    /// Returns the highest sequence number of the active memtable
    #[must_use]
    #[doc(hidden)]
    pub fn get_memtable_lsn(&self) -> Option<SeqNo> {
        self.active_memtable
            .read()
            .expect("lock is poisoned")
            .get_lsn()
    }

    /// Recovers the level manifest, loading all segments from disk.
    fn recover_levels<P: AsRef<Path>>(
        tree_path: P,
        tree_id: TreeId,
        block_cache: &Arc<BlockCache>,
        descriptor_table: &Arc<FileDescriptorTable>,
    ) -> crate::Result<LevelManifest> {
        use crate::{
            file::{BLOCKS_FILE, LEVELS_MANIFEST_FILE, SEGMENTS_FOLDER},
            SegmentId,
        };

        let tree_path = tree_path.as_ref();
        log::debug!("Recovering disk segments from {tree_path:?}");

        let manifest_path = tree_path.join(LEVELS_MANIFEST_FILE);

        let segment_ids_to_recover = LevelManifest::recover_ids(&manifest_path)?;

        let mut segments = vec![];

        for dirent in std::fs::read_dir(tree_path.join(SEGMENTS_FOLDER))? {
            let dirent = dirent?;
            let segment_path = dirent.path();

            assert!(segment_path.is_dir());

            let segment_id = dirent
                .file_name()
                .to_str()
                .expect("invalid segment folder name")
                .parse::<SegmentId>()
                .expect("should be valid segment ID");

            log::debug!("Recovering segment from {segment_path:?}");

            if segment_ids_to_recover.contains(&segment_id) {
                let segment = Segment::recover(
                    &segment_path,
                    tree_id,
                    Arc::clone(block_cache),
                    descriptor_table.clone(),
                )?;

                descriptor_table.insert(
                    segment_path.join(BLOCKS_FILE),
                    (tree_id, segment.metadata.id).into(),
                );

                segments.push(Arc::new(segment));
                log::debug!("Recovered segment from {segment_path:?}");
            } else {
                log::debug!(
                    "Deleting unfinished segment (not part of level manifest): {}",
                    segment_path.to_string_lossy()
                );
                std::fs::remove_dir_all(segment_path)?;
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
