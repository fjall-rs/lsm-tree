use crate::{
    compaction::CompactionStrategy, config::TreeType, tree::inner::MemtableId, AnyTree, BlobTree,
    Config, KvPair, MemTable, Segment, SegmentId, SeqNo, Snapshot, Tree, UserKey, UserValue,
    ValueType,
};
use enum_dispatch::enum_dispatch;
use std::{
    ops::RangeBounds,
    sync::{Arc, RwLockWriteGuard},
};

pub type RangeItem = crate::Result<KvPair>;

/// Generic Tree API
#[allow(clippy::module_name_repetitions)]
#[enum_dispatch]
pub trait AbstractTree {
    #[doc(hidden)]
    fn verify(&self) -> crate::Result<usize>;

    /// Synchronously flushes a memtable to a disk segment.
    ///
    /// This method will not make the segment immediately available,
    /// use [`register_segments`] for that.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn flush_memtable(
        &self,
        segment_id: SegmentId,
        memtable: &Arc<MemTable>,
    ) -> crate::Result<Arc<Segment>>;

    /// Atomically registers flushed disk segments into the tree, removing their associated sealed memtables.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn register_segments(&self, segments: &[Arc<Segment>]) -> crate::Result<()>;

    /// Write-locks the active memtable for exclusive access
    fn lock_active_memtable(&self) -> RwLockWriteGuard<'_, MemTable>;

    /// Sets the active memtable.
    ///
    /// May be used to restore the LSM-tree's in-memory state from a write-ahead log
    /// after tree recovery.
    fn set_active_memtable(&self, memtable: MemTable);

    /// Adds a sealed memtables.
    ///
    /// May be used to restore the LSM-tree's in-memory state from some journals.
    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<MemTable>);

    /// Performs compaction on the tree's levels, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn compact(
        &self,
        strategy: Arc<dyn CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()>;

    /// Returns the next segment's ID.
    fn get_next_segment_id(&self) -> SegmentId;

    /// Returns the tree config.
    fn tree_config(&self) -> &Config;

    /// Returns the highest sequence number.
    fn get_lsn(&self) -> Option<SeqNo>;

    /// Returns the approximate size of the active memtable in bytes.
    ///
    /// May be used to flush the memtable if it grows too large.
    fn active_memtable_size(&self) -> u32;

    /// Returns the tree type.
    fn tree_type(&self) -> TreeType;

    /// Seals the active memtable, and returns a reference to it.
    fn rotate_memtable(&self) -> Option<(MemtableId, Arc<MemTable>)>;

    /// Returns the amount of disk segments currently in the tree.
    fn segment_count(&self) -> usize;

    /// Returns the amount of disk segments in the first level.
    fn first_level_segment_count(&self) -> usize;

    /// Approximates the amount of items in the tree.
    fn approximate_len(&self) -> u64;

    /// Returns the disk space usage.
    fn disk_space(&self) -> u64;

    /// Returns the highest sequence number of the active memtable.
    fn get_memtable_lsn(&self) -> Option<SeqNo>;

    /// Returns the highest sequence number that is flushed to disk.
    fn get_segment_lsn(&self) -> Option<SeqNo>;

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
    /// use lsm_tree::{AbstractTree, Config, Tree};
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
    fn len(&self) -> crate::Result<usize> {
        let mut count = 0;

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
    /// use lsm_tree::{AbstractTree, Config, Tree};
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
    fn is_empty(&self) -> crate::Result<bool> {
        self.first_key_value().map(|x| x.is_none())
    }

    /// Returns the first key-value pair in the tree.
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
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
    fn first_key_value(&self) -> crate::Result<Option<KvPair>> {
        self.iter().next().transpose()
    }

    /// Returns the last key-value pair in the tree.
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
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
    fn last_key_value(&self) -> crate::Result<Option<KvPair>> {
        self.iter().next_back().transpose()
    }

    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    /// assert_eq!(3, tree.iter().into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    #[must_use]
    fn iter(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static> {
        self.range::<UserKey, _>(..)
    }

    /// Returns an iterator that scans through the entire tree, returning keys only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn keys(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static>;

    /// Returns an iterator that scans through the entire tree, returning values only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn values(&self) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static>;

    /// Returns an iterator over a snapshot instant, returning keys only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn keys_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static>;

    /// Returns an iterator over a snapshot instant, returning values only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn values_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static>;

    /// Creates an iterator over a snapshot instant.
    fn iter_with_seqno(
        &self,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static>;

    /// Creates an bounded iterator over a snapshot instant.
    fn range_with_seqno<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static>;

    /// Creates a prefix iterator over a snapshot instant.
    fn prefix_with_seqno<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<Arc<MemTable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static>;

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    ///
    /// tree.insert("a", "abc", 0);
    /// tree.insert("f", "abc", 1);
    /// tree.insert("g", "abc", 2);
    /// assert_eq!(2, tree.range("a"..="f").into_iter().count());
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static>;

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
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
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
    ) -> Box<dyn DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static>;

    /// Retrieves an item from the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
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
    fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<UserValue>>;

    /// Retrieves an item from a snapshot instant.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get_with_seqno<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: SeqNo,
    ) -> crate::Result<Option<UserValue>>;

    /// Opens a read-only point-in-time snapshot of the tree
    ///
    /// Dropping the snapshot will close the snapshot
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
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
    fn snapshot(&self, seqno: SeqNo) -> Snapshot;

    /// Opens a snapshot of this partition with a given sequence number
    #[must_use]
    fn snapshot_at(&self, seqno: SeqNo) -> Snapshot {
        self.snapshot(seqno)
    }

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
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
    fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.get(key).map(|x| x.is_some())
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
    /// use lsm_tree::{AbstractTree, Config, Tree};
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
    fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V, seqno: SeqNo) -> (u32, u32);

    /// Inserts a key-value pair.
    fn raw_insert_with_lock<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        lock: &RwLockWriteGuard<'_, MemTable>,
        key: K,
        value: V,
        seqno: SeqNo,
        r#type: ValueType,
    ) -> (u32, u32);

    /// Removes an item from the tree.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
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
    fn remove<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32);

    /// Removes an item from the tree.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// Returns the added item's size and new size of the memtable.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// # let tree = Config::new(folder).open()?;
    /// tree.insert("a", "abc", 0);
    ///
    /// let item = tree.get("a")?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove_weak("a", 1);
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
    fn remove_weak<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> (u32, u32);
}
