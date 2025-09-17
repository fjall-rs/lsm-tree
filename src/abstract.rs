// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::CompactionStrategy, config::TreeType, iter_guard::IterGuardImpl, segment::Segment,
    tree::inner::MemtableId, AnyTree, BlobTree, Config, Guard, KvPair, Memtable, SegmentId, SeqNo,
    Tree, UserKey, UserValue,
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
    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn iter(
        &self,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_> {
        self.range::<&[u8], _>(.., seqno, index)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_>;

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl<'_>> + '_>;

    /// Ingests a sorted stream of key-value pairs into the tree.
    ///
    /// Can only be called on a new fresh, empty tree.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the tree is **not** initially empty.
    ///
    /// Will panic if the input iterator is not sorted in ascending order.
    #[doc(hidden)]
    fn ingest(&self, iter: impl Iterator<Item = (UserKey, UserValue)>) -> crate::Result<()>;

    /// Returns the approximate number of tombstones in the tree.
    fn tombstone_count(&self) -> u64;

    // TODO: clear() with Nuke compaction strategy (write lock) -> drop_range(..)

    /// Drops segments that are fully contained in a given range.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn drop_range(&self, key_range: crate::KeyRange) -> crate::Result<()>;

    /// Performs major compaction, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn major_compact(&self, target_size: u64, seqno_threshold: SeqNo) -> crate::Result<()>;

    /// Gets the memory usage of all pinned filters in the tree.
    fn pinned_filter_size(&self) -> usize;

    /// Gets the memory usage of all pinned index blocks in the tree.
    fn pinned_block_index_size(&self) -> usize;

    // TODO:?
    /* #[doc(hidden)]
    fn verify(&self) -> crate::Result<usize>; */

    /// Synchronously flushes a memtable to a disk segment.
    ///
    /// This method will not make the segment immediately available,
    /// use [`AbstractTree::register_segments`] for that.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn flush_memtable(
        &self,
        segment_id: SegmentId, // TODO: remove?
        memtable: &Arc<Memtable>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<Option<Segment>>;

    /// Atomically registers flushed disk segments into the tree, removing their associated sealed memtables.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn register_segments(&self, segments: &[Segment], seqno_threshold: SeqNo) -> crate::Result<()>;

    /// Write-locks the active memtable for exclusive access
    fn lock_active_memtable(&self) -> RwLockWriteGuard<'_, Arc<Memtable>>;

    /// Clears the active memtable atomically.
    fn clear_active_memtable(&self);

    /// Sets the active memtable.
    ///
    /// May be used to restore the LSM-tree's in-memory state from a write-ahead log
    /// after tree recovery.
    fn set_active_memtable(&self, memtable: Memtable);

    /// Returns the number of sealed memtables.
    fn sealed_memtable_count(&self) -> usize;

    /// Adds a sealed memtables.
    ///
    /// May be used to restore the LSM-tree's in-memory state from some journals.
    fn add_sealed_memtable(&self, id: MemtableId, memtable: Arc<Memtable>);

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
    fn get_highest_seqno(&self) -> Option<SeqNo> {
        let memtable_seqno = self.get_highest_memtable_seqno();
        let segment_seqno = self.get_highest_persisted_seqno();
        memtable_seqno.max(segment_seqno)
    }

    /// Returns the approximate size of the active memtable in bytes.
    ///
    /// May be used to flush the memtable if it grows too large.
    fn active_memtable_size(&self) -> u64;

    /// Returns the tree type.
    fn tree_type(&self) -> TreeType;

    /// Seals the active memtable, and returns a reference to it.
    fn rotate_memtable(&self) -> Option<(MemtableId, Arc<Memtable>)>;

    /// Returns the number of disk segments currently in the tree.
    fn segment_count(&self) -> usize;

    /// Returns the number of segments in levels[idx].
    ///
    /// Returns `None` if the level does not exist (if idx >= 7).
    fn level_segment_count(&self, idx: usize) -> Option<usize>;

    /// Returns the number of disjoint runs in L0.
    ///
    /// Can be used to determine whether to write stall.
    fn l0_run_count(&self) -> usize;

    /// Returns the number of blob files currently in the tree.
    fn blob_file_count(&self) -> usize {
        0
    }

    /// Approximates the number of items in the tree.
    fn approximate_len(&self) -> usize;

    /// Returns the disk space usage.
    fn disk_space(&self) -> u64;

    /// Returns the highest sequence number of the active memtable.
    fn get_highest_memtable_seqno(&self) -> Option<SeqNo>;

    /// Returns the highest sequence number that is flushed to disk.
    fn get_highest_persisted_seqno(&self) -> Option<SeqNo>;

    /// Scans the entire tree, returning the number of items.
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
    /// assert_eq!(tree.len(0, None)?, 0);
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    /// assert_eq!(tree.len(3, None)?, 3);
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn len(&self, seqno: SeqNo, index: Option<Arc<Memtable>>) -> crate::Result<usize> {
        let mut count = 0;

        for item in self.iter(seqno, index) {
            let _ = item.key()?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the tree is empty.
    ///
    /// This operation has O(log N) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder).open()?;
    /// assert!(tree.is_empty(0, None)?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(!tree.is_empty(1, None)?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn is_empty(&self, seqno: SeqNo, index: Option<Arc<Memtable>>) -> crate::Result<bool> {
        self.first_key_value(seqno, index).map(|x| x.is_none())
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
    /// let (key, _) = tree.first_key_value(3, None)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn first_key_value(
        &self,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> crate::Result<Option<KvPair>> {
        self.iter(seqno, index)
            .next()
            .map(Guard::into_inner)
            .transpose()
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
    /// let (key, _) = tree.last_key_value(3, None)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), TreeError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn last_key_value(
        &self,
        seqno: SeqNo,
        index: Option<Arc<Memtable>>,
    ) -> crate::Result<Option<KvPair>> {
        self.iter(seqno, index)
            .next_back()
            .map(Guard::into_inner)
            .transpose()
    }

    /// Returns the size of a value if it exists.
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
    /// let size = tree.size_of("a", 1)?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, size);
    ///
    /// let size = tree.size_of("b", 1)?.unwrap_or_default();
    /// assert_eq!(0, size);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>>;

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
    /// let item = tree.get("a", 1)?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<UserValue>>;

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// let tree = Config::new(folder).open()?;
    /// assert!(!tree.contains_key("a", 0)?);
    ///
    /// tree.insert("a", "abc", 0);
    /// assert!(tree.contains_key("a", 1)?);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        self.get(key, seqno).map(|x| x.is_some())
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
    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64);

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
    /// let item = tree.get("a", 1)?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove("a", 1);
    ///
    /// let item = tree.get("a", 2)?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64);

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
    /// let item = tree.get("a", 1)?.expect("should have item");
    /// assert_eq!("abc".as_bytes(), &*item);
    ///
    /// tree.remove_weak("a", 1);
    ///
    /// let item = tree.get("a", 2)?;
    /// assert_eq!(None, item);
    /// #
    /// # Ok::<(), lsm_tree::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64);
}
