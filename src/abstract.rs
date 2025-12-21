// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    iter_guard::IterGuardImpl, table::Table, version::Version, vlog::BlobFile, AnyTree, BlobTree,
    Config, Guard, InternalValue, KvPair, Memtable, SeqNo, TableId, Tree, UserKey, UserValue,
};
use std::{
    ops::RangeBounds,
    sync::{Arc, MutexGuard, RwLockWriteGuard},
};

pub type RangeItem = crate::Result<KvPair>;

type FlushToTablesResult = (Vec<Table>, Option<Vec<BlobFile>>);

/// Generic Tree API
#[enum_dispatch::enum_dispatch]
pub trait AbstractTree {
    /// Returns the number of cached table file descriptors.
    fn table_file_cache_size(&self) -> usize;

    // TODO: remove
    #[doc(hidden)]
    fn version_memtable_size_sum(&self) -> u64 {
        self.get_version_history_lock().memtable_size_sum()
    }

    #[doc(hidden)]
    fn next_table_id(&self) -> TableId;

    #[doc(hidden)]
    fn id(&self) -> crate::TreeId;

    /// Like [`AbstractTree::get`], but returns the actual internal entry, not just the user value.
    ///
    /// Used in tests.
    #[doc(hidden)]
    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>>;

    #[doc(hidden)]
    fn current_version(&self) -> Version;

    #[doc(hidden)]
    fn get_version_history_lock(&self) -> RwLockWriteGuard<'_, crate::version::SuperVersions>;

    /// Seals the active memtable and flushes to table(s).
    ///
    /// If there are already other sealed memtables lined up, those will be flushed as well.
    ///
    /// Only used in tests.
    #[doc(hidden)]
    fn flush_active_memtable(&self, eviction_seqno: SeqNo) -> crate::Result<()> {
        let lock = self.get_flush_lock();
        self.rotate_memtable();
        self.flush(&lock, eviction_seqno)?;
        Ok(())
    }

    /// Synchronously flushes pending sealed memtables to tables.
    ///
    /// Returns the sum of flushed memtable sizes that were flushed.
    ///
    /// The function may not return a result, if nothing was flushed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn flush(
        &self,
        _lock: &MutexGuard<'_, ()>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<Option<u64>> {
        use crate::{compaction::stream::CompactionStream, merge::Merger};

        let version_history = self.get_version_history_lock();
        let latest = version_history.latest_version();

        if latest.sealed_memtables.len() == 0 {
            return Ok(None);
        }

        let sealed_ids = latest
            .sealed_memtables
            .iter()
            .map(|mt| mt.id)
            .collect::<Vec<_>>();

        let flushed_size = latest.sealed_memtables.iter().map(|mt| mt.size()).sum();

        let merger = Merger::new(
            latest
                .sealed_memtables
                .iter()
                .map(|mt| mt.iter().map(Ok))
                .collect::<Vec<_>>(),
        );
        let stream = CompactionStream::new(merger, seqno_threshold);

        drop(version_history);

        if let Some((tables, blob_files)) = self.flush_to_tables(stream)? {
            self.register_tables(
                &tables,
                blob_files.as_deref(),
                None,
                &sealed_ids,
                seqno_threshold,
            )?;
        }

        Ok(Some(flushed_size))
    }

    /// Returns an iterator that scans through the entire tree.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    fn iter(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        self.range::<&[u8], _>(.., seqno, index)
    }

    /// Returns an iterator over a prefixed set of items.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static>;

    /// Returns an iterator over a range of items.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static>;

    /// Returns the approximate number of tombstones in the tree.
    fn tombstone_count(&self) -> u64;

    /// Returns the approximate number of weak tombstones (single deletes) in the tree.
    fn weak_tombstone_count(&self) -> u64;

    /// Returns the approximate number of values reclaimable once weak tombstones can be GC'd.
    fn weak_tombstone_reclaimable_count(&self) -> u64;

    /// Drops tables that are fully contained in a given range.
    ///
    /// Accepts any `RangeBounds`, including unbounded or exclusive endpoints.
    /// If the normalized lower bound is greater than the upper bound, the
    /// method returns without performing any work.
    ///
    /// # Errors
    ///
    /// Will return `Err` only if an IO error occurs.
    fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(&self, range: R) -> crate::Result<()>;

    /// Drops all tables and clears all memtables atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` only if an IO error occurs.
    fn clear(&self) -> crate::Result<()>;

    /// Performs major compaction, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn major_compact(&self, target_size: u64, seqno_threshold: SeqNo) -> crate::Result<()>;

    /// Returns the disk space used by stale blobs.
    fn stale_blob_bytes(&self) -> u64 {
        0
    }

    /// Gets the space usage of all filters in the tree.
    ///
    /// May not correspond to the actual memory size because filter blocks may be paged out.
    fn filter_size(&self) -> u64;

    /// Gets the memory usage of all pinned filters in the tree.
    fn pinned_filter_size(&self) -> usize;

    /// Gets the memory usage of all pinned index blocks in the tree.
    fn pinned_block_index_size(&self) -> usize;

    /// Gets the length of the version free list.
    fn version_free_list_len(&self) -> usize;

    /// Returns the metrics structure.
    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics>;

    /// Acquires the flush lock which is required to call [`Tree::flush`].
    fn get_flush_lock(&self) -> MutexGuard<'_, ()>;

    /// Synchronously flushes a memtable to a table.
    ///
    /// This method will not make the table immediately available,
    /// use [`AbstractTree::register_tables`] for that.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[warn(clippy::type_complexity)]
    fn flush_to_tables(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<FlushToTablesResult>>;

    /// Atomically registers flushed tables into the tree, removing their associated sealed memtables.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn register_tables(
        &self,
        tables: &[Table],
        blob_files: Option<&[BlobFile]>,
        frag_map: Option<crate::blob_tree::FragmentationMap>,
        sealed_memtables_to_delete: &[crate::tree::inner::MemtableId],
        gc_watermark: SeqNo,
    ) -> crate::Result<()>;

    /// Clears the active memtable atomically.
    fn clear_active_memtable(&self);

    /// Returns the number of sealed memtables.
    fn sealed_memtable_count(&self) -> usize;

    /// Performs compaction on the tree's levels, blocking the caller until it's done.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn compact(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        seqno_threshold: SeqNo,
    ) -> crate::Result<()>;

    /// Returns the next table's ID.
    fn get_next_table_id(&self) -> TableId;

    /// Returns the tree config.
    fn tree_config(&self) -> &Config;

    /// Returns the highest sequence number.
    fn get_highest_seqno(&self) -> Option<SeqNo> {
        let memtable_seqno = self.get_highest_memtable_seqno();
        let table_seqno = self.get_highest_persisted_seqno();
        memtable_seqno.max(table_seqno)
    }

    /// Returns the active memtable.
    fn active_memtable(&self) -> Arc<Memtable>;

    /// Returns the tree type.
    fn tree_type(&self) -> crate::TreeType;

    /// Seals the active memtable.
    fn rotate_memtable(&self) -> Option<Arc<Memtable>>;

    /// Returns the number of tables currently in the tree.
    fn table_count(&self) -> usize;

    /// Returns the number of tables in `levels[idx]`.
    ///
    /// Returns `None` if the level does not exist (if idx >= 7).
    fn level_table_count(&self, idx: usize) -> Option<usize>;

    /// Returns the number of disjoint runs in L0.
    ///
    /// Can be used to determine whether to write stall.
    fn l0_run_count(&self) -> usize;

    /// Returns the number of blob files currently in the tree.
    fn blob_file_count(&self) -> usize;

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
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
    fn len(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<usize> {
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
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
    fn is_empty(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<bool> {
        Ok(self
            .first_key_value(seqno, index)
            .map(crate::Guard::key)
            .transpose()?
            .is_none())
    }

    /// Returns the first key-value pair in the tree.
    /// The key in this pair is the minimum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree, Guard};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    ///
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let key = tree.first_key_value(3, None).expect("item should exist").key()?;
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
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        self.iter(seqno, index).next()
    }

    /// Returns the last key-value pair in the tree.
    /// The key in this pair is the maximum key in the tree.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lsm_tree::Error as TreeError;
    /// # use lsm_tree::{AbstractTree, Config, Tree, Guard};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
    /// #
    /// tree.insert("1", "abc", 0);
    /// tree.insert("3", "abc", 1);
    /// tree.insert("5", "abc", 2);
    ///
    /// let key = tree.last_key_value(3, None).expect("item should exist").key()?;
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
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        self.iter(seqno, index).next_back()
    }

    /// Returns the size of a value if it exists.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// use lsm_tree::{AbstractTree, Config, Tree};
    ///
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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

    /// Retrieves multiple values for a given set of keys.
    ///
    /// The result is a `Vec<Option<UserValue>>` with the same length as the
    /// input keys. Each element is either `Some(value)` if the key was found,
    /// or `None` if the key was not found. The order of the results corresponds
    /// to the order of the input keys.
    fn multi_get(
        &self,
        keys: &[&[u8]],
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<UserValue>>>;

    /// Returns `true` if the tree contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # let folder = tempfile::tempdir()?;
    /// # use lsm_tree::{AbstractTree, Config, Tree};
    /// #
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
    /// let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
    /// # let tree = Config::new(folder, Default::default(), Default::default()).open()?;
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
