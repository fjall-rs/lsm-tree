// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::FragmentationMap,
    compaction::worker::NewCompactionResult,
    iter_guard::IterGuardImpl,
    table::Table,
    version::{SuperVersion, Version},
    vlog::BlobFile,
    AnyTree, BlobTree, Config, EditRecord, Guard, InternalValue, KvPair, Memtable, MemtableId,
    SeqNo, TableId, Tree, UserKey, UserValue, VersionEdit,
};
use std::{
    ops::RangeBounds,
    sync::{Arc, MutexGuard},
};

pub type RangeItem = crate::Result<KvPair>;

// TODO: change to struct
pub type FlushToTablesResult = (Vec<Table>, Option<Vec<BlobFile>>);

pub struct NewFlushToTablesResult {
    pub tables: Vec<Table>,
    pub blob_files: Option<Vec<BlobFile>>,
    pub frag_map: Option<FragmentationMap>,
    pub evicted_sealed_memtables: Vec<MemtableId>,
    pub flushed_size: u64,

    pub version_edit: VersionEdit,
}

/// Generic Tree API
#[enum_dispatch::enum_dispatch]
pub trait AbstractTree {
    /// Debug method for tracing the MVCC history of a key.
    #[doc(hidden)]
    fn print_trace(&self, key: &[u8]) -> crate::Result<()>;

    /// Returns the number of cached table file descriptors.
    fn table_file_cache_size(&self) -> usize;

    // TODO: remove
    #[doc(hidden)]
    fn version_memtable_size_sum(&self) -> u64 {
        //self.get_version_history_lock().memtable_size_sum()
        todo!()
    }

    #[doc(hidden)]
    fn inner(&self) -> &crate::tree::inner::TreeInner;

    /// Returns a new [`SuperVersion`] with the active memtable removed.
    fn clear_active_memtable(&self) -> Option<SuperVersion> {
        let super_version = self.current_super_version();

        if super_version.active_memtable.is_empty() {
            return None;
        }

        let mut copy = super_version.clone();
        copy.active_memtable = Arc::new(Memtable::new(self.inner().memtable_id_counter.next()));
        copy.sealed_memtables = Arc::new(crate::tree::sealed::SealedMemtables::default());

        // Clear active is only used for recovery where snapshots do not exist yet
        copy.seqno = super_version.seqno;

        Some(copy)
    }

    // TODO: remove this? just provide active_memtable_size and write_buffer_size?
    /// Returns the active memtable.
    fn active_memtable(&self) -> Arc<Memtable> {
        self.current_super_version().active_memtable
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

    // TODO: rename latest_*
    #[doc(hidden)]
    fn current_super_version(&self) -> SuperVersion {
        self.tree_config()
            .version_set_provider
            // TODO: rename latest_super_version
            .latest_version()
    }

    // TODO: rename latest_*
    #[doc(hidden)]
    fn current_version(&self) -> Version {
        self.current_super_version().version
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

    /// Returns the disk space used by stale blobs.
    fn stale_blob_bytes(&self) -> u64 {
        0
    }

    /// Gets the disk space usage of all filters in the tree.
    ///
    /// May not correspond to the actual memory size because filter blocks may be paged out.
    fn filter_size(&self) -> u64;

    /// Gets the memory usage of all pinned filters in the tree.
    fn pinned_filter_size(&self) -> usize;

    /// Gets the memory usage of all pinned index blocks in the tree.
    fn pinned_block_index_size(&self) -> usize;

    /// Gets the length of the version free list.
    fn version_set_len(&self) -> usize {
        self.tree_config()
            .version_set_provider
            .version_history_len()
    }

    /// Returns the metrics structure.
    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics>;

    /// Runs a compaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn run_compaction(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        gc_watermark: SeqNo,
    ) -> crate::Result<NewCompactionResult>;

    /// Flushes the write buffer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn new_flush(&self, gc_watermark: SeqNo) -> crate::Result<Option<NewFlushToTablesResult>> {
        use crate::{compaction::stream::CompactionStream, merge::Merger};

        let latest = self.tree_config().version_set_provider.latest_version();

        if latest.sealed_memtables.len() == 0 {
            log::trace!("Flushing aborted because there are no sealed memtables");
            return Ok(None);
        }

        let sealed_ids = latest
            .sealed_memtables
            .iter()
            .map(|mt| mt.id)
            .collect::<Vec<_>>();

        log::debug!("Flushing sealed memtables {sealed_ids:?} to table(s)");

        let flushed_size: u64 = latest.sealed_memtables.iter().map(|mt| mt.size()).sum();

        let merger = Merger::new(
            latest
                .sealed_memtables
                .iter()
                .map(|mt| mt.iter().map(Ok))
                .collect::<Vec<_>>(),
        );
        let stream = CompactionStream::new(merger, gc_watermark);

        let Some((tables, blob_files)) = self.flush_to_tables(stream)? else {
            return Ok(None);
        };

        let mut version_edit = vec![EditRecord::AddTables {
            level: 0,
            tables: tables.iter().map(Table::id).collect(),
        }];

        if let Some(blob_files) = &blob_files {
            if !blob_files.is_empty() {
                version_edit.push(EditRecord::AddBlobFiles {
                    blob_files: blob_files.iter().map(BlobFile::id).collect(),
                });
            }
        }

        let version_edit = VersionEdit::new(version_edit);

        Ok(Some(NewFlushToTablesResult {
            tables,
            blob_files,
            frag_map: None,
            evicted_sealed_memtables: sealed_ids,
            flushed_size,

            version_edit,
        }))
    }

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

    /// Returns the number of sealed memtables.
    fn sealed_memtable_count(&self) -> usize {
        self.tree_config()
            .version_set_provider
            .latest_version()
            .sealed_memtables
            .len()
    }

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

    /// Returns the tree type.
    fn tree_type(&self) -> crate::TreeType {
        // NOTE: This is only really safe to do, because we validate
        // that the config's kv_separation_opts is consistent with the tree type during recovery.
        if self.tree_config().kv_separation_opts.is_some() {
            crate::TreeType::Blob
        } else {
            crate::TreeType::Standard
        }
    }

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
