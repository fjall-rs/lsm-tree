// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::handle::BlobIndirection,
    file::BLOBS_FOLDER,
    table::Table,
    tree::ingest::Ingestion as TableIngestion,
    vlog::{BlobFileWriter, ValueHandle},
    SeqNo, UserKey, UserValue,
};

/// Bulk ingestion for [`BlobTree`]
///
/// Items NEED to be added in ascending key order.
///
/// Uses table ingestion for the index and a blob file writer for large
/// values so both streams advance together.
pub struct BlobIngestion<'a> {
    tree: &'a crate::BlobTree,
    pub(crate) table: TableIngestion<'a>,
    pub(crate) blob: BlobFileWriter,
    seqno: SeqNo,
    separation_threshold: u32,
    last_key: Option<UserKey>,
}

impl<'a> BlobIngestion<'a> {
    /// Creates a new ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new(tree: &'a crate::BlobTree) -> crate::Result<Self> {
        #[expect(
            clippy::expect_used,
            reason = "cannot define blob tree without kv separation options"
        )]
        let kv = tree
            .index
            .config
            .kv_separation_opts
            .as_ref()
            .expect("kv separation options should exist");

        let blob_file_size = kv.file_target_size;

        let table = TableIngestion::new(&tree.index)?;
        let blob = BlobFileWriter::new(
            tree.index.0.blob_file_id_counter.clone(),
            tree.index.config.path.join(BLOBS_FOLDER),
        )?
        .use_target_size(blob_file_size)
        .use_compression(kv.compression);

        let separation_threshold = kv.separation_threshold;

        Ok(Self {
            tree,
            table,
            blob,
            seqno: 0,
            separation_threshold,
            last_key: None,
        })
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        // Check order before any blob I/O to avoid partial writes on failure
        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        #[expect(clippy::cast_possible_truncation)]
        let value_size = value.len() as u32;

        if value_size >= self.separation_threshold {
            let offset = self.blob.offset();
            let blob_file_id = self.blob.blob_file_id();
            let on_disk_size = self.blob.write(&key, self.seqno, &value)?;

            let indirection = BlobIndirection {
                vhandle: ValueHandle {
                    blob_file_id,
                    offset,
                    on_disk_size,
                },
                size: value_size,
            };

            let cloned_key = key.clone();
            let res = self.table.write_indirection(key, indirection);
            if res.is_ok() {
                self.last_key = Some(cloned_key);
            }
            res
        } else {
            let cloned_key = key.clone();
            let res = self.table.write(key, value);
            if res.is_ok() {
                self.last_key = Some(cloned_key);
            }
            res
        }
    }

    /// Writes a tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        let cloned_key = key.clone();
        let res = self.table.write_tombstone(key);
        if res.is_ok() {
            self.last_key = Some(cloned_key);
        }
        res
    }

    /// Writes a weak tombstone for a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write_weak_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        let cloned_key = key.clone();
        let res = self.table.write_weak_tombstone(key);
        if res.is_ok() {
            self.last_key = Some(cloned_key);
        }
        res
    }

    /// Finishes the ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::significant_drop_tightening)]
    pub fn finish(self) -> crate::Result<()> {
        use crate::AbstractTree;

        let index = self.index().clone();

        // CRITICAL SECTION: Atomic flush + seqno allocation + registration
        //
        // For BlobTree, we must coordinate THREE components atomically:
        //   1. Index tree memtable flush
        //   2. Value log blob files
        //   3. Index tree tables (with blob indirections)
        //
        // The sequence ensures all components see the same global_seqno:
        //   1. Acquire flush lock on index tree
        //   2. Flush index tree active memtable
        //   3. Finalize blob writer (creates blob files)
        //   4. Finalize table writer (creates index tables)
        //   5. Allocate next global seqno
        //   6. Recover tables with that seqno
        //   7. Register version with same seqno + blob files
        //
        // This prevents race conditions where blob files and their index
        // entries could have mismatched sequence numbers.
        let flush_lock = index.get_flush_lock();

        // Flush any pending index memtable writes to ensure ingestion sees
        // a consistent snapshot of the index.
        // We call rotate + flush directly because we already hold the lock.
        index.rotate_memtable();
        index.flush(&flush_lock, 0)?;

        // Finalize the blob writer first, ensuring all large values are
        // written to blob files before we finalize the index tables that
        // reference them.
        let blob_files = self.blob.finish()?;

        // Finalize the table writer, creating index tables with blob
        // indirections pointing to the blob files we just created.
        let results = self.table.writer.finish()?;

        // Acquire locks for version registration on the index tree. We must
        // hold both the compaction state lock and version history lock to
        // safely modify the tree's version.
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut _compaction_state = index.compaction_state.lock().expect("lock is poisoned");
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut version_lock = index.version_history.write().expect("lock is poisoned");

        // Allocate the next global sequence number. This seqno will be shared
        // by all ingested tables, blob files, and the version that registers
        // them, ensuring consistent MVCC snapshots across the value log.
        let global_seqno = index.config.seqno.next();

        // Recover all created index tables, assigning them the global_seqno
        // we just allocated. These tables contain indirections to the blob
        // files created above, so they must share the same sequence number
        // for MVCC correctness.
        //
        // We intentionally do NOT pin filter/index blocks here. Large ingests
        // are typically placed in level 1, and pinning would increase memory
        // pressure unnecessarily.
        let created_tables = results
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    index
                        .config
                        .path
                        .join(crate::file::TABLES_FOLDER)
                        .join(table_id.to_string()),
                    checksum,
                    global_seqno,
                    index.id,
                    index.config.cache.clone(),
                    index.config.descriptor_table.clone(),
                    false,
                    false,
                    #[cfg(feature = "metrics")]
                    index.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Upgrade the version with our ingested tables and blob files, using
        // the global_seqno we allocated earlier. This ensures the version,
        // tables, and blob files all share the same sequence number, which is
        // critical for GC correctness - we must not delete blob files that are
        // still referenced by visible snapshots.
        //
        // We use upgrade_version_with_seqno (instead of upgrade_version) because
        // we need precise control over the seqno: it must match the seqno we
        // already assigned to the recovered tables.
        version_lock.upgrade_version_with_seqno(
            &index.config.path,
            |current| {
                let mut copy = current.clone();
                copy.version =
                    copy.version
                        .with_new_l0_run(&created_tables, Some(&blob_files), None);
                Ok(copy)
            },
            global_seqno,
            &self.tree.index.config.visible_seqno,
        )?;

        // Perform maintenance on the version history (e.g., clean up old versions).
        // We use gc_watermark=0 since ingestion doesn't affect sealed memtables.
        if let Err(e) = version_lock.maintenance(&index.config.path, 0) {
            log::warn!("Version GC failed: {e:?}");
        }

        Ok(())
    }

    #[inline]
    fn index(&self) -> &crate::Tree {
        &self.tree.index
    }
}
