// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{
    config::FilterPolicyEntry, table::multi_writer::MultiWriter, BlobIndirection, SeqNo, UserKey,
    UserValue,
};
use std::path::PathBuf;

pub const INITIAL_CANONICAL_LEVEL: usize = 1;

/// Bulk ingestion
///
/// Items NEED to be added in ascending key order.
///
/// Ingested data bypasses memtables and is written directly into new tables,
/// using the same table writer configuration that is used for flush and compaction.
pub struct Ingestion<'a> {
    folder: PathBuf,
    tree: &'a Tree,
    pub(crate) writer: MultiWriter,
    seqno: SeqNo,
    last_key: Option<UserKey>,
}

impl<'a> Ingestion<'a> {
    /// Creates a new ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new(tree: &'a Tree) -> crate::Result<Self> {
        let folder = tree.config.path.join(crate::file::TABLES_FOLDER);
        log::debug!("Ingesting into tables in {}", folder.display());

        let index_partitioning = tree
            .config
            .index_block_partitioning_policy
            .get(INITIAL_CANONICAL_LEVEL);

        let filter_partitioning = tree
            .config
            .filter_block_partitioning_policy
            .get(INITIAL_CANONICAL_LEVEL);

        // TODO: maybe create a PrepareMultiWriter that can be used by flush, ingest and compaction worker
        let mut writer = MultiWriter::new(
            folder.clone(),
            tree.table_id_counter.clone(),
            64 * 1_024 * 1_024,
            6,
        )?
        .use_bloom_policy({
            if tree.config.expect_point_read_hits {
                crate::config::BloomConstructionPolicy::BitsPerKey(0.0)
            } else if let FilterPolicyEntry::Bloom(p) =
                tree.config.filter_policy.get(INITIAL_CANONICAL_LEVEL)
            {
                p
            } else {
                crate::config::BloomConstructionPolicy::BitsPerKey(0.0)
            }
        })
        .use_data_block_size(
            tree.config
                .data_block_size_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_data_block_hash_ratio(
            tree.config
                .data_block_hash_ratio_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_data_block_compression(
            tree.config
                .data_block_compression_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_index_block_compression(
            tree.config
                .index_block_compression_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_data_block_restart_interval(
            tree.config
                .data_block_restart_interval_policy
                .get(INITIAL_CANONICAL_LEVEL),
        )
        .use_index_block_restart_interval(
            tree.config
                .index_block_restart_interval_policy
                .get(INITIAL_CANONICAL_LEVEL),
        );

        if index_partitioning {
            writer = writer.use_partitioned_index();
        }
        if filter_partitioning {
            writer = writer.use_partitioned_filter();
        }

        Ok(Self {
            folder,
            tree,
            writer,
            seqno: 0,
            last_key: None,
        })
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(crate) fn write_indirection(
        &mut self,
        key: UserKey,
        indirection: BlobIndirection,
    ) -> crate::Result<()> {
        use crate::coding::Encode;

        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        let cloned_key = key.clone();
        self.writer.write(crate::InternalValue::from_components(
            key,
            indirection.encode_into_vec(),
            self.seqno,
            crate::ValueType::Indirection,
        ))?;

        self.writer.register_blob(indirection);

        // Remember the last user key to validate the next call's ordering
        self.last_key = Some(cloned_key);

        Ok(())
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        if let Some(prev) = &self.last_key {
            assert!(
                key > *prev,
                "next key in ingestion must be greater than last key"
            );
        }

        let cloned_key = key.clone();

        self.writer.write(crate::InternalValue::from_components(
            key,
            value,
            self.seqno,
            crate::ValueType::Value,
        ))?;

        // Remember the last user key to validate the next call's ordering
        self.last_key = Some(cloned_key);

        Ok(())
    }

    /// Writes a key-value pair.
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
        let res = self.writer.write(crate::InternalValue::from_components(
            key,
            crate::UserValue::empty(),
            self.seqno,
            crate::ValueType::Tombstone,
        ));
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
        use crate::{AbstractTree, Table};

        if self.last_key.is_none() {
            log::trace!("No data written to Ingestion, returning early");
            return Ok(());
        }

        // CRITICAL SECTION: Atomic flush + seqno allocation + registration
        //
        // We must ensure no concurrent writes interfere between flushing the
        // active memtable and registering the ingested tables. The sequence is:
        //   1. Acquire flush lock (prevents concurrent flushes)
        //   2. Flush active memtable (ensures no pending writes)
        //   3. Finish ingestion writer (creates table files)
        //   4. Allocate next global seqno (atomic timestamp)
        //   5. Recover tables with that seqno
        //   6. Register version with same seqno
        //
        // Why not flush in new()?
        // If we flushed in new(), there would be a race condition:
        //   new() -> flush -> [TIME PASSES + OTHER WRITES] -> finish() -> seqno
        // The seqno would be disconnected from the flush, violating MVCC.
        //
        // By holding the flush lock throughout, we guarantee atomicity.
        let flush_lock = self.tree.get_flush_lock();

        // Flush any pending memtable writes to ensure ingestion sees a
        // consistent snapshot and lookup order remains correct.
        // We call rotate + flush directly because we already hold the lock.
        self.tree.rotate_memtable();
        self.tree.flush(&flush_lock, 0)?;

        // Finalize the ingestion writer, writing all buffered data to disk.
        let results = self.writer.finish()?;

        log::info!("Finished ingestion writer");

        // Acquire locks for version registration. We must hold both the
        // compaction state lock and version history lock to safely modify
        // the tree's version.
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut _compaction_state = self.tree.compaction_state.lock().expect("lock is poisoned");
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut version_lock = self.tree.version_history.write().expect("lock is poisoned");

        // Allocate the next global sequence number. This seqno will be shared
        // by all ingested tables and the version that registers them, ensuring
        // consistent MVCC snapshots.
        let global_seqno = self.tree.config.seqno.next();

        // Recover all created tables, assigning them the global_seqno we just
        // allocated. This ensures all ingested tables share the same sequence
        // number, which is critical for MVCC correctness.
        //
        // We intentionally do NOT pin filter/index blocks here. Large ingests
        // are typically placed in level 1, and pinning would increase memory
        // pressure unnecessarily.
        let created_tables = results
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    self.folder.join(table_id.to_string()),
                    checksum,
                    global_seqno,
                    self.tree.id,
                    self.tree.config.cache.clone(),
                    self.tree.config.descriptor_table.clone(),
                    false,
                    false,
                    #[cfg(feature = "metrics")]
                    self.tree.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // Upgrade the version with our ingested tables, using the global_seqno
        // we allocated earlier. This ensures the version and all tables share
        // the same sequence number.
        //
        // We use upgrade_version_with_seqno (instead of upgrade_version) because
        // we need precise control over the seqno: it must match the seqno we
        // already assigned to the recovered tables.
        version_lock.upgrade_version_with_seqno(
            &self.tree.config.path,
            |current| {
                let mut copy = current.clone();
                copy.version = copy.version.with_new_l0_run(&created_tables, None, None);
                Ok(copy)
            },
            global_seqno,
            &self.tree.config.visible_seqno,
        )?;

        // Perform maintenance on the version history (e.g., clean up old versions).
        // We use gc_watermark=0 since ingestion doesn't affect sealed memtables.
        if let Err(e) = version_lock.maintenance(&self.tree.config.path, 0) {
            log::warn!("Version GC failed: {e:?}");
        }

        Ok(())
    }
}
