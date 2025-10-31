// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{
    compaction::MoveDown, config::FilterPolicyEntry, table::multi_writer::MultiWriter,
    AbstractTree, BlobIndirection, SeqNo, UserKey, UserValue,
};
use std::{path::PathBuf, sync::Arc};

pub const INITIAL_CANONICAL_LEVEL: usize = 1;

/// Bulk ingestion
///
/// Items NEED to be added in ascending key order.
pub struct Ingestion<'a> {
    folder: PathBuf,
    tree: &'a Tree,
    pub(crate) writer: MultiWriter,
    seqno: SeqNo,
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
            if let FilterPolicyEntry::Bloom(p) =
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
        })
    }

    /// Sets the ingestion seqno.
    #[must_use]
    pub fn with_seqno(mut self, seqno: SeqNo) -> Self {
        self.seqno = seqno;
        self
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

        self.writer.write(crate::InternalValue::from_components(
            key,
            indirection.encode_into_vec(),
            self.seqno,
            crate::ValueType::Indirection,
        ))?;

        self.writer.register_blob(indirection);

        Ok(())
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        self.writer.write(crate::InternalValue::from_components(
            key,
            value,
            self.seqno,
            crate::ValueType::Value,
        ))
    }

    /// Writes a key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn write_tombstone(&mut self, key: UserKey) -> crate::Result<()> {
        self.writer.write(crate::InternalValue::from_components(
            key,
            crate::UserValue::empty(),
            self.seqno,
            crate::ValueType::Tombstone,
        ))
    }

    /// Finishes the ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn finish(self) -> crate::Result<()> {
        use crate::Table;

        let results = self.writer.finish()?;

        log::info!("Finished ingestion writer");

        let pin_filter = self
            .tree
            .config
            .filter_block_pinning_policy
            .get(INITIAL_CANONICAL_LEVEL);

        let pin_index = self
            .tree
            .config
            .filter_block_pinning_policy
            .get(INITIAL_CANONICAL_LEVEL);

        let created_tables = results
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                // TODO: table recoverer struct w/ builder pattern
                // Table::recover()
                //  .pin_filters(true)
                //  .with_metrics(metrics)
                //  .run(path, tree_id, cache, descriptor_table);

                Table::recover(
                    self.folder.join(table_id.to_string()),
                    checksum,
                    self.tree.id,
                    self.tree.config.cache.clone(),
                    self.tree.config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    self.tree.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        self.tree.register_tables(&created_tables, None, None, 0)?;

        let last_level_idx = self.tree.config.level_count - 1;

        self.tree
            .compact(Arc::new(MoveDown(0, last_level_idx)), 0)?;

        Ok(())
    }
}
