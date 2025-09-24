// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{
    compaction::MoveDown, segment::multi_writer::MultiWriter, AbstractTree, SeqNo, UserKey,
    UserValue,
};
use std::{path::PathBuf, sync::Arc};

/// Bulk ingestion
///
/// Items NEED to be added in ascending key order.
pub struct Ingestion<'a> {
    folder: PathBuf,
    tree: &'a Tree,
    writer: MultiWriter,
    seqno: SeqNo,
}

impl<'a> Ingestion<'a> {
    /// Creates a new ingestion.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new(tree: &'a Tree) -> crate::Result<Self> {
        let folder = tree.config.path.join(crate::file::SEGMENTS_FOLDER);
        log::debug!("Ingesting into disk segments in {}", folder.display());

        // TODO: 3.0.0 look at tree configuration
        let writer = MultiWriter::new(
            folder.clone(),
            tree.segment_id_counter.clone(),
            64 * 1_024 * 1_024,
        )?
        .use_data_block_hash_ratio(tree.config.data_block_hash_ratio)
        .use_data_block_compression(tree.config.data_block_compression_policy.get(6));

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
        use crate::Segment;

        let results = self.writer.finish()?;

        log::info!("Finished ingestion writer");

        let pin_filter = self.tree.config.filter_block_pinning_policy.get(6);
        let pin_index = self.tree.config.filter_block_pinning_policy.get(6);

        let created_segments = results
            .into_iter()
            .map(|segment_id| -> crate::Result<Segment> {
                // TODO: segment recoverer struct w/ builder pattern
                // Segment::recover()
                //  .pin_filters(true)
                //  .with_metrics(metrics)
                //  .run(path, tree_id, cache, descriptor_table);

                Segment::recover(
                    self.folder.join(segment_id.to_string()),
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

        self.tree.register_segments(&created_segments, None, 0)?;

        let last_level_idx = self
            .tree
            .manifest
            .read()
            .expect("lock is poisoned")
            .last_level_index();

        self.tree
            .compact(Arc::new(MoveDown(0, last_level_idx)), 0)?;

        Ok(())
    }
}
