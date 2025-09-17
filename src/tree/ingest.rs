// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{segment::multi_writer::MultiWriter, AbstractTree, UserKey, UserValue};
use std::path::PathBuf;

pub struct Ingestion<'a> {
    folder: PathBuf,
    tree: &'a Tree,
    writer: MultiWriter,
}

impl<'a> Ingestion<'a> {
    pub fn new(tree: &'a Tree) -> crate::Result<Self> {
        assert_eq!(
            0,
            tree.segment_count(),
            "can only perform bulk_ingest on empty trees",
        );

        let folder = tree.config.path.join(crate::file::SEGMENTS_FOLDER);
        log::debug!("Ingesting into disk segments in {}", folder.display());

        let writer = MultiWriter::new(
            folder.clone(),
            tree.segment_id_counter.clone(),
            64 * 1_024 * 1_024, // TODO: look at tree configuration
        )?
        // TODO: use restart interval etc.
        .use_data_block_hash_ratio(tree.config.data_block_hash_ratio)
        .use_data_block_compression(tree.config.compression);

        Ok(Self {
            folder,
            tree,
            writer,
        })
    }

    pub fn write(&mut self, key: UserKey, value: UserValue) -> crate::Result<()> {
        self.writer.write(crate::InternalValue::from_components(
            key,
            value,
            0,
            crate::ValueType::Value,
        ))
    }

    pub fn finish(self) -> crate::Result<()> {
        use crate::{compaction::MoveDown, Segment};
        use std::sync::Arc;

        let results = self.writer.finish()?;

        log::info!("Finished ingestion writer");

        let created_segments = results
            .into_iter()
            .map(|segment_id| -> crate::Result<Segment> {
                // TODO: look at tree configuration

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
                    self.tree.config.prefix_extractor.clone(),
                    false,
                    false,
                    #[cfg(feature = "metrics")]
                    self.tree.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        self.tree.register_segments(&created_segments, 0)?;

        self.tree.compact(Arc::new(MoveDown(0, 2)), 0)?;

        Ok(())
    }
}
