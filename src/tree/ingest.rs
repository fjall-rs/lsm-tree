// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{
use std::path::PathBuf;
    segment::{multi_writer::MultiWriter, Segment},
    AbstractTree, UserKey, UserValue, ValueType,
};
use std::{path::PathBuf, sync::Arc};

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
        log::debug!("Ingesting into disk segments in {folder:?}");

        let writer = MultiWriter::new(
            folder.clone(),
            tree.segment_id_counter.clone(),
            64 * 1_024 * 1_024, // TODO: look at tree configuration
        )?
        .use_compression(tree.config.compression);

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
        use crate::compaction::MoveDown;

        let results = self.writer.finish()?;

        log::info!("Finished ingestion writer");

        let created_segments = results
            .into_iter()
            .map(|segment_id| -> crate::Result<Segment> {
                Segment::recover(
                    self.folder.join(segment_id.to_string()),
                    self.tree.id,
                    self.tree.config.cache.clone(),
                    self.tree.config.descriptor_table.clone(),
                    true,
                ) // TODO: look at configuration

                // todo!()

                /* let segment_id = trailer.metadata.id;
                let segment_file_path = self.folder.join(segment_id.to_string());

                let block_index = TwoLevelBlockIndex::from_file(
                    &segment_file_path,
                    &trailer.metadata,
                    trailer.offsets.tli_ptr,
                    (self.tree.id, segment_id).into(),
                    self.tree.config.descriptor_table.clone(),
                    self.tree.config.cache.clone(),
                )?;
                let block_index = BlockIndexImpl::TwoLevel(block_index);
                let block_index = Arc::new(block_index);

                Ok(SegmentInner {
                    tree_id: self.tree.id,

                    descriptor_table: self.tree.config.descriptor_table.clone(),
                    cache: self.tree.config.cache.clone(),

                    metadata: trailer.metadata,
                    offsets: trailer.offsets,

                    #[allow(clippy::needless_borrows_for_generic_args)]
                    block_index,

                    bloom_filter: Segment::load_bloom(
                        &segment_file_path,
                        trailer.offsets.bloom_ptr,
                    )?,

                    path: segment_file_path,
                    is_deleted: AtomicBool::default(),
                }
                .into()) */
            })
            .collect::<crate::Result<Vec<_>>>()?;

        self.tree.register_segments(&created_segments)?;

        self.tree.compact(Arc::new(MoveDown(0, 2)), 0)?;

        Ok(())
    }
}
