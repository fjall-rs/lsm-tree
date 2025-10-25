use std::iter::Peekable;
use std::time::Instant;

use crate::blob_tree::handle::BlobIndirection;
use crate::blob_tree::FragmentationMap;
use crate::coding::{Decode, Encode};
use crate::compaction::state::CompactionState;
use crate::compaction::worker::Options;
use crate::compaction::Input as CompactionPayload;
use crate::file::SEGMENTS_FOLDER;
use crate::segment::multi_writer::MultiWriter;
use crate::tree::inner::SuperVersion;
use crate::version::Version;
use crate::vlog::{BlobFileId, BlobFileMergeScanner, BlobFileWriter};
use crate::{BlobFile, HashSet, InternalValue, Segment};

pub(super) fn prepare_table_writer(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<MultiWriter> {
    let table_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    let dst_lvl = payload.canonical_level.into();

    let data_block_size = opts.config.data_block_size_policy.get(dst_lvl);
    let index_block_size = opts.config.index_block_size_policy.get(dst_lvl);

    let data_block_restart_interval = opts.config.data_block_restart_interval_policy.get(dst_lvl);
    let index_block_restart_interval = opts.config.index_block_restart_interval_policy.get(dst_lvl);

    let data_block_compression = opts.config.data_block_compression_policy.get(dst_lvl);
    let index_block_compression = opts.config.index_block_compression_policy.get(dst_lvl);

    let data_block_hash_ratio = opts.config.data_block_hash_ratio_policy.get(dst_lvl);

    let index_partioning = opts.config.index_block_partitioning_policy.get(dst_lvl);
    let filter_partioning = opts.config.filter_block_partitioning_policy.get(dst_lvl);

    log::debug!(
        "Compacting tables {:?} into L{} (canonical L{}), target_size={}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, index_block_size={index_block_size}, data_block_compression={data_block_compression}, index_block_compression={index_block_compression}, mvcc_gc_watermark={}",
        payload.segment_ids,
        payload.dest_level,
        payload.canonical_level,
        payload.target_size,
        opts.eviction_seqno,
    );

    let mut table_writer = MultiWriter::new(
        table_base_folder,
        opts.segment_id_generator.clone(),
        payload.target_size,
    )?;

    if index_partioning {
        table_writer = table_writer.use_partitioned_index();
    }
    if filter_partioning {
        table_writer = table_writer.use_partitioned_filter();
    }

    let last_level = (version.level_count() - 1) as u8;
    let is_last_level = payload.dest_level == last_level;

    Ok(table_writer
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_data_block_size(data_block_size)
        .use_index_block_size(index_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_index_block_compression(index_block_compression)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::segment::filter::BloomConstructionPolicy;

            if is_last_level && opts.config.expect_point_read_hits {
                BloomConstructionPolicy::BitsPerKey(0.0)
            } else {
                match opts
                    .config
                    .filter_policy
                    .get(usize::from(payload.dest_level))
                {
                    Bloom(policy) => policy,
                    None => BloomConstructionPolicy::BitsPerKey(0.0),
                }
            }
        }))
}

// TODO: 3.0.0 find a good name
pub(super) trait CompactionFlavour {
    fn write(&mut self, item: InternalValue) -> crate::Result<()>;

    #[warn(clippy::too_many_arguments)]
    fn finish(
        self: Box<Self>,
        super_version: &mut SuperVersion,
        state: &mut CompactionState,
        opts: &Options,
        payload: &CompactionPayload,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
    ) -> crate::Result<()>;
}

/// Compaction worker that will relocate blobs that sit in blob files that are being rewritten
pub struct RelocatingCompaction {
    inner: StandardCompaction,
    blob_scanner: Peekable<BlobFileMergeScanner>,
    blob_writer: BlobFileWriter,
    rewriting_blob_file_ids: HashSet<BlobFileId>,
    rewriting_blob_files: Vec<BlobFile>,
}

impl RelocatingCompaction {
    pub fn new(
        inner: StandardCompaction,
        blob_scanner: Peekable<BlobFileMergeScanner>,
        blob_writer: BlobFileWriter,
        rewriting_blob_files: Vec<BlobFile>,
    ) -> Self {
        Self {
            inner,
            blob_scanner,
            blob_writer,
            rewriting_blob_file_ids: rewriting_blob_files.iter().map(BlobFile::id).collect(),
            rewriting_blob_files,
        }
    }

    // TODO: vvv validate/unit test this vvv

    /// Drains all blobs that come "before" the given vptr.
    fn drain_blobs(&mut self, key: &[u8], vptr: &BlobIndirection) -> crate::Result<()> {
        loop {
            let Some(blob) = self.blob_scanner.next_if(|x| match x {
                Ok((entry, blob_file_id)) => {
                    entry.key != key
                        || (*blob_file_id != vptr.vhandle.blob_file_id)
                        || (entry.offset < vptr.vhandle.offset)
                }
                Err(_) => true,
            }) else {
                break;
            };

            match blob {
                Ok((entry, _)) => {
                    assert!(entry.key <= key, "vptr was not matched with blob");
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

impl CompactionFlavour for RelocatingCompaction {
    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_indirection() {
            let mut reader = &item.value[..];

            let Ok(mut indirection) = BlobIndirection::decode_from(&mut reader) else {
                log::error!("Failed to deserialize blob indirection: {item:?}");
                return Ok(());
            };

            log::trace!(
                "{:?}:{} => encountered indirection: {indirection:?}",
                item.key.user_key,
                item.key.seqno,
            );

            if self
                .rewriting_blob_file_ids
                .contains(&indirection.vhandle.blob_file_id)
            {
                self.drain_blobs(&item.key.user_key, &indirection)?;

                let (blob_entry, blob_file_id) = self
                    .blob_scanner
                    .next()
                    .expect("vptr was not matched with blob (scanner is unexpectedly exhausted)")?;

                assert_eq!(
                    blob_file_id, indirection.vhandle.blob_file_id,
                    "matched blob has different blob file ID than vptr",
                );
                assert_eq!(
                    blob_entry.key, item.key.user_key,
                    "matched blob has different key than vptr",
                );
                assert_eq!(
                    blob_entry.offset, indirection.vhandle.offset,
                    "matched blob has different offset than vptr",
                );

                log::trace!(
                    "=> use blob: {:?}:{} offset: {} from BF {}",
                    blob_entry.key,
                    blob_entry.seqno,
                    blob_entry.offset,
                    blob_file_id,
                );

                indirection.vhandle.blob_file_id = self.blob_writer.blob_file_id();
                indirection.vhandle.offset = self.blob_writer.offset();

                log::trace!("RELOCATE to {indirection:?}");

                self.blob_writer.write_raw(
                    &item.key.user_key,
                    item.key.seqno,
                    &blob_entry.value,
                    blob_entry.uncompressed_len,
                )?;

                self.inner
                    .table_writer
                    .write(InternalValue::from_components(
                        item.key.user_key,
                        indirection.encode_into_vec(),
                        item.key.seqno,
                        crate::ValueType::Indirection,
                    ))?;
            } else {
                // This blob is not part of the rewritten blob files
                // So just pass it through
                log::trace!("Pass through {indirection:?} because it is not being relocated");
                self.inner.table_writer.write(item)?;
            }

            self.inner.table_writer.register_blob(indirection);
        } else {
            self.inner.table_writer.write(item)?;
        }

        Ok(())
    }

    fn finish(
        mut self: Box<Self>,
        super_version: &mut SuperVersion,
        state: &mut CompactionState,
        opts: &Options,
        payload: &CompactionPayload,
        dst_lvl: usize,
        blob_frag_map_diff: FragmentationMap,
    ) -> crate::Result<()> {
        log::debug!(
            "Relocating compaction done in {:?}",
            self.inner.start.elapsed(),
        );

        let table_ids_to_delete = std::mem::take(&mut self.inner.tables_to_rewrite);

        let created_tables = self.inner.consume_writer(opts, dst_lvl)?;
        let created_blob_files = self.blob_writer.finish()?;

        let mut blob_files_to_drop = self.rewriting_blob_files;

        for blob_file in super_version.version.blob_files.iter() {
            if blob_file.is_dead(super_version.version.gc_stats()) {
                blob_files_to_drop.push(blob_file.clone());
            }
        }

        state.upgrade_version(
            super_version,
            |current| {
                Ok(current.with_merge(
                    &payload.segment_ids.iter().copied().collect::<Vec<_>>(),
                    &created_tables,
                    payload.dest_level as usize,
                    if blob_frag_map_diff.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map_diff)
                    },
                    created_blob_files,
                    blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<HashSet<_>>(),
                ))
            },
            opts.eviction_seqno,
        )?;

        // NOTE: If the application were to crash >here< it's fine
        // The tables/blob files are not referenced anymore, and will be
        // cleaned up upon recovery
        for table in table_ids_to_delete {
            table.mark_as_deleted();
        }

        for blob_file in blob_files_to_drop {
            blob_file.mark_as_deleted();
        }

        Ok(())
    }
}

/// Standard compaction worker that just passes through all its data
pub struct StandardCompaction {
    start: Instant,
    table_writer: MultiWriter,
    tables_to_rewrite: Vec<Segment>,
}

impl StandardCompaction {
    pub fn new(table_writer: MultiWriter, tables_to_rewrite: Vec<Segment>) -> Self {
        Self {
            start: Instant::now(),
            table_writer,
            tables_to_rewrite,
        }
    }

    fn consume_writer(self, opts: &Options, dst_lvl: usize) -> crate::Result<Vec<Segment>> {
        let table_base_folder = self.table_writer.base_path.clone();

        let pin_filter = opts.config.filter_block_pinning_policy.get(dst_lvl);
        let pin_index = opts.config.filter_block_pinning_policy.get(dst_lvl);

        self.table_writer
            .finish()?
            .into_iter()
            .map(|table_id| -> crate::Result<Segment> {
                Segment::recover(
                    table_base_folder.join(table_id.to_string()),
                    opts.tree_id,
                    opts.config.cache.clone(),
                    opts.config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    opts.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()
    }
}

impl CompactionFlavour for StandardCompaction {
    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let indirection = if item.key.value_type.is_indirection() {
            Some({
                let mut reader = &item.value[..];
                BlobIndirection::decode_from(&mut reader)?
            })
        } else {
            None
        };

        self.table_writer.write(item)?;

        if let Some(indirection) = indirection {
            self.table_writer.register_blob(indirection);
        }

        Ok(())
    }

    fn finish(
        mut self: Box<Self>,
        super_version: &mut SuperVersion,
        state: &mut CompactionState,
        opts: &Options,
        payload: &CompactionPayload,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
    ) -> crate::Result<()> {
        log::debug!("Compaction done in {:?}", self.start.elapsed());

        let table_ids_to_delete = std::mem::take(&mut self.tables_to_rewrite);

        let created_segments = self.consume_writer(opts, dst_lvl)?;

        let mut blob_files_to_drop = Vec::default();

        for blob_file in super_version.version.blob_files.iter() {
            if blob_file.is_dead(super_version.version.gc_stats()) {
                blob_files_to_drop.push(blob_file.clone());
            }
        }

        state.upgrade_version(
            super_version,
            |current| {
                Ok(current.with_merge(
                    &payload.segment_ids.iter().copied().collect::<Vec<_>>(),
                    &created_segments,
                    payload.dest_level as usize,
                    if blob_frag_map.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map)
                    },
                    Vec::default(),
                    blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<HashSet<_>>(),
                ))
            },
            opts.eviction_seqno,
        )?;

        // NOTE: If the application were to crash >here< it's fine
        // The tables are not referenced anymore, and will be
        // cleaned up upon recovery
        for table in table_ids_to_delete {
            table.mark_as_deleted();
        }

        for blob_file in blob_files_to_drop {
            blob_file.mark_as_deleted();
        }

        Ok(())
    }
}
