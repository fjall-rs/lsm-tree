use std::iter::Peekable;
use std::time::Instant;

use crate::blob_tree::handle::BlobIndirection;
use crate::blob_tree::FragmentationMap;
use crate::coding::{Decode, Encode};
use crate::compaction::worker::Options;
use crate::compaction::Input as CompactionPayload;
use crate::file::SEGMENTS_FOLDER;
use crate::level_manifest::LevelManifest;
use crate::segment::multi_writer::MultiWriter;
use crate::version::Version;
use crate::vlog::{BlobFileId, BlobFileMergeScanner, BlobFileWriter};
use crate::{BlobFile, HashSet, InternalValue, Segment};

pub(super) fn prepare_table_writer(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<MultiWriter> {
    let segments_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    let dst_lvl = payload.canonical_level.into();

    let data_block_size = opts.config.data_block_size_policy.get(dst_lvl);
    let index_block_size = opts.config.index_block_size_policy.get(dst_lvl);

    let data_block_restart_interval = opts.config.data_block_restart_interval_policy.get(dst_lvl);
    let index_block_restart_interval = opts.config.index_block_restart_interval_policy.get(dst_lvl);

    let data_block_compression = opts.config.data_block_compression_policy.get(dst_lvl);
    let index_block_compression = opts.config.index_block_compression_policy.get(dst_lvl);

    let data_block_hash_ratio = opts.config.data_block_hash_ratio_policy.get(dst_lvl);

    let table_writer = MultiWriter::new(
        segments_base_folder,
        opts.segment_id_generator.clone(),
        payload.target_size,
    )?;

    let last_level = (version.level_count() - 1) as u8;
    let is_last_level = payload.dest_level == last_level;

    log::debug!(
          "Compacting tables {:?} into L{} (canonical L{}), data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, index_block_size={index_block_size}, data_block_compression={data_block_compression}, index_block_compression={index_block_compression}, mvcc_gc_watermark={}",
          payload.segment_ids,
          payload.dest_level,
          payload.canonical_level,
          opts.eviction_seqno,
      );

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

    fn finish(
        self: Box<Self>,
        levels: &mut LevelManifest,
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

impl CompactionFlavour for RelocatingCompaction {
    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_indirection() {
            let mut reader = &item.value[..];

            let Ok(mut indirection) = BlobIndirection::decode_from(&mut reader) else {
                log::error!("Failed to deserialize blob indirection: {item:?}");
                return Ok(());
            };

            log::debug!(
                "{:?}:{} => encountered indirection: {indirection:?}",
                item.key.user_key,
                item.key.seqno,
            );

            if self
                .rewriting_blob_file_ids
                .contains(&indirection.vhandle.blob_file_id)
            {
                loop {
                    // TODO: uglyyyy
                    let blob = self
                        .blob_scanner
                        .peek()
                        .expect("should have enough blob entries");

                    if let Ok((entry, blob_file_id)) = blob {
                        if self.rewriting_blob_file_ids.contains(blob_file_id) {
                            // This blob is part of the rewritten blob files
                            if entry.key < item.key.user_key {
                                self.blob_scanner.next().expect("should exist")?;
                                continue;
                            }

                            if entry.key == item.key.user_key {
                                if *blob_file_id < indirection.vhandle.blob_file_id {
                                    self.blob_scanner.next().expect("should exist")?;
                                    continue;
                                }
                                if entry.offset < indirection.vhandle.offset {
                                    self.blob_scanner.next().expect("should exist")?;
                                    continue;
                                }
                                if entry.offset == indirection.vhandle.offset {
                                    // This is the blob we need
                                    break;
                                }
                            }
                            assert!(
                                (entry.key > item.key.user_key),
                                "we passed vptr without getting blob",
                            );
                            break;
                        }

                        break;
                    }

                    let e = self.blob_scanner.next().expect("should exist");
                    return Err(e.expect_err("should be error"));
                }

                let blob = self.blob_scanner.next().expect("should have blob")?;

                log::info!(
                    "=> use blob: {:?}:{} offset: {} from BF {}",
                    blob.0.key,
                    blob.0.seqno,
                    blob.0.offset,
                    blob.1,
                );

                indirection.vhandle.blob_file_id = self.blob_writer.blob_file_id();
                indirection.vhandle.offset = self.blob_writer.offset();

                log::debug!("RELOCATE to {indirection:?}");

                self.blob_writer
                    .write(&item.key.user_key, item.key.seqno, &blob.0.value)?;

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
                self.inner.table_writer.register_blob(indirection);
                self.inner.table_writer.write(item)?;
            }
        } else {
            self.inner.table_writer.write(item)?;
        }

        Ok(())
    }

    fn finish(
        mut self: Box<Self>,
        levels: &mut LevelManifest,
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

        let mut blob_file_ids_to_drop = self.rewriting_blob_file_ids;

        for blob_file in levels.current_version().value_log.values() {
            if blob_file.is_dead(levels.current_version().gc_stats()) {
                blob_file_ids_to_drop.insert(blob_file.id());
                self.rewriting_blob_files.push(blob_file.clone());
            }
        }

        levels.atomic_swap(
            |current| {
                current.with_merge(
                    &payload.segment_ids.iter().copied().collect::<Vec<_>>(),
                    &created_tables,
                    payload.dest_level as usize,
                    if blob_frag_map_diff.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map_diff)
                    },
                    created_blob_files,
                    blob_file_ids_to_drop,
                )
            },
            opts.eviction_seqno,
        )?;

        // NOTE: If the application were to crash >here< it's fine
        // The tables/blob files are not referenced anymore, and will be
        // cleaned up upon recovery
        for table in table_ids_to_delete {
            table.mark_as_deleted();
        }

        for blob_file in self.rewriting_blob_files {
            blob_file.mark_as_deleted();
        }

        Ok(())
    }
}

impl RelocatingCompaction {
    pub fn new(
        inner: StandardCompaction,
        blob_scanner: Peekable<BlobFileMergeScanner>,
        blob_writer: BlobFileWriter,
        rewriting_blob_file_ids: HashSet<BlobFileId>,
        rewriting_blob_files: Vec<BlobFile>,
    ) -> Self {
        Self {
            inner,
            blob_scanner,
            blob_writer,
            rewriting_blob_file_ids,
            rewriting_blob_files,
        }
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

    fn register_blob(&mut self, indirection: BlobIndirection) {
        self.table_writer.register_blob(indirection);
    }

    fn consume_writer(self, opts: &Options, dst_lvl: usize) -> crate::Result<Vec<Segment>> {
        let segments_base_folder = self.table_writer.base_path.clone();

        let pin_filter = opts.config.filter_block_pinning_policy.get(dst_lvl);
        let pin_index = opts.config.filter_block_pinning_policy.get(dst_lvl);

        let writer_results = self.table_writer.finish()?;

        let created_segments = writer_results
            .into_iter()
            .map(|segment_id| -> crate::Result<Segment> {
                Segment::recover(
                    segments_base_folder.join(segment_id.to_string()),
                    opts.tree_id,
                    opts.config.cache.clone(),
                    opts.config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    opts.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(created_segments)
    }
}

impl CompactionFlavour for StandardCompaction {
    fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.key.value_type.is_indirection() {
            let mut reader = &item.value[..];
            let indirection = BlobIndirection::decode_from(&mut reader)?;
            self.register_blob(indirection);
        }

        self.table_writer.write(item)
    }

    fn finish(
        mut self: Box<Self>,
        levels: &mut LevelManifest,
        opts: &Options,
        payload: &CompactionPayload,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
    ) -> crate::Result<()> {
        log::debug!("Compaction done in {:?}", self.start.elapsed());

        let table_ids_to_delete = std::mem::take(&mut self.tables_to_rewrite);

        let created_segments = self.consume_writer(opts, dst_lvl)?;

        let mut blob_files_to_drop = Vec::default();

        for blob_file in levels.current_version().value_log.values() {
            if blob_file.is_dead(levels.current_version().gc_stats()) {
                blob_files_to_drop.push(blob_file.clone());
            }
        }

        levels.atomic_swap(
            |current| {
                current.with_merge(
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
                )
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
