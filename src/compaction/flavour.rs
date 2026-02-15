// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::blob_tree::handle::BlobIndirection;
use crate::blob_tree::FragmentationMap;
use crate::coding::{Decode, Encode};
use crate::compaction::worker::Options;
use crate::compaction::Input as CompactionPayload;
use crate::file::TABLES_FOLDER;
use crate::table::multi_writer::MultiWriter;
use crate::version::{SuperVersions, Version};
use crate::vlog::blob_file::scanner::ScanEntry;
use crate::vlog::{BlobFileId, BlobFileMergeScanner, BlobFileWriter};
use crate::{BlobFile, HashSet, InternalValue, Table};
use std::iter::Peekable;
use std::time::Instant;

/// Drains all blobs that come "before" the given vptr.
fn drain_blobs<I: Iterator<Item = crate::Result<(ScanEntry, BlobFileId)>>>(
    scanner: &mut Peekable<I>,
    key: &[u8],
    vptr: &BlobIndirection,
) -> crate::Result<()> {
    loop {
        let Some(blob) = scanner.next_if(|x| match x {
            Ok((entry, blob_file_id)) => {
                entry.key != key
                    || (*blob_file_id != vptr.vhandle.blob_file_id)
                    || (entry.offset < vptr.vhandle.offset)
            }
            Err(_) => true,
        }) else {
            break;
        };
        let (entry, _) = blob?;

        assert!(entry.key <= key, "vptr was not matched with blob");
    }

    Ok(())
}

pub(super) fn prepare_table_writer(
    version: &Version,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<MultiWriter> {
    let table_base_folder = opts.config.path.join(TABLES_FOLDER);

    let dst_lvl = payload.canonical_level.into();

    let data_block_size = opts.config.data_block_size_policy.get(dst_lvl);

    let data_block_restart_interval = opts.config.data_block_restart_interval_policy.get(dst_lvl);
    let index_block_restart_interval = opts.config.index_block_restart_interval_policy.get(dst_lvl);

    let data_block_compression = opts.config.data_block_compression_policy.get(dst_lvl);
    let index_block_compression = opts.config.index_block_compression_policy.get(dst_lvl);

    let data_block_hash_ratio = opts.config.data_block_hash_ratio_policy.get(dst_lvl);

    let index_partitioning = opts.config.index_block_partitioning_policy.get(dst_lvl);
    let filter_partitioning = opts.config.filter_block_partitioning_policy.get(dst_lvl);

    log::debug!(
        "Compacting tables {:?} into L{} (canonical L{}), target_size={}, data_block_restart_interval={data_block_restart_interval}, index_block_restart_interval={index_block_restart_interval}, data_block_size={data_block_size}, data_block_compression={data_block_compression:?}, index_block_compression={index_block_compression:?}, mvcc_gc_watermark={}",
        payload.table_ids,
        payload.dest_level,
        payload.canonical_level,
        payload.target_size,
        opts.mvcc_gc_watermark,
    );

    let mut table_writer = MultiWriter::new(
        table_base_folder,
        opts.table_id_generator.clone(),
        payload.target_size,
        payload.dest_level,
    )?;

    if index_partitioning {
        table_writer = table_writer.use_partitioned_index();
    }
    if filter_partitioning {
        table_writer = table_writer.use_partitioned_filter();
    }

    #[expect(clippy::cast_possible_truncation, reason = "max key size = u16")]
    let last_level = (version.level_count() - 1) as u8;
    let is_last_level = payload.dest_level == last_level;

    Ok(table_writer
        .use_data_block_restart_interval(data_block_restart_interval)
        .use_index_block_restart_interval(index_block_restart_interval)
        .use_data_block_compression(data_block_compression)
        .use_data_block_size(data_block_size)
        .use_data_block_hash_ratio(data_block_hash_ratio)
        .use_index_block_compression(index_block_compression)
        .use_bloom_policy({
            use crate::config::FilterPolicyEntry::{Bloom, None};
            use crate::table::filter::BloomConstructionPolicy;

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
        })
        .use_prefix_extractor(opts.config.prefix_extractor.clone()))
}

// TODO: find a better name
pub(super) trait CompactionFlavour {
    fn write(&mut self, item: InternalValue) -> crate::Result<()>;

    #[warn(clippy::too_many_arguments)]
    fn finish(
        self: Box<Self>,
        super_version: &mut SuperVersions,
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
    fn drain_blobs(&mut self, key: &[u8], indirection: &BlobIndirection) -> crate::Result<()> {
        drain_blobs(&mut self.blob_scanner, key, indirection)
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

                #[expect(clippy::expect_used, reason = "vptr is expected to match with blob")]
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
        super_version: &mut SuperVersions,
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

        let current_version = super_version.latest_version();

        for blob_file in current_version.version.blob_files.iter() {
            if blob_file.is_dead(current_version.version.gc_stats()) {
                blob_files_to_drop.push(blob_file.clone());
            }
        }

        super_version.upgrade_version(
            &opts.config.path,
            |current| {
                let mut copy = current.clone();

                copy.version = copy.version.with_merge(
                    &payload.table_ids.iter().copied().collect::<Vec<_>>(),
                    &created_tables,
                    payload.dest_level as usize,
                    if blob_frag_map_diff.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map_diff)
                    },
                    created_blob_files,
                    &blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<HashSet<_>>(),
                );

                Ok(copy)
            },
            &opts.global_seqno,
            &opts.visible_seqno,
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
    tables_to_rewrite: Vec<Table>,
}

impl StandardCompaction {
    pub fn new(table_writer: MultiWriter, tables_to_rewrite: Vec<Table>) -> Self {
        Self {
            start: Instant::now(),
            table_writer,
            tables_to_rewrite,
        }
    }

    fn consume_writer(self, opts: &Options, dst_lvl: usize) -> crate::Result<Vec<Table>> {
        let table_base_folder = self.table_writer.base_path.clone();

        let pin_filter = opts.config.filter_block_pinning_policy.get(dst_lvl);
        let pin_index = opts.config.index_block_pinning_policy.get(dst_lvl);

        self.table_writer
            .finish()?
            .into_iter()
            .map(|(table_id, checksum)| -> crate::Result<Table> {
                Table::recover(
                    table_base_folder.join(table_id.to_string()),
                    checksum,
                    0,
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
        super_version: &mut SuperVersions,
        opts: &Options,
        payload: &CompactionPayload,
        dst_lvl: usize,
        blob_frag_map: FragmentationMap,
    ) -> crate::Result<()> {
        log::debug!("Compaction done in {:?}", self.start.elapsed());

        let table_ids_to_delete = std::mem::take(&mut self.tables_to_rewrite);

        let created_tables = self.consume_writer(opts, dst_lvl)?;

        let mut blob_files_to_drop = Vec::default();

        let current_version = super_version.latest_version();

        for blob_file in current_version.version.blob_files.iter() {
            if blob_file.is_dead(current_version.version.gc_stats()) {
                blob_files_to_drop.push(blob_file.clone());
            }
        }

        super_version.upgrade_version(
            &opts.config.path,
            |current| {
                let mut copy = current.clone();

                copy.version = copy.version.with_merge(
                    &payload.table_ids.iter().copied().collect::<Vec<_>>(),
                    &created_tables,
                    payload.dest_level as usize,
                    if blob_frag_map.is_empty() {
                        None
                    } else {
                        Some(blob_frag_map)
                    },
                    Vec::default(),
                    &blob_files_to_drop
                        .iter()
                        .map(BlobFile::id)
                        .collect::<HashSet<_>>(),
                );

                Ok(copy)
            },
            &opts.global_seqno,
            &opts.visible_seqno,
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

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{vlog::ValueHandle, UserKey, UserValue};

    #[expect(clippy::unnecessary_wraps)]
    fn entry(
        blob_file_id: BlobFileId,
        key: &[u8],
        offset: u64,
    ) -> crate::Result<(ScanEntry, BlobFileId)> {
        Ok((
            ScanEntry {
                key: UserKey::from(key),
                offset,
                seqno: 0,
                uncompressed_len: 0,
                value: UserValue::empty(),
            },
            blob_file_id,
        ))
    }

    #[test]
    fn drain_blobs_simple() -> crate::Result<()> {
        let mut iter = [
            entry(0, b"a", 0),
            entry(0, b"a", 1),
            entry(0, b"a", 2),
            entry(0, b"a", 3),
            entry(0, b"a", 4),
        ]
        .into_iter()
        .peekable();

        drain_blobs(
            &mut iter,
            b"a",
            &BlobIndirection {
                size: 0,
                vhandle: ValueHandle {
                    blob_file_id: 0,
                    offset: 4,
                    on_disk_size: 0,
                },
            },
        )?;

        assert_eq!(entry(0, b"a", 4)?, iter.next().unwrap()?);

        Ok(())
    }

    #[test]
    fn drain_blobs_multiple_keys() -> crate::Result<()> {
        let mut iter = [
            entry(0, b"a", 0),
            entry(0, b"b", 0),
            entry(0, b"c", 0),
            entry(0, b"d", 0),
            entry(0, b"e", 0),
        ]
        .into_iter()
        .peekable();

        drain_blobs(
            &mut iter,
            b"e",
            &BlobIndirection {
                size: 0,
                vhandle: ValueHandle {
                    blob_file_id: 0,
                    offset: 0,
                    on_disk_size: 0,
                },
            },
        )?;

        assert_eq!(entry(0, b"e", 0)?, iter.next().unwrap()?);

        Ok(())
    }
}
