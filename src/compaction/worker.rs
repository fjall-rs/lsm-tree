// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use super::{CompactionStrategy, Input as CompactionPayload};
use crate::{
    compaction::{stream::CompactionStream, Choice},
    file::SEGMENTS_FOLDER,
    level_manifest::LevelManifest,
    merge::Merger,
    run_scanner::RunScanner,
    segment::{multi_writer::MultiWriter, Segment},
    stop_signal::StopSignal,
    tree::inner::TreeId,
    Config, InternalValue, SegmentId, SeqNo,
};
use std::{
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockWriteGuard},
    time::Instant,
};

pub type CompactionReader<'a> = Box<dyn Iterator<Item = crate::Result<InternalValue>> + 'a>;

/// Compaction options
pub struct Options {
    pub tree_id: TreeId,

    pub segment_id_generator: Arc<AtomicU64>,

    /// Configuration of tree.
    pub config: Config,

    /// Levels manifest.
    pub levels: Arc<RwLock<LevelManifest>>,

    /// Compaction strategy to use.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal to interrupt a compaction worker in case
    /// the tree is dropped.
    pub stop_signal: StopSignal,

    /// Evicts items that are older than this seqno (MVCC GC).
    pub eviction_seqno: u64,

    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            tree_id: tree.id,
            segment_id_generator: tree.segment_id_counter.clone(),
            config: tree.config.clone(),
            levels: tree.manifest.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
            eviction_seqno: 0,
            #[cfg(feature = "metrics")]
            metrics: tree.metrics.clone(),
        }
    }
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<()> {
    log::trace!("Acquiring levels manifest lock");
    let original_levels = opts.levels.write().expect("lock is poisoned");

    let start = Instant::now();
    log::trace!(
        "Consulting compaction strategy {:?}",
        opts.strategy.get_name(),
    );
    let choice = opts.strategy.choose(&original_levels, &opts.config);

    log::debug!("Compaction choice: {choice:?} in {:?}", start.elapsed());

    match choice {
        Choice::Merge(payload) => merge_segments(original_levels, opts, &payload),
        Choice::Move(payload) => move_segments(original_levels, opts, &payload),
        Choice::Drop(payload) => drop_segments(
            original_levels,
            opts,
            &payload.into_iter().collect::<Vec<_>>(),
        ),
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
            Ok(())
        }
    }
}

fn create_compaction_stream<'a>(
    levels: &LevelManifest,
    to_compact: &[SegmentId],
    eviction_seqno: SeqNo,
) -> crate::Result<Option<CompactionStream<Merger<CompactionReader<'a>>>>> {
    let mut readers: Vec<CompactionReader<'_>> = vec![];
    let mut found = 0;

    for level in levels.current_version().iter_levels() {
        if level.is_empty() {
            continue;
        }

        if level.is_disjoint() && level.len() > 1 {
            let run = level.first().expect("run should exist");

            let Some(lo) = run
                .iter()
                .enumerate()
                .filter(|(_, segment)| to_compact.contains(&segment.id()))
                .min_by(|(a, _), (b, _)| a.cmp(b))
                .map(|(idx, _)| idx)
            else {
                continue;
            };

            let Some(hi) = run
                .iter()
                .enumerate()
                .filter(|(_, segment)| to_compact.contains(&segment.id()))
                .max_by(|(a, _), (b, _)| a.cmp(b))
                .map(|(idx, _)| idx)
            else {
                continue;
            };

            readers.push(Box::new(RunScanner::culled(
                run.clone(),
                (Some(lo), Some(hi)),
            )?));

            found += hi - lo + 1;
        } else {
            for segment in level
                .iter()
                .flat_map(|x| x.iter())
                .filter(|x| to_compact.contains(&x.metadata.id))
            {
                found += 1;
                readers.push(Box::new(segment.scan()?));
            }
        }
    }

    Ok(if found == to_compact.len() {
        Some(CompactionStream::new(Merger::new(readers), eviction_seqno))
    } else {
        None
    })
}

fn move_segments(
    mut levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    // Fail-safe for buggy compaction strategies
    if levels.should_decline_compaction(payload.segment_ids.iter().copied()) {
        log::warn!(
        "Compaction task created by {:?} contained hidden segments, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
        opts.strategy.get_name(),
    );
        return Ok(());
    }

    let segment_ids = payload.segment_ids.iter().copied().collect::<Vec<_>>();

    levels.atomic_swap(
        |current| current.with_moved(&segment_ids, payload.dest_level as usize),
        opts.eviction_seqno,
    )?;

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn merge_segments(
    mut levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    if opts.stop_signal.is_stopped() {
        log::debug!("Stopping before compaction because of stop signal");
        return Ok(());
    }

    // Fail-safe for buggy compaction strategies
    if levels.should_decline_compaction(payload.segment_ids.iter().copied()) {
        log::warn!(
            "Compaction task created by {:?} contained hidden segments, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(());
    }

    let Some(segments) = payload
        .segment_ids
        .iter()
        .map(|&id| levels.get_segment(id).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
            "Compaction task created by {:?} contained segments not referenced in the level manifest",
            opts.strategy.get_name(),
        );
        return Ok(());
    };

    let segments_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    log::debug!(
        "Compacting segments {:?} into L{}, compression={}, mvcc_gc_watermark={}",
        payload.segment_ids,
        payload.dest_level,
        opts.config.compression,
        opts.eviction_seqno,
    );

    let Some(merge_iter) = create_compaction_stream(
        &levels,
        &payload.segment_ids.iter().copied().collect::<Vec<_>>(),
        opts.eviction_seqno,
    )?
    else {
        log::warn!(
            "Compaction task tried to compact segments that do not exist, declining to run it"
        );
        return Ok(());
    };

    let last_level = levels.last_level_index();

    levels.hide_segments(payload.segment_ids.iter().copied());

    // IMPORTANT: Free lock so the compaction (which may go on for a while)
    // does not block possible other compactions and reads
    drop(levels);

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let is_last_level = payload.dest_level == last_level;

    let start = Instant::now();

    let segment_writer = match MultiWriter::new(
        segments_base_folder.clone(),
        opts.segment_id_generator.clone(),
        payload.target_size,
    ) {
        Ok(v) => v,
        Err(e) => {
            log::error!("Compaction failed: {e:?}");

            // IMPORTANT: Show the segments again, because compaction failed
            opts.levels
                .write()
                .expect("lock is poisoned")
                .show_segments(payload.segment_ids.iter().copied());

            return Ok(());
        }
    };

    let mut segment_writer = segment_writer
        .use_data_block_restart_interval(16)
        .use_data_block_compression(opts.config.compression)
        .use_data_block_size(opts.config.data_block_size)
        .use_data_block_hash_ratio(opts.config.data_block_hash_ratio)
        .use_bloom_policy({
            use crate::segment::filter::BloomConstructionPolicy;

            if opts.config.bloom_bits_per_key >= 0 {
                // TODO:
                // NOTE: Apply some MONKEY to have very high FPR on small levels
                // because it's cheap
                //
                // See https://nivdayan.github.io/monkeykeyvaluestore.pdf
                /* match payload.dest_level {
                    0 => BloomConstructionPolicy::FpRate(0.00001),
                    1 => BloomConstructionPolicy::FpRate(0.0005),
                    _ => BloomConstructionPolicy::BitsPerKey(
                        opts.config.bloom_bits_per_key.unsigned_abs(),
                    ),
                } */
                BloomConstructionPolicy::BitsPerKey(opts.config.bloom_bits_per_key.unsigned_abs())
            } else {
                BloomConstructionPolicy::BitsPerKey(0)
            }
        });

    for (idx, item) in merge_iter.enumerate() {
        let item = match item {
            Ok(v) => v,
            Err(e) => {
                log::error!("Compaction failed: {e:?}");

                // IMPORTANT: Show the segments again, because compaction failed
                opts.levels
                    .write()
                    .expect("lock is poisoned")
                    .show_segments(payload.segment_ids.iter().copied());

                return Ok(());
            }
        };

        // IMPORTANT: We can only drop tombstones when writing into last level
        if is_last_level && item.is_tombstone() {
            continue;
        }

        if let Err(e) = segment_writer.write(item) {
            log::error!("Compaction failed: {e:?}");

            // IMPORTANT: Show the segments again, because compaction failed
            opts.levels
                .write()
                .expect("lock is poisoned")
                .show_segments(payload.segment_ids.iter().copied());

            return Ok(());
        }

        if idx % 1_000_000 == 0 && opts.stop_signal.is_stopped() {
            log::debug!("compactor: stopping amidst compaction because of stop signal");
            return Ok(());
        }
    }

    let writer_results = match segment_writer.finish() {
        Ok(v) => v,
        Err(e) => {
            log::error!("Compaction failed: {e:?}");

            // IMPORTANT: Show the segments again, because compaction failed
            opts.levels
                .write()
                .expect("lock is poisoned")
                .show_segments(payload.segment_ids.iter().copied());

            return Ok(());
        }
    };

    log::debug!(
        "Compacted in {:?} ({} segments created)",
        start.elapsed(),
        writer_results.len(),
    );

    let created_segments = writer_results
        .into_iter()
        .map(|segment_id| -> crate::Result<Segment> {
            Segment::recover(
                segments_base_folder.join(segment_id.to_string()),
                opts.tree_id,
                opts.config.cache.clone(),
                opts.config.descriptor_table.clone(),
                opts.config.prefix_extractor.clone(),
                payload.dest_level <= 1, // TODO: look at configuration
                payload.dest_level <= 2, // TODO: look at configuration
                #[cfg(feature = "metrics")]
                opts.metrics.clone(),
            )

            /* let segment_id = trailer.metadata.id;
            let segment_file_path = segments_base_folder.join(segment_id.to_string());

            let block_index = match payload.dest_level {
                0 | 1 => {
                    let block_index = FullBlockIndex::from_file(
                        &segment_file_path,
                        &trailer.metadata,
                        &trailer.offsets,
                    )?;
                    BlockIndexImpl::Full(block_index)
                }
                _ => {
                    // NOTE: Need to allow because of false positive in Clippy
                    // because of "bloom" feature
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    let block_index = TwoLevelBlockIndex::from_file(
                        &segment_file_path,
                        &trailer.metadata,
                        trailer.offsets.tli_ptr,
                        (opts.tree_id, segment_id).into(),
                        opts.config.descriptor_table.clone(),
                        opts.config.cache.clone(),
                    )?;
                    BlockIndexImpl::TwoLevel(block_index)
                }
            };
            let block_index = Arc::new(block_index);

            let bloom_filter = Segment::load_bloom(&segment_file_path, trailer.offsets.bloom_ptr)?;

            Ok(SegmentInner {
                path: segment_file_path,

                tree_id: opts.tree_id,

                descriptor_table: opts.config.descriptor_table.clone(),
                cache: opts.config.cache.clone(),

                metadata: trailer.metadata,
                offsets: trailer.offsets,

                #[allow(clippy::needless_borrows_for_generic_args)]
                block_index,

                bloom_filter,

                is_deleted: AtomicBool::default(),
            }
            .into()) */
        })
        .collect::<crate::Result<Vec<_>>>();

    let created_segments = match created_segments {
        Ok(v) => v,
        Err(e) => {
            log::error!("Compaction failed: {e:?}");

            // IMPORTANT: Show the segments again, because compaction failed
            opts.levels
                .write()
                .expect("lock is poisoned")
                .show_segments(payload.segment_ids.iter().copied());

            return Ok(());
        }
    };

    // NOTE: Mind lock order L -> M -> S
    log::trace!("compactor: acquiring levels manifest write lock");
    let mut levels = opts.levels.write().expect("lock is poisoned");
    log::trace!("compactor: acquired levels manifest write lock");

    let swap_result = levels.atomic_swap(
        |current| {
            current.with_merge(
                &payload.segment_ids.iter().copied().collect::<Vec<_>>(),
                &created_segments,
                payload.dest_level as usize,
            )
        },
        opts.eviction_seqno,
    );

    if let Err(e) = swap_result {
        // IMPORTANT: Show the segments again, because compaction failed
        levels.show_segments(payload.segment_ids.iter().copied());
        return Err(e);
    }

    // NOTE: If the application were to crash >here< it's fine
    // The segments are not referenced anymore, and will be
    // cleaned up upon recovery
    for segment in segments {
        segment.mark_as_deleted();
    }

    levels.show_segments(payload.segment_ids.iter().copied());

    if let Err(e) = levels.maintenance(opts.eviction_seqno) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    drop(levels);

    log::trace!("Compaction successful");

    Ok(())
}

fn drop_segments(
    mut levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    ids_to_drop: &[SegmentId],
) -> crate::Result<()> {
    // Fail-safe for buggy compaction strategies
    if levels.should_decline_compaction(ids_to_drop.iter().copied()) {
        log::warn!(
            "Compaction task created by {:?} contained hidden segments, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(());
    }

    let Some(segments) = ids_to_drop
        .iter()
        .map(|&id| levels.get_segment(id).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
        "Compaction task created by {:?} contained segments not referenced in the level manifest",
        opts.strategy.get_name(),
    );
        return Ok(());
    };

    // IMPORTANT: Write the manifest with the removed segments first
    // Otherwise the segment files are deleted, but are still referenced!
    levels.atomic_swap(
        |current| current.with_dropped(ids_to_drop),
        opts.eviction_seqno, // TODO: make naming in code base eviction_seqno vs watermark vs threshold consistent
    )?;

    // NOTE: If the application were to crash >here< it's fine
    // The segments are not referenced anymore, and will be
    // cleaned up upon recovery
    for segment in segments {
        segment.mark_as_deleted();
    }

    if let Err(e) = levels.maintenance(opts.eviction_seqno) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    drop(levels);

    log::trace!("Dropped {} segments", ids_to_drop.len());

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::AbstractTree;
    use std::sync::Arc;
    use test_log::test;

    #[test]
    #[ignore]
    fn compaction_drop_segments() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(folder).open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("a", "a", 2);
        tree.flush_active_memtable(0)?;

        assert_eq!(3, tree.approximate_len());

        tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)), 3)?;

        assert_eq!(0, tree.segment_count());

        Ok(())
    }
}
