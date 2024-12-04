// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{CompactionStrategy, Input as CompactionPayload};
use crate::{
    compaction::{stream::CompactionStream, Choice},
    file::SEGMENTS_FOLDER,
    level_manifest::LevelManifest,
    segment::{
        block_index::two_level_index::TwoLevelBlockIndex,
        id::GlobalSegmentId,
        level_reader::LevelReader,
        multi_writer::MultiWriter,
        scanner::{CompactionMerger, CompactionReader},
        Segment, SegmentInner,
    },
    stop_signal::StopSignal,
    tree::inner::{SealedMemtables, TreeId},
    Config, SegmentId, SeqNo,
};
use std::{
    path::Path,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockWriteGuard},
    time::Instant,
};

/// Compaction options
pub struct Options {
    pub tree_id: TreeId,

    pub segment_id_generator: Arc<AtomicU64>,

    /// Configuration of tree.
    pub config: Config,

    /// Levels manifest.
    pub levels: Arc<RwLock<LevelManifest>>,

    /// Sealed memtables (required for temporarily locking).
    pub sealed_memtables: Arc<RwLock<SealedMemtables>>,

    /// Compaction strategy.
    ///
    /// The one inside `config` is NOT used.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal
    pub stop_signal: StopSignal,

    /// Evicts items that are older than this seqno
    pub eviction_seqno: u64,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            tree_id: tree.id,
            segment_id_generator: tree.segment_id_counter.clone(),
            config: tree.config.clone(),
            sealed_memtables: tree.sealed_memtables.clone(),
            levels: tree.levels.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
            eviction_seqno: 0,
        }
    }
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<()> {
    log::trace!("compactor: acquiring levels manifest lock");
    let mut original_levels = opts.levels.write().expect("lock is poisoned");

    log::trace!("compactor: consulting compaction strategy");
    let choice = opts.strategy.choose(&original_levels, &opts.config);

    log::debug!("compactor: choice: {choice:?}");

    match choice {
        Choice::Merge(payload) => merge_segments(original_levels, opts, &payload),
        Choice::Move(payload) => {
            let segment_map = original_levels.get_all_segments();

            original_levels.atomic_swap(|recipe| {
                for segment_id in payload.segment_ids {
                    if let Some(segment) = segment_map.get(&segment_id).cloned() {
                        for level in recipe.iter_mut() {
                            level.remove(segment_id);
                        }

                        recipe
                            .get_mut(payload.dest_level as usize)
                            .expect("destination level should exist")
                            .insert(segment);
                    }
                }
            })
        }
        Choice::Drop(payload) => {
            drop_segments(
                original_levels,
                opts,
                &payload
                    .into_iter()
                    .map(|x| (opts.tree_id, x).into())
                    .collect::<Vec<_>>(),
            )?;
            Ok(())
        }
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
            Ok(())
        }
    }
}

fn create_compaction_stream<'a, P: AsRef<Path>>(
    segment_base_folder: P,
    levels: &LevelManifest,
    to_compact: &[SegmentId],
    eviction_seqno: SeqNo,
) -> crate::Result<Option<CompactionStream<CompactionMerger<'a>>>> {
    use std::ops::Bound::Unbounded;

    let mut readers: Vec<CompactionReader<'_>> = vec![];
    let mut found = 0;

    for level in &levels.levels {
        if level.is_empty() {
            continue;
        }

        if level.is_disjoint && level.len() > 1 {
            let Some(lo) = level
                .segments
                .iter()
                .enumerate()
                .filter(|(_, segment)| to_compact.contains(&segment.metadata.id))
                .min_by(|(a, _), (b, _)| a.cmp(b))
                .map(|(idx, _)| idx)
            else {
                continue;
            };

            let Some(hi) = level
                .segments
                .iter()
                .enumerate()
                .filter(|(_, segment)| to_compact.contains(&segment.metadata.id))
                .max_by(|(a, _), (b, _)| a.cmp(b))
                .map(|(idx, _)| idx)
            else {
                continue;
            };

            readers.push(Box::new(LevelReader::from_indexes(
                level.clone(),
                &(Unbounded, Unbounded),
                (Some(lo), Some(hi)),
                crate::segment::value_block::CachePolicy::Read,
            )));

            found += hi - lo + 1;
        } else {
            for &id in to_compact {
                if let Some(segment) = level.segments.iter().find(|x| x.metadata.id == id) {
                    found += 1;
                    readers.push(Box::new(segment.scan(&segment_base_folder)?));
                }
            }
        }
    }

    Ok(if found == to_compact.len() {
        Some(CompactionStream::new(
            CompactionMerger::new(readers),
            eviction_seqno,
        ))
    } else {
        None
    })
}

#[allow(clippy::too_many_lines)]
fn merge_segments(
    mut levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    if opts.stop_signal.is_stopped() {
        log::debug!("compactor: stopping before compaction because of stop signal");
    }

    let segments_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    let Some(merge_iter) = create_compaction_stream(
        &segments_base_folder,
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

    drop(levels);

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let is_last_level = payload.dest_level == last_level;

    let start = Instant::now();

    let mut segment_writer = MultiWriter::new(
        opts.segment_id_generator.clone(),
        payload.target_size,
        crate::segment::writer::Options {
            folder: segments_base_folder.clone(),
            segment_id: 0, // TODO: this is never used in MultiWriter
            data_block_size: opts.config.data_block_size,
            index_block_size: opts.config.index_block_size,
        },
    )?
    .use_compression(opts.config.compression);

    #[cfg(feature = "bloom")]
    {
        use crate::segment::writer::BloomConstructionPolicy;

        if opts.config.bloom_bits_per_key >= 0 {
            // NOTE: Apply some MONKEY to have very high FPR on small levels
            // because it's cheap
            let bloom_policy = match payload.dest_level {
                // TODO: increase to 0.00001 when https://github.com/fjall-rs/lsm-tree/issues/63 is fixed
                0 => BloomConstructionPolicy::FpRate(0.0001),
                1 => BloomConstructionPolicy::FpRate(0.001),
                _ => BloomConstructionPolicy::BitsPerKey(
                    opts.config.bloom_bits_per_key.unsigned_abs(),
                ),
            };

            segment_writer = segment_writer.use_bloom_policy(bloom_policy);
        } else {
            segment_writer =
                segment_writer.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0));
        }
    }

    for (idx, item) in merge_iter.enumerate() {
        let item = item?;

        // IMPORTANT: We can only drop tombstones when writing into last level
        if is_last_level && item.is_tombstone() {
            continue;
        }

        segment_writer.write(item)?;

        if idx % 100_000 == 0 && opts.stop_signal.is_stopped() {
            log::debug!("compactor: stopping amidst compaction because of stop signal");
            return Ok(());
        }
    }

    let writer_results = segment_writer.finish()?;

    log::debug!(
        "Compacted in {}ms ({} segments created)",
        start.elapsed().as_millis(),
        writer_results.len()
    );

    let created_segments = writer_results
        .into_iter()
        .map(|trailer| -> crate::Result<Segment> {
            let segment_id = trailer.metadata.id;
            let segment_file_path = segments_base_folder.join(segment_id.to_string());

            let tli_ptr = trailer.offsets.tli_ptr;

            #[cfg(feature = "bloom")]
            let bloom_ptr = trailer.offsets.bloom_ptr;

            // NOTE: Need to allow because of false positive in Clippy
            // because of "bloom" feature
            #[allow(clippy::needless_borrows_for_generic_args)]
            let block_index = Arc::new(TwoLevelBlockIndex::from_file(
                &segment_file_path,
                tli_ptr,
                (opts.tree_id, segment_id).into(),
                opts.config.descriptor_table.clone(),
                opts.config.block_cache.clone(),
            )?);

            Ok(SegmentInner {
                tree_id: opts.tree_id,

                descriptor_table: opts.config.descriptor_table.clone(),
                block_cache: opts.config.block_cache.clone(),

                metadata: trailer.metadata,
                offsets: trailer.offsets,

                #[allow(clippy::needless_borrows_for_generic_args)]
                block_index,

                #[cfg(feature = "bloom")]
                bloom_filter: Segment::load_bloom(&segment_file_path, bloom_ptr)?,
            }
            .into())
        })
        .collect::<crate::Result<Vec<_>>>()?;

    // NOTE: Mind lock order L -> M -> S
    log::trace!("compactor: acquiring levels manifest write lock");
    let mut original_levels = opts.levels.write().expect("lock is poisoned");

    // IMPORTANT: Write lock memtable(s), otherwise segments may get deleted while a range read is happening
    // NOTE: Mind lock order L -> M -> S
    log::trace!("compactor: acquiring sealed memtables write lock");
    let sealed_memtables_guard = opts.sealed_memtables.write().expect("lock is poisoned");

    let swap_result = original_levels.atomic_swap(|recipe| {
        for segment in created_segments.iter().cloned() {
            log::trace!("Persisting segment {}", segment.metadata.id);

            recipe
                .get_mut(payload.dest_level as usize)
                .expect("destination level should exist")
                .insert(segment);
        }

        for segment_id in &payload.segment_ids {
            log::trace!("Removing segment {segment_id}");

            for level in recipe.iter_mut() {
                level.remove(*segment_id);
            }
        }
    });

    if let Err(e) = swap_result {
        // IMPORTANT: Show the segments again, because compaction failed
        original_levels.show_segments(payload.segment_ids.iter().copied());
        return Err(e);
    };

    for segment in &created_segments {
        let segment_file_path = segments_base_folder.join(segment.metadata.id.to_string());

        opts.config.descriptor_table.insert(
            &segment_file_path,
            (opts.tree_id, segment.metadata.id).into(),
        );
    }

    // NOTE: Segments are registered, we can unlock the memtable(s) safely
    drop(sealed_memtables_guard);

    // NOTE: If the application were to crash >here< it's fine
    // The segments are not referenced anymore, and will be
    // cleaned up upon recovery
    for segment_id in &payload.segment_ids {
        let segment_file_path = segments_base_folder.join(segment_id.to_string());
        log::trace!("Removing old segment at {segment_file_path:?}");

        if let Err(e) = std::fs::remove_file(segment_file_path) {
            log::error!("Failed to cleanup file of deleted segment: {e:?}");
        }
    }

    for segment_id in &payload.segment_ids {
        log::trace!("Closing file handles for old segment file");

        opts.config
            .descriptor_table
            .remove((opts.tree_id, *segment_id).into());
    }

    original_levels.show_segments(payload.segment_ids.iter().copied());

    drop(original_levels);

    log::debug!("compactor: done");

    Ok(())
}

fn drop_segments(
    mut original_levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    segment_ids: &[GlobalSegmentId],
) -> crate::Result<()> {
    let segments_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    // IMPORTANT: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::trace!("compaction: acquiring sealed memtables write lock");
    let memtable_lock = opts.sealed_memtables.write().expect("lock is poisoned");

    // IMPORTANT: Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    original_levels.atomic_swap(|recipe| {
        for key in segment_ids {
            let segment_id = key.segment_id();
            log::trace!("Removing segment {segment_id}");

            for level in recipe.iter_mut() {
                level.remove(segment_id);
            }
        }
    })?;

    drop(memtable_lock);
    drop(original_levels);

    // NOTE: If the application were to crash >here< it's fine
    // The segments are not referenced anymore, and will be
    // cleaned up upon recovery
    for key in segment_ids {
        let segment_id = key.segment_id();

        let segment_file_path = segments_base_folder.join(segment_id.to_string());
        log::trace!("Removing old segment at {segment_file_path:?}");

        if let Err(e) = std::fs::remove_file(segment_file_path) {
            log::error!("Failed to cleanup file of deleted segment: {e:?}");
        }
    }

    for key in segment_ids {
        log::trace!("Closing file handles for segment data file");
        opts.config.descriptor_table.remove(*key);
    }

    log::trace!("Dropped {} segments", segment_ids.len());

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::AbstractTree;
    use std::sync::Arc;
    use test_log::test;

    #[test]
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
