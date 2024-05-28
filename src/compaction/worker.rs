use super::{CompactionStrategy, Input as CompactionPayload};
use crate::{
    compaction::Choice,
    file::SEGMENTS_FOLDER,
    levels::LevelManifest,
    merge::{BoxedIterator, MergeIterator},
    segment::{block_index::BlockIndex, id::GlobalSegmentId, multi_writer::MultiWriter, Segment},
    snapshot::Counter as SnapshotCounter,
    stop_signal::StopSignal,
    tree_inner::{SealedMemtables, TreeId},
    Config,
};
use std::{
    collections::HashSet,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockWriteGuard},
    time::Instant,
};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

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

    /// Snapshot counter (required for checking if there are open snapshots).
    pub open_snapshots: SnapshotCounter,

    /// Compaction strategy.
    ///
    /// The one inside `config` is NOT used.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal
    pub stop_signal: StopSignal,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            tree_id: tree.id,
            segment_id_generator: tree.segment_id_counter.clone(),
            config: tree.config.clone(),
            sealed_memtables: tree.sealed_memtables.clone(),
            levels: tree.levels.clone(),
            open_snapshots: tree.open_snapshots.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
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
                for segment_id in &payload.segment_ids {
                    if let Some(segment) = segment_map.get(segment_id).cloned() {
                        for level in recipe.iter_mut() {
                            level.remove(*segment_id);
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

    let merge_iter = {
        let to_merge: Vec<_> = {
            let segments = levels.get_all_segments();

            payload
                .segment_ids
                .iter()
                // NOTE: Throw away duplicate segment IDs
                .collect::<HashSet<_>>()
                .into_iter()
                .filter_map(|x| segments.get(x))
                .cloned()
                .collect()
        };

        // NOTE: When there are open snapshots
        // we don't want to GC old versions of items
        // otherwise snapshots will lose data
        //
        // Also, keep versions around for a bit (don't evict when compacting into L0 & L1)
        let no_snapshots_open = !opts.open_snapshots.has_open_snapshots();
        let is_deep_level = payload.dest_level >= 2;

        let mut segment_readers: Vec<BoxedIterator<'_>> = Vec::with_capacity(to_merge.len());

        for segment in to_merge {
            let iter = Box::new(
                segment
                    .iter()
                    .cache_policy(crate::segment::value_block::CachePolicy::Read),
            );
            segment_readers.push(iter);
        }

        MergeIterator::new(segment_readers).evict_old_versions(no_snapshots_open && is_deep_level)
    };

    let last_level = levels.last_level_index();

    levels.hide_segments(&payload.segment_ids);
    drop(levels);

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let is_last_level = payload.dest_level == last_level;
    let should_evict_tombstones = is_last_level;

    let start = Instant::now();

    // TODO: MONKEY
    #[cfg(feature = "bloom")]
    let bloom_fp_rate = match payload.dest_level {
        0 => 0.0001,
        1 => 0.001,
        2 => 0.01,
        _ => {
            if is_last_level {
                0.5
            } else {
                0.1
            }
        }
    };

    let mut segment_writer = MultiWriter::new(
        opts.segment_id_generator.clone(),
        payload.target_size,
        crate::segment::writer::Options {
            segment_id: 0, // TODO: this is never used in MultiWriter

            block_size: opts.config.inner.block_size,
            evict_tombstones: should_evict_tombstones,
            folder: segments_base_folder.clone(),

            #[cfg(feature = "bloom")]
            bloom_fp_rate,
        },
    )?;

    for (idx, item) in merge_iter.enumerate() {
        segment_writer.write(item?)?;

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
        .map(|trailer| -> crate::Result<Arc<Segment>> {
            let segment_id = trailer.metadata.id;
            let segment_file_path = segments_base_folder.join(segment_id.to_string());

            let tli_ptr = trailer.offsets.tli_ptr;

            #[cfg(feature = "bloom")]
            let bloom_ptr = trailer.offsets.bloom_ptr;

            Ok(Arc::new(Segment {
                tree_id: opts.tree_id,

                descriptor_table: opts.config.descriptor_table.clone(),
                block_cache: opts.config.block_cache.clone(),

                metadata: trailer.metadata,
                offsets: trailer.offsets,

                // TODO: if L0, L1, preload block index (non-partitioned)
                #[allow(clippy::needless_borrows_for_generic_args)]
                block_index: BlockIndex::from_file(
                    &segment_file_path,
                    tli_ptr,
                    (opts.tree_id, segment_id).into(),
                    opts.config.descriptor_table.clone(),
                    opts.config.block_cache.clone(),
                )?
                .into(),

                #[cfg(feature = "bloom")]
                bloom_filter: {
                    use crate::serde::Deserializable;
                    use std::{
                        fs::File,
                        io::{Seek, SeekFrom},
                    };

                    assert!(bloom_ptr > 0, "can not find bloom filter block");

                    let mut reader = File::open(&segment_file_path)?;
                    reader.seek(SeekFrom::Start(bloom_ptr))?;
                    BloomFilter::deserialize(&mut reader)?
                },
            }))
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

    // IMPORTANT: Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    if let Err(e) = swap_result {
        // IMPORTANT: Show the segments again, because compaction failed
        original_levels.show_segments(&payload.segment_ids);
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
        std::fs::remove_file(segment_file_path)?;
    }

    for segment_id in &payload.segment_ids {
        log::trace!("Closing file handles for old segment file");
        opts.config
            .descriptor_table
            .remove((opts.tree_id, *segment_id).into());
    }

    original_levels.show_segments(&payload.segment_ids);

    drop(original_levels);

    log::debug!("compactor: done");

    Ok(())
}

fn drop_segments(
    mut original_levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    segment_ids: &[GlobalSegmentId],
) -> crate::Result<()> {
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
        log::trace!("rm -rf segment folder {segment_id}");

        std::fs::remove_dir_all(
            opts.config
                .path
                .join(SEGMENTS_FOLDER)
                .join(segment_id.to_string()),
        )?;
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
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn compaction_drop_segments() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(folder).open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable()?;
        tree.insert("a", "a", 1);
        tree.flush_active_memtable()?;
        tree.insert("a", "a", 2);
        tree.flush_active_memtable()?;

        assert_eq!(3, tree.approximate_len());

        tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)))?;

        assert_eq!(0, tree.segment_count());

        Ok(())
    }
}
