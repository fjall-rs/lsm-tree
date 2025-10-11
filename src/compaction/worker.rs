// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{CompactionStrategy, Input as CompactionPayload};
use crate::{
    blob_tree::FragmentationMap,
    compaction::{
        flavour::{RelocatingCompaction, StandardCompaction},
        state::CompactionState,
        stream::CompactionStream,
        Choice,
    },
    file::BLOBS_FOLDER,
    merge::Merger,
    run_scanner::RunScanner,
    stop_signal::StopSignal,
    tree::inner::{SuperVersion, TreeId},
    version::Version,
    vlog::{BlobFileMergeScanner, BlobFileScanner, BlobFileWriter},
    BlobFile, Config, HashSet, InternalValue, SegmentId, SeqNo, SequenceNumberCounter,
};
use std::{
    sync::{atomic::AtomicU64, Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard},
    time::Instant,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

pub type CompactionReader<'a> = Box<dyn Iterator<Item = crate::Result<InternalValue>> + 'a>;

/// Compaction options
pub struct Options {
    pub tree_id: TreeId,

    pub segment_id_generator: Arc<AtomicU64>, // TODO: change segment_id_generator to be SequenceNumberCounter

    pub blob_file_id_generator: SequenceNumberCounter,

    /// Configuration of tree.
    pub config: Config,

    pub super_version: Arc<RwLock<SuperVersion>>,

    /// Compaction strategy to use.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal to interrupt a compaction worker in case
    /// the tree is dropped.
    pub stop_signal: StopSignal,

    /// Evicts items that are older than this seqno (MVCC GC).
    pub eviction_seqno: u64,

    pub compaction_state: Arc<Mutex<CompactionState>>,

    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            tree_id: tree.id,
            segment_id_generator: tree.segment_id_counter.clone(),
            blob_file_id_generator: tree.blob_file_id_generator.clone(),
            config: tree.config.clone(),
            super_version: tree.super_version.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
            eviction_seqno: 0,

            compaction_state: tree.compaction_state.clone(),

            #[cfg(feature = "metrics")]
            metrics: tree.metrics.clone(),
        }
    }
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<()> {
    let compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

    let super_version = opts.super_version.read().expect("lock is poisoned");

    let start = Instant::now();
    log::trace!(
        "Consulting compaction strategy {:?}",
        opts.strategy.get_name(),
    );
    let choice = opts
        .strategy
        .choose(&super_version.version, &opts.config, &compaction_state);

    log::debug!("Compaction choice: {choice:?} in {:?}", start.elapsed());

    match choice {
        Choice::Merge(payload) => merge_segments(compaction_state, super_version, opts, &payload),
        Choice::Move(payload) => move_segments(compaction_state, super_version, opts, &payload),
        Choice::Drop(payload) => drop_segments(
            compaction_state,
            super_version,
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
    version: &Version,
    to_compact: &[SegmentId],
    eviction_seqno: SeqNo,
) -> crate::Result<Option<CompactionStream<'a, Merger<CompactionReader<'a>>>>> {
    let mut readers: Vec<CompactionReader<'_>> = vec![];
    let mut found = 0;

    for level in version.iter_levels() {
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
    mut compaction_state: MutexGuard<'_, CompactionState>,
    super_version: RwLockReadGuard<'_, SuperVersion>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    drop(super_version);

    let mut super_version = opts.super_version.write().expect("lock is poisoned");

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(payload.segment_ids.iter().copied())
    {
        log::warn!(
        "Compaction task created by {:?} contained hidden segments, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
        opts.strategy.get_name(),
    );
        return Ok(());
    }

    let segment_ids = payload.segment_ids.iter().copied().collect::<Vec<_>>();

    compaction_state.upgrade_version(
        &mut super_version,
        |current| Ok(current.with_moved(&segment_ids, payload.dest_level as usize)),
        opts.eviction_seqno,
    )?;

    if let Err(e) = compaction_state.maintenance(opts.eviction_seqno) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    Ok(())
}

fn hidden_guard(
    payload: &CompactionPayload,
    opts: &Options,
    f: impl FnOnce() -> crate::Result<()>,
) -> crate::Result<()> {
    f().inspect_err(|e| {
        log::error!("Compaction failed: {e:?}");

        // IMPORTANT: We need to show tables again on error
        let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

        compaction_state
            .hidden_set_mut()
            .show(payload.segment_ids.iter().copied());
    })
}

#[allow(clippy::too_many_lines)]
fn merge_segments(
    mut compaction_state: MutexGuard<'_, CompactionState>,
    super_version: RwLockReadGuard<'_, SuperVersion>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    if opts.stop_signal.is_stopped() {
        log::debug!("Stopping before compaction because of stop signal");
        return Ok(());
    }

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(payload.segment_ids.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden segments, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(());
    }

    let Some(segments) = payload
        .segment_ids
        .iter()
        .map(|&id| super_version.version.get_segment(id).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
            "Compaction task created by {:?} contained segments not referenced in the level manifest",
            opts.strategy.get_name(),
        );
        return Ok(());
    };

    let mut blob_frag_map = FragmentationMap::default();

    let Some(mut merge_iter) = create_compaction_stream(
        &super_version.version,
        &payload.segment_ids.iter().copied().collect::<Vec<_>>(),
        opts.eviction_seqno,
    )?
    else {
        log::warn!(
            "Compaction task tried to compact segments that do not exist, declining to run it"
        );
        return Ok(());
    };

    let dst_lvl = payload.canonical_level.into();
    let last_level = opts.config.level_count - 1;

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let is_last_level = payload.dest_level == last_level;

    let current_version = &super_version.version;

    let table_writer = super::flavour::prepare_table_writer(current_version, opts, payload)?;

    let start = Instant::now();

    let mut compactor = match &opts.config.kv_separation_opts {
        Some(blob_opts) => {
            merge_iter = merge_iter.with_expiration_callback(&mut blob_frag_map);

            let blob_files_to_rewrite = {
                // TODO: 3.0.0 vvv if blob gc is disabled, skip this part vvv

                // TODO: 3.0.0 unit test and optimize... somehow
                let mut linked_blob_files = payload
                    .segment_ids
                    .iter()
                    .map(|&id| current_version.get_segment(id).expect("table should exist"))
                    .filter_map(|x| x.get_linked_blob_files().expect("handle error"))
                    .flatten()
                    .map(|blob_file_ref| {
                        current_version
                            .value_log
                            .get(&blob_file_ref.blob_file_id)
                            .expect("blob file should exist")
                    })
                    .filter(|blob_file| {
                        blob_file
                            .is_stale(current_version.gc_stats(), blob_opts.staleness_threshold)
                    })
                    .filter(|blob_file| {
                        // NOTE: Dead blob files are dropped anyway during current_version change commit
                        !blob_file.is_dead(current_version.gc_stats())
                    })
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();

                linked_blob_files.sort_by_key(|a| a.id());

                let cutoff_point = {
                    let len = linked_blob_files.len() as f32;
                    (len * blob_opts.age_cutoff) as usize
                };
                linked_blob_files.drain(cutoff_point..);

                // NOTE: If there is any table not part of our compaction input
                // that also points to the blob file, we cannot rewrite the blob file
                for table in current_version.iter_segments() {
                    if payload.segment_ids.contains(&table.id()) {
                        continue;
                    }

                    let other_ref = table
                        .get_linked_blob_files()
                        .expect("should not fail")
                        .unwrap_or_default();

                    let other_ref = other_ref
                        .iter()
                        .find(|x| linked_blob_files.iter().any(|bf| bf.id() == x.blob_file_id));

                    if let Some(other_ref) = other_ref {
                        linked_blob_files.retain(|x| x.id() != other_ref.blob_file_id);
                    }
                }

                linked_blob_files.into_iter().cloned().collect::<Vec<_>>()
            };

            if blob_files_to_rewrite.is_empty() {
                Box::new(StandardCompaction::new(table_writer, segments))
                    as Box<dyn super::flavour::CompactionFlavour>
            } else {
                log::debug!(
                    "Relocate blob files: {:#?}",
                    blob_files_to_rewrite
                        .iter()
                        .map(BlobFile::id)
                        .collect::<Vec<_>>(),
                );

                let scanner = BlobFileMergeScanner::new(
                    blob_files_to_rewrite
                        .iter()
                        .map(|bf| {
                            Ok(BlobFileScanner::new(&bf.0.path, bf.id())?
                                .use_compression(bf.0.meta.compression))
                        })
                        .collect::<crate::Result<Vec<_>>>()?,
                );

                let writer = BlobFileWriter::new(
                    opts.blob_file_id_generator.clone(),
                    blob_opts.file_target_size,
                    opts.config.path.join(BLOBS_FOLDER),
                )?
                .use_compression(blob_opts.compression);

                let inner = StandardCompaction::new(table_writer, segments);

                Box::new(RelocatingCompaction::new(
                    inner,
                    scanner.peekable(),
                    writer,
                    blob_files_to_rewrite.iter().map(BlobFile::id).collect(),
                    blob_files_to_rewrite,
                ))
            }
        }
        None => Box::new(StandardCompaction::new(table_writer, segments)),
    };

    log::trace!("Blob file GC preparation done in {:?}", start.elapsed());

    drop(super_version);

    {
        compaction_state
            .hidden_set_mut()
            .hide(payload.segment_ids.iter().copied());
    }

    // IMPORTANT: Unlock exclusive compaction lock as we are now doing the actual (CPU-intensive) compaction
    drop(compaction_state);

    hidden_guard(payload, opts, || {
        for (idx, item) in merge_iter.enumerate() {
            let item = item?;

            // IMPORTANT: We can only drop tombstones when writing into last level
            if is_last_level && item.is_tombstone() {
                continue;
            }

            compactor.write(item)?;

            if idx % 1_000_000 == 0 && opts.stop_signal.is_stopped() {
                log::debug!("Stopping amidst compaction because of stop signal");
                return Ok(());
            }
        }

        Ok(())
    })?;

    let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

    log::trace!("Acquiring super version write lock");
    let mut super_version = opts.super_version.write().expect("lock is poisoned");
    log::trace!("Acquired super version write lock");

    compactor
        .finish(
            &mut super_version,
            &mut compaction_state,
            opts,
            payload,
            dst_lvl,
            blob_frag_map,
        )
        .inspect_err(|e| {
            // NOTE: We cannot use hidden_guard here because we already locked the compaction state

            log::error!("Compaction failed: {e:?}");

            compaction_state
                .hidden_set_mut()
                .show(payload.segment_ids.iter().copied());
        })?;

    compaction_state
        .hidden_set_mut()
        .show(payload.segment_ids.iter().copied());

    compaction_state
        .maintenance(opts.eviction_seqno)
        .inspect_err(|e| {
            log::error!("Manifest maintenance failed: {e:?}");
        })?;

    drop(super_version);
    drop(compaction_state);

    log::trace!("Compaction successful");

    Ok(())
}

fn drop_segments(
    mut compaction_state: MutexGuard<'_, CompactionState>,
    super_version: RwLockReadGuard<'_, SuperVersion>,
    opts: &Options,
    ids_to_drop: &[SegmentId],
) -> crate::Result<()> {
    drop(super_version);

    let mut super_version = opts.super_version.write().expect("lock is poisoned");

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(ids_to_drop.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden segments, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(());
    }

    let Some(segments) = ids_to_drop
        .iter()
        .map(|&id| super_version.version.get_segment(id).cloned())
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
    compaction_state.upgrade_version(
        &mut super_version,
        |current| current.with_dropped(ids_to_drop),
        opts.eviction_seqno, // TODO: make naming in code base eviction_seqno vs watermark vs threshold consistent
    )?;

    // NOTE: If the application were to crash >here< it's fine
    // The segments are not referenced anymore, and will be
    // cleaned up upon recovery
    for segment in segments {
        segment.mark_as_deleted();
    }

    // TODO: fwiw also add all dead blob files
    // TODO: look if any blob files can be trivially deleted as well

    if let Err(e) = compaction_state.maintenance(opts.eviction_seqno) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    drop(super_version);
    drop(compaction_state);

    log::trace!("Dropped {} segments", ids_to_drop.len());

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
        tree.insert("b", "a", 1);
        tree.flush_active_memtable(0)?;
        tree.insert("c", "a", 2);
        tree.flush_active_memtable(0)?;

        assert_eq!(3, tree.approximate_len());

        tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)), 3)?;

        assert_eq!(0, tree.segment_count());

        Ok(())
    }
}
