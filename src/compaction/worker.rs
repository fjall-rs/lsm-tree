// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{CompactionAction, CompactionResult, CompactionStrategy, Input as CompactionPayload};
use crate::{
    blob_tree::FragmentationMap,
    compaction::{
        filter::{Context, StreamFilterAdapter},
        flavour::{RelocatingCompaction, StandardCompaction},
        state::CompactionState,
        stream::CompactionStream,
        Choice,
    },
    file::BLOBS_FOLDER,
    merge::Merger,
    run_scanner::RunScanner,
    stop_signal::StopSignal,
    tree::inner::TreeId,
    version::{Run, SuperVersions, Version},
    vlog::{BlobFileMergeScanner, BlobFileScanner, BlobFileWriter},
    BlobFile, Config, HashSet, InternalValue, SeqNo, SequenceNumberCounter,
    SharedSequenceNumberGenerator, Table, TableId,
};
use std::{
    sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard},
    time::Instant,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

pub type CompactionReader<'a> = Box<dyn Iterator<Item = crate::Result<InternalValue>> + 'a>;

/// Compaction options
pub struct Options {
    pub tree_id: TreeId,

    pub global_seqno: SharedSequenceNumberGenerator,

    pub visible_seqno: SharedSequenceNumberGenerator,

    pub table_id_generator: SequenceNumberCounter,

    pub blob_file_id_generator: SequenceNumberCounter,

    /// Configuration of tree.
    pub config: Arc<Config>,

    pub version_history: Arc<RwLock<SuperVersions>>,

    /// Compaction strategy to use.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal to interrupt a compaction worker in case
    /// the tree is dropped.
    pub stop_signal: StopSignal,

    /// Evicts items that are older than this seqno (MVCC GC).
    pub mvcc_gc_watermark: u64,

    pub compaction_state: Arc<Mutex<CompactionState>>,

    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            global_seqno: tree.config.seqno.clone(),
            visible_seqno: tree.config.visible_seqno.clone(),
            tree_id: tree.id,
            table_id_generator: tree.table_id_counter.clone(),
            blob_file_id_generator: tree.blob_file_id_counter.clone(),
            config: tree.config.clone(),
            version_history: tree.version_history.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
            mvcc_gc_watermark: 0,

            compaction_state: tree.compaction_state.clone(),

            #[cfg(feature = "metrics")]
            metrics: tree.metrics.clone(),
        }
    }
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<CompactionResult> {
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let version_history_lock = opts.version_history.read().expect("lock is poisoned");

    let start = Instant::now();
    log::trace!(
        "Consulting compaction strategy {:?}",
        opts.strategy.get_name(),
    );
    let choice = opts.strategy.choose(
        &version_history_lock.latest_version().version,
        &opts.config,
        &compaction_state,
    );

    log::debug!("Compaction choice: {choice:?} in {:?}", start.elapsed());

    match choice {
        Choice::Merge(payload) => {
            merge_tables(compaction_state, version_history_lock, opts, &payload)
        }
        Choice::Move(payload) => {
            drop(version_history_lock);

            move_tables(&compaction_state, opts, &payload)
        }
        Choice::Drop(payload) => {
            drop(version_history_lock);

            let ids = payload.into_iter().collect::<Vec<_>>();
            drop_tables(compaction_state, opts, &ids)
        }
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
            Ok(CompactionResult::nothing())
        }
    }
}

fn pick_run_indexes(run: &Run<Table>, to_compact: &[TableId]) -> Option<(usize, usize)> {
    let lo = run
        .iter()
        .position(|table| to_compact.contains(&table.id()))?;

    let hi = run
        .iter()
        .rposition(|table| to_compact.contains(&table.id()))?;

    Some((lo, hi))
}

fn create_compaction_stream<'a>(
    version: &Version,
    to_compact: &[TableId],
    eviction_seqno: SeqNo,
    merge_operator: Option<Arc<dyn crate::merge_operator::MergeOperator>>,
    comparator: crate::comparator::SharedComparator,
) -> crate::Result<Option<CompactionStream<'a, Merger<CompactionReader<'a>>>>> {
    let mut readers: Vec<CompactionReader<'_>> = vec![];
    let mut found = 0;

    for run in version.iter_levels().flat_map(|lvl| lvl.iter()) {
        if run.len() > 1 {
            let Some((lo, hi)) = pick_run_indexes(run, to_compact) else {
                continue;
            };

            readers.push(Box::new(RunScanner::culled(
                run.clone(),
                (Some(lo), Some(hi)),
            )?));

            found += hi - lo + 1;
        } else {
            for table in run.iter().filter(|x| to_compact.contains(&x.metadata.id)) {
                found += 1;
                readers.push(Box::new(table.scan()?));
            }
        }
    }

    Ok(if found == to_compact.len() {
        Some(
            CompactionStream::new(Merger::new(readers, comparator), eviction_seqno)
                .with_merge_operator(merge_operator),
        )
    } else {
        None
    })
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "version_history_lock must be held across upgrade_version and maintenance"
)]
fn move_tables(
    compaction_state: &MutexGuard<'_, CompactionState>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<CompactionResult> {
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(payload.table_ids.iter().copied())
    {
        log::warn!(
        "Compaction task created by {:?} contained hidden tables, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
        opts.strategy.get_name(),
    );
        return Ok(CompactionResult::nothing());
    }

    let table_count = payload.table_ids.len();
    let table_ids = payload.table_ids.iter().copied().collect::<Vec<_>>();

    version_history_lock.upgrade_version(
        &opts.config.path,
        |current| {
            let mut copy = current.clone();

            copy.version = copy
                .version
                .with_moved(&table_ids, payload.dest_level as usize);

            Ok(copy)
        },
        &opts.global_seqno,
        &opts.visible_seqno,
    )?;

    if let Err(e) = version_history_lock.maintenance(&opts.config.path, opts.mvcc_gc_watermark) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    Ok(CompactionResult {
        action: CompactionAction::Moved,
        dest_level: Some(payload.dest_level),
        tables_in: table_count,
        tables_out: table_count,
    })
}

/// Picks blob files to rewrite (defragment)
fn pick_blob_files_to_rewrite(
    picked_tables: &HashSet<TableId>,
    current_version: &Version,
    blob_opts: &crate::KvSeparationOptions,
) -> crate::Result<Vec<BlobFile>> {
    use crate::Table;

    // We start off by getting all the blob files that are referenced by the tables
    // that we want to compact.
    let linked_blob_files = picked_tables
        .iter()
        .map(|&id| {
            current_version.get_table(id).unwrap_or_else(|| {
                panic!("Table {id} should exist");
            })
        })
        .map(Table::list_blob_file_references)
        .collect::<Result<Vec<_>, _>>()?;

    // Then we filter all blob files that are not fragmented or old enough.
    let mut linked_blob_files = linked_blob_files
        .into_iter()
        .flatten()
        .flatten()
        .map(|blob_file_ref| {
            current_version
                .blob_files
                .get(blob_file_ref.blob_file_id)
                .unwrap_or_else(|| {
                    panic!("Blob file {} should exist", blob_file_ref.blob_file_id);
                })
        })
        .filter(|blob_file| {
            blob_file.is_stale(current_version.gc_stats(), blob_opts.staleness_threshold)
        })
        .filter(|blob_file| {
            // NOTE: Dead blob files are dropped anyway during current_version change commit
            !blob_file.is_dead(current_version.gc_stats())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    linked_blob_files.sort_by_key(|a| a.id());

    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "precision loss and truncation are acceptable for cutoff calculation"
    )]
    let cutoff_point = {
        let len = linked_blob_files.len() as f32;
        (len * blob_opts.age_cutoff) as usize
    };
    linked_blob_files.drain(cutoff_point..);

    // IMPORTANT: Additionally, we also have to check if any other tables reference any of our candidate blob files.
    // We have to *not* include blob files that are referenced by other tables, because otherwise those
    // blob references would point into nothing (becoming dangling).
    for table in current_version.iter_tables() {
        if picked_tables.contains(&table.id()) {
            continue;
        }

        let other_refs = table
            .list_blob_file_references()?
            .unwrap_or_default()
            .into_iter()
            .filter(|x| linked_blob_files.iter().any(|bf| bf.id() == x.blob_file_id))
            .collect::<Vec<_>>();

        for additional_ref in other_refs {
            linked_blob_files.retain(|x| x.id() != additional_ref.blob_file_id);
        }
    }

    Ok(linked_blob_files.into_iter().cloned().collect::<Vec<_>>())
}

fn hidden_guard<T>(
    payload: &CompactionPayload,
    opts: &Options,
    f: impl FnOnce() -> crate::Result<T>,
) -> crate::Result<T> {
    f().inspect_err(|e| {
        log::error!("Compaction failed: {e:?}");

        // IMPORTANT: We need to show tables again on error
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

        compaction_state
            .hidden_set_mut()
            .show(payload.table_ids.iter().copied());
    })
}

#[expect(clippy::too_many_lines)]
fn merge_tables(
    mut compaction_state: MutexGuard<'_, CompactionState>,
    version_history_lock: RwLockReadGuard<'_, SuperVersions>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<CompactionResult> {
    if opts.stop_signal.is_stopped() {
        log::debug!("Stopping before compaction because of stop signal");
        return Ok(CompactionResult::nothing());
    }

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(payload.table_ids.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden tables, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    }

    let current_super_version = version_history_lock.latest_version();

    let Some(tables) = payload
        .table_ids
        .iter()
        .map(|&id| current_super_version.version.get_table(id).cloned())
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
            "Compaction task created by {:?} contained tables not referenced in the level manifest",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    };

    let tables_in = payload.table_ids.len();

    // Collect range tombstones from input tables before they are moved.
    // Canonicalize to avoid duplicate RTs across input tables (MultiWriter
    // rotation copies the same RT into every output table during flush).
    let mut input_range_tombstones: Vec<crate::range_tombstone::RangeTombstone> = tables
        .iter()
        .flat_map(|t| t.range_tombstones().iter().cloned())
        .collect();
    input_range_tombstones.sort();
    input_range_tombstones.dedup();

    let mut blob_frag_map = FragmentationMap::default();

    let Some(mut merge_iter) = create_compaction_stream(
        &current_super_version.version,
        &payload.table_ids.iter().copied().collect::<Vec<_>>(),
        opts.mvcc_gc_watermark,
        opts.config.merge_operator.clone(),
        opts.config.comparator.clone(),
    )?
    else {
        log::warn!(
            "Compaction task tried to compact tables that do not exist, declining to run it"
        );
        return Ok(CompactionResult::nothing());
    };

    let dst_lvl = payload.canonical_level.into();
    let is_last_level = payload.dest_level == opts.config.level_count - 1;

    merge_iter = merge_iter
        .evict_tombstones(is_last_level)
        .zero_seqnos(false);

    let blobs_folder = opts.config.path.join(BLOBS_FOLDER);

    let filter_ctx = Context { is_last_level };

    // Construct the compaction filter
    let mut compaction_filter = opts.config.compaction_filter_factory.as_ref().map(|f| {
        log::trace!("Installing custom compaction filter {:?}", f.name());
        f.make_filter(&filter_ctx)
    });

    // This is used by the compaction filter if it wants to write new blobs
    // TODO: the filter should really pipe new blobs into the compaction stream directly,
    // TODO: but that will probably require to change the protocol between filter <-> compaction stream a bit
    let mut filter_blob_writer = None;
    let mut merge_iter = merge_iter.with_filter(StreamFilterAdapter::new(
        compaction_filter.as_deref_mut(),
        opts,
        &current_super_version.version,
        &blobs_folder,
        &mut filter_blob_writer,
        &filter_ctx,
    ));

    let table_writer =
        super::flavour::prepare_table_writer(&current_super_version.version, opts, payload)?;

    let start = Instant::now();

    let mut compactor = match &opts.config.kv_separation_opts {
        Some(blob_opts) => {
            merge_iter = merge_iter.with_drop_callback(&mut blob_frag_map);

            let blob_files_to_rewrite = pick_blob_files_to_rewrite(
                &payload.table_ids,
                &current_super_version.version,
                blob_opts,
            )?;

            if blob_files_to_rewrite.is_empty() {
                log::debug!("No blob relocation needed");

                Box::new(StandardCompaction::new(table_writer, tables))
                    as Box<dyn super::flavour::CompactionFlavour>
            } else {
                log::debug!(
                    "Relocate blob files: {:?}",
                    blob_files_to_rewrite
                        .iter()
                        .map(BlobFile::id)
                        .collect::<Vec<_>>(),
                );

                let scanner = BlobFileMergeScanner::new(
                    blob_files_to_rewrite
                        .iter()
                        .map(|bf| BlobFileScanner::new(&bf.0.path, bf.id()))
                        .collect::<crate::Result<Vec<_>>>()?,
                );

                let writer = BlobFileWriter::new(
                    opts.blob_file_id_generator.clone(),
                    &blobs_folder,
                    opts.tree_id,
                    opts.config.descriptor_table.clone(),
                )?
                .use_target_size(blob_opts.file_target_size)
                .use_passthrough_compression(blob_opts.compression);

                let inner = StandardCompaction::new(table_writer, tables);

                Box::new(RelocatingCompaction::new(
                    inner,
                    scanner.peekable(),
                    writer,
                    blob_files_to_rewrite,
                ))
            }
        }
        None => Box::new(StandardCompaction::new(table_writer, tables)),
    };

    log::trace!("Blob file GC preparation done in {:?}", start.elapsed());

    drop(version_history_lock);

    {
        compaction_state
            .hidden_set_mut()
            .hide(payload.table_ids.iter().copied());
    }

    // IMPORTANT: Unlock exclusive compaction lock as we are now doing the actual (CPU-intensive) compaction
    drop(compaction_state);

    hidden_guard(payload, opts, || {
        // Propagate range tombstones to output tables BEFORE writing KV items,
        // so that if the compactor rotates tables during the merge loop,
        // earlier tables already carry the RT metadata.
        //
        // Keep RTs even at the last level until compaction itself becomes
        // RT-aware and can physically drop covered KVs. Dropping the RT first
        // would only remove the logical delete marker and can resurrect data.
        if !input_range_tombstones.is_empty() {
            log::debug!(
                "Propagating {} range tombstones to compaction output",
                input_range_tombstones.len(),
            );
            compactor.write_range_tombstones(&input_range_tombstones);
        }

        for (idx, item) in merge_iter.enumerate() {
            let item = item?;

            compactor.write(item)?;

            // NOTE: When stop_signal fires mid-merge, the loop exits early but
            // compaction proceeds to commit whatever was written so far. The
            // resulting CompactionResult will report `Merged` even though not
            // all input items were processed. This is pre-existing behavior:
            // partial merge output is valid and committed to the version history.
            if idx % 1_000_000 == 0 && opts.stop_signal.is_stopped() {
                log::debug!("Stopping amidst compaction because of stop signal");
                return Ok(());
            }
        }

        Ok(())
    })?;

    if let Some(filter) = compaction_filter {
        filter.finish();
    }

    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut compaction_state = opts.compaction_state.lock().expect("lock is poisoned");

    log::trace!("Acquiring super version write lock");
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");
    log::trace!("Acquired super version write lock");

    log::trace!("Blob fragmentation diff: {blob_frag_map:#?}");

    let extra_blob_files = filter_blob_writer
        .map(BlobFileWriter::finish)
        .transpose()
        .inspect_err(|e| {
            // NOTE: We cannot use hidden_guard here because we already locked the compaction state

            log::error!("Compaction failed while finishing filter blob writer: {e:?}");

            compaction_state
                .hidden_set_mut()
                .show(payload.table_ids.iter().copied());
        })?
        .unwrap_or_default();

    let tables_out = compactor
        .finish(
            &mut version_history_lock,
            opts,
            payload,
            dst_lvl,
            blob_frag_map,
            extra_blob_files,
        )
        .inspect_err(|e| {
            // NOTE: We cannot use hidden_guard here because we already locked the compaction state

            log::error!("Compaction failed: {e:?}");

            compaction_state
                .hidden_set_mut()
                .show(payload.table_ids.iter().copied());
        })?;

    compaction_state
        .hidden_set_mut()
        .show(payload.table_ids.iter().copied());

    version_history_lock
        .maintenance(&opts.config.path, opts.mvcc_gc_watermark)
        .inspect_err(|e| {
            log::error!("Manifest maintenance failed: {e:?}");
        })?;

    drop(version_history_lock);
    drop(compaction_state);

    log::trace!("Compaction successful");

    Ok(CompactionResult {
        action: CompactionAction::Merged,
        dest_level: Some(payload.dest_level),
        tables_in,
        tables_out,
    })
}

fn drop_tables(
    compaction_state: MutexGuard<'_, CompactionState>,
    opts: &Options,
    ids_to_drop: &[TableId],
) -> crate::Result<CompactionResult> {
    #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
    let mut version_history_lock = opts.version_history.write().expect("lock is poisoned");

    // Fail-safe for buggy compaction strategies
    if compaction_state
        .hidden_set()
        .should_decline_compaction(ids_to_drop.iter().copied())
    {
        log::warn!(
            "Compaction task created by {:?} contained hidden tables, declining to run it - please report this at https://github.com/fjall-rs/lsm-tree/issues/new?template=bug_report.md",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    }

    let Some(tables) = ids_to_drop
        .iter()
        .map(|&id| {
            version_history_lock
                .latest_version()
                .version
                .get_table(id)
                .cloned()
        })
        .collect::<Option<Vec<_>>>()
    else {
        log::warn!(
            "Compaction task created by {:?} contained tables not referenced in the level manifest",
            opts.strategy.get_name(),
        );
        return Ok(CompactionResult::nothing());
    };

    log::debug!("Dropping tables: {ids_to_drop:?}");

    let mut dropped_blob_files = vec![];

    // IMPORTANT: Write the manifest with the removed tables first
    // Otherwise the table files are deleted, but are still referenced!
    version_history_lock.upgrade_version(
        &opts.config.path,
        |current| {
            let mut copy = current.clone();

            copy.version = copy
                .version
                .with_dropped(ids_to_drop, &mut dropped_blob_files)?;

            Ok(copy)
        },
        &opts.global_seqno,
        &opts.visible_seqno,
    )?;

    if let Err(e) = version_history_lock.maintenance(&opts.config.path, opts.mvcc_gc_watermark) {
        log::error!("Manifest maintenance failed: {e:?}");
        return Err(e);
    }

    drop(version_history_lock);

    // NOTE: If the application were to crash >here< it's fine
    // The tables are not referenced anymore, and will be
    // cleaned up upon recovery
    for table in tables {
        table.mark_as_deleted();
    }

    for blob_file in dropped_blob_files {
        blob_file.mark_as_deleted();
    }

    let tables_dropped = ids_to_drop.len();

    drop(compaction_state);

    log::trace!("Dropped {tables_dropped} tables");

    Ok(CompactionResult {
        action: CompactionAction::Dropped,
        dest_level: None,
        tables_in: tables_dropped,
        tables_out: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::{create_compaction_stream, pick_run_indexes};
    use crate::{
        compaction::{state::CompactionState, Choice, CompactionStrategy, Input},
        config::BlockSizePolicy,
        version::Version,
        AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter, TableId,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn compaction_stream_run_not_found() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert!(create_compaction_stream(
            &tree.current_version(),
            &[666],
            0,
            None,
            crate::comparator::default_comparator()
        )?
        .is_none());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            Some((0, 2)),
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[0, 1, 2],
            )
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run_2() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            Some((0, 0)),
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[0],
            )
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run_3() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            Some((2, 2)),
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[2],
            )
        );

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn compaction_stream_run_4() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;

        tree.insert("c", "c", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(
            None,
            pick_run_indexes(
                tree.current_version()
                    .level(0)
                    .unwrap()
                    .iter()
                    .next()
                    .unwrap(),
                &[4],
            )
        );

        Ok(())
    }

    #[test]
    fn compaction_drop_tables() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.approximate_len());
        assert_eq!(0, tree.sealed_memtable_count());

        tree.insert("b", "a", 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.approximate_len());
        assert_eq!(0, tree.sealed_memtable_count());

        tree.insert("c", "a", 2);
        tree.flush_active_memtable(0)?;
        assert_eq!(3, tree.approximate_len());
        assert_eq!(0, tree.sealed_memtable_count());

        tree.compact(Arc::new(crate::compaction::Fifo::new(1, None)), 3)?;

        assert_eq!(0, tree.table_count());

        Ok(())
    }

    #[test]
    fn blob_file_picking_simple() -> crate::Result<()> {
        struct InPlaceStrategy(Vec<TableId>);

        impl CompactionStrategy for InPlaceStrategy {
            fn get_name(&self) -> &'static str {
                "InPlaceCompaction"
            }

            fn choose(&self, _: &Version, _: &Config, _: &CompactionState) -> Choice {
                Choice::Merge(Input {
                    table_ids: self.0.iter().copied().collect(),
                    dest_level: 6,
                    target_size: 64_000_000,
                    canonical_level: 6, // We don't really care - this compaction is only used for very specific unit tests
                })
            }
        }

        let folder = tempfile::tempdir()?;

        let tree = crate::Config::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(1))
        .with_kv_separation(Some(
            KvSeparationOptions::default()
                .separation_threshold(1)
                .age_cutoff(1.0)
                .staleness_threshold(0.01)
                .compression(crate::CompressionType::None),
        ))
        .open()?;

        tree.insert("a", "a", 0);
        tree.insert("b", "b", 0);
        tree.insert("c", "c", 0);
        tree.flush_active_memtable(1_000)?;
        assert_eq!(0, tree.sealed_memtable_count());
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        tree.major_compact(1, 1_000)?;
        assert_eq!(3, tree.table_count());
        assert_eq!(1, tree.blob_file_count());
        // We now have tables [1, 2, 3] pointing into blob file 0

        tree.drop_range("a"..="a")?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            assert_eq!(
                &{
                    let mut map = crate::HashMap::default();
                    map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                    map
                },
                &**tree.current_version().gc_stats(),
            );
        }

        // Even though we are compacting table #2, blob file is not rewritten
        // because table #3 still points into it
        tree.compact(Arc::new(InPlaceStrategy(vec![2])), 1_000)?;
        assert_eq!(2, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        {
            assert_eq!(
                &{
                    let mut map = crate::HashMap::default();
                    map.insert(0, crate::blob_tree::FragmentationEntry::new(1, 1, 1));
                    map
                },
                &**tree.current_version().gc_stats(),
            );
        }

        // Because tables #3 & #4 both point into the blob file
        // Only selecting both for compaction will actually rewrite the file
        tree.compact(Arc::new(InPlaceStrategy(vec![3, 4])), 1_000)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.blob_file_count());

        // Fragmentation is cleared up because blob file was relocated
        {
            assert_eq!(
                crate::HashMap::default(),
                **tree.current_version().gc_stats(),
            );
        }

        Ok(())
    }
}
