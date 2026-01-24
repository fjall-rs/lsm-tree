// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::state::CompactionState,
    config::Config,
    fs::{FileSystem, StdFileSystem},
    stop_signal::StopSignal,
    version::{persist_version, SuperVersions, Version},
    SequenceNumberCounter, TableId,
};
use std::sync::{atomic::AtomicU64, Arc, Mutex, RwLock};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Unique tree ID
///
/// Tree IDs are monotonically increasing integers.
pub type TreeId = u64;

/// Unique memtable ID
///
/// Memtable IDs map one-to-one to some table.
pub type MemtableId = u64;

/// Hands out a unique (monotonically increasing) tree ID.
pub fn get_next_tree_id() -> TreeId {
    static TREE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    TREE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub struct TreeInner<F: FileSystem = StdFileSystem> {
    /// Unique tree ID
    pub id: TreeId,

    /// Hands out a unique (monotonically increasing) memtable ID
    #[doc(hidden)]
    pub memtable_id_counter: SequenceNumberCounter,

    /// Hands out a unique (monotonically increasing) table ID
    #[doc(hidden)]
    pub table_id_counter: SequenceNumberCounter,

    // This is not really used in the normal tree, but we need it in the blob tree
    /// Hands out a unique (monotonically increasing) blob file ID
    pub(crate) blob_file_id_counter: SequenceNumberCounter,

    pub(crate) version_history: Arc<RwLock<SuperVersions<F>>>,

    pub(crate) compaction_state: Arc<Mutex<CompactionState>>,

    /// Tree configuration
    pub config: Arc<Config<F>>,

    /// Compaction may take a while; setting the signal to `true`
    /// will interrupt the compaction and kill the worker.
    pub(crate) stop_signal: StopSignal,

    /// Used by major compaction to be the exclusive compaction going on.
    ///
    /// Minor compactions use `major_compaction_lock.read()` instead, so they
    /// can be concurrent next to each other.
    pub(crate) major_compaction_lock: RwLock<()>,

    /// Serializes flush operations.
    pub(crate) flush_lock: Mutex<()>,
    #[doc(hidden)]
    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl<F: FileSystem> TreeInner<F> {
    pub(crate) fn create_new(config: Config<F>) -> crate::Result<Self> {
        let version = Version::<F>::new(
            0,
            if config.kv_separation_opts.is_some() {
                crate::TreeType::Blob
            } else {
                crate::TreeType::Standard
            },
        );
        persist_version::<F>(&config.path, &version)?;

        Ok(Self {
            id: get_next_tree_id(),
            memtable_id_counter: SequenceNumberCounter::new(1),
            table_id_counter: SequenceNumberCounter::default(),
            blob_file_id_counter: SequenceNumberCounter::default(),
            config: Arc::new(config),
            version_history: Arc::new(RwLock::new(SuperVersions::new(version))),
            stop_signal: StopSignal::default(),
            major_compaction_lock: RwLock::default(),
            flush_lock: Mutex::default(),
            compaction_state: Arc::new(Mutex::new(CompactionState::default())),

            #[cfg(feature = "metrics")]
            metrics: Metrics::default().into(),
        })
    }

    pub fn get_next_table_id(&self) -> TableId {
        self.table_id_counter.next()
    }
}

impl<F: FileSystem> Drop for TreeInner<F> {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::trace!("Sending stop signal to compactors");
        self.stop_signal.send();
    }
}
