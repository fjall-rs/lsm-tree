// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::state::CompactionState,
    config::Config,
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

pub struct TreeInner {
    /// Unique tree ID
    pub id: TreeId,

    /// Hands out a unique (monotonically increasing) table ID
    #[doc(hidden)]
    pub table_id_counter: SequenceNumberCounter,

    // This is not really used in the normal tree, but we need it in the blob tree
    /// Hands out a unique (monotonically increasing) blob file ID
    pub(crate) blob_file_id_generator: SequenceNumberCounter,

    pub(crate) version_history: Arc<RwLock<SuperVersions>>,

    pub(crate) compaction_state: Arc<Mutex<CompactionState>>,

    /// Tree configuration
    pub config: Config,

    /// Compaction may take a while; setting the signal to `true`
    /// will interrupt the compaction and kill the worker.
    pub(crate) stop_signal: StopSignal,

    /// Used by major compaction to be the exclusive compaction going on.
    ///
    /// Minor compactions use `major_compaction_lock.read()` instead, so they
    /// can be concurrent next to each other.
    pub(crate) major_compaction_lock: RwLock<()>,

    pub(crate) flush_lock: Mutex<()>,

    #[doc(hidden)]
    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl TreeInner {
    pub(crate) fn create_new(config: Config) -> crate::Result<Self> {
        let version = Version::new(0);
        persist_version(&config.path, &version)?;

        Ok(Self {
            id: get_next_tree_id(),
            table_id_counter: SequenceNumberCounter::default(),
            blob_file_id_generator: SequenceNumberCounter::default(),
            config,
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

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::trace!("Sending stop signal to compactors");
        self.stop_signal.send();
    }
}
