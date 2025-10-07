// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    config::Config, level_manifest::LevelManifest, memtable::Memtable, stop_signal::StopSignal,
    tree::sealed::SealedMemtables, SegmentId, SequenceNumberCounter,
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
/// Memtable IDs map one-to-one to some segment.
pub type MemtableId = u64;

/// Hands out a unique (monotonically increasing) tree ID.
pub fn get_next_tree_id() -> TreeId {
    static TREE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    TREE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub struct SuperVersion {
    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) manifest: LevelManifest,
}

#[allow(clippy::module_name_repetitions)]
pub struct TreeInner {
    /// Unique tree ID
    pub id: TreeId,

    /// Hands out a unique (monotonically increasing) table ID
    #[doc(hidden)]
    pub segment_id_counter: Arc<AtomicU64>,

    // This is not really used in the normal tree, but we need it in the blob tree
    /// Hands out a unique (monotonically increasing) blob file ID
    pub(crate) blob_file_id_generator: SequenceNumberCounter,

    pub(crate) super_version: Arc<RwLock<SuperVersion>>,

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

    // TODO: 3.0.0 compaction state
    // Serializes compactions when they look at the tree levels and prepare compactions
    pub(crate) compaction_lock: Arc<Mutex<()>>,

    #[doc(hidden)]
    #[cfg(feature = "metrics")]
    pub metrics: Arc<Metrics>,
}

impl TreeInner {
    pub(crate) fn create_new(config: Config) -> crate::Result<Self> {
        let manifest = LevelManifest::create_new(&config.path)?;

        Ok(Self {
            id: get_next_tree_id(),
            segment_id_counter: Arc::new(AtomicU64::default()),
            blob_file_id_generator: SequenceNumberCounter::default(),
            config,
            super_version: Arc::new(RwLock::new(SuperVersion {
                active_memtable: Arc::default(),
                sealed_memtables: Arc::default(),
                manifest,
            })),
            stop_signal: StopSignal::default(),
            major_compaction_lock: RwLock::default(),
            compaction_lock: Arc::default(),

            #[cfg(feature = "metrics")]
            metrics: Metrics::default().into(),
        })
    }

    pub fn get_next_segment_id(&self) -> SegmentId {
        self.segment_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::trace!("Sending stop signal to compactors");
        self.stop_signal.send();
    }
}
