// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    config::Config, file::LEVELS_MANIFEST_FILE, level_manifest::LevelManifest, memtable::Memtable,
    segment::meta::SegmentId, stop_signal::StopSignal,
};
use std::sync::{atomic::AtomicU64, Arc, RwLock};

/// Unique tree ID
///
/// Tree IDs are monotonically increasing integers.
pub type TreeId = u64;

/// Unique memtable ID
///
/// Memtable IDs map one-to-one to some segment.
pub type MemtableId = u64;

/// Stores references to all sealed memtables
///
/// Memtable IDs are monotonically increasing, so we don't really
/// need a search tree; also there are only a handful of them at most.
#[derive(Default)]
pub struct SealedMemtables(Vec<(MemtableId, Arc<Memtable>)>);

impl SealedMemtables {
    pub fn add(&mut self, id: MemtableId, memtable: Arc<Memtable>) {
        self.0.push((id, memtable));
    }

    pub fn remove(&mut self, id_to_remove: MemtableId) {
        self.0.retain(|(id, _)| *id != id_to_remove);
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(MemtableId, Arc<Memtable>)> {
        self.0.iter()
    }
}

/// Hands out a unique (monotonically increasing) tree ID
pub fn get_next_tree_id() -> TreeId {
    static TREE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    TREE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[allow(clippy::module_name_repetitions)]
pub struct TreeInner {
    /// Unique tree ID
    pub id: TreeId,

    /// Hands out a unique (monotonically increasing) segment ID
    #[doc(hidden)]
    pub segment_id_counter: Arc<AtomicU64>,

    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<RwLock<Memtable>>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<RwLock<SealedMemtables>>,

    /// Level manifest
    #[doc(hidden)]
    pub levels: Arc<RwLock<LevelManifest>>,

    /// Tree configuration
    pub config: Config,

    /// Compaction may take a while; setting the signal to `true`
    /// will interrupt the compaction and kill the worker.
    pub(crate) stop_signal: StopSignal,
}

impl TreeInner {
    pub(crate) fn create_new(config: Config) -> crate::Result<Self> {
        let levels = LevelManifest::create_new(
            config.inner.level_count,
            config.path.join(LEVELS_MANIFEST_FILE),
        )?;

        Ok(Self {
            id: get_next_tree_id(),
            segment_id_counter: Arc::new(AtomicU64::default()),
            config,
            active_memtable: Arc::default(),
            sealed_memtables: Arc::default(),
            levels: Arc::new(RwLock::new(levels)),
            stop_signal: StopSignal::default(),
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
