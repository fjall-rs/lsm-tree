use crate::{
    config::{Config, PersistedConfig},
    descriptor_table::FileDescriptorTable,
    file::LEVELS_MANIFEST_FILE,
    levels::LevelManifest,
    memtable::MemTable,
    segment::meta::SegmentId,
    snapshot::Counter as SnapshotCounter,
    stop_signal::StopSignal,
    BlockCache,
};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

#[doc(hidden)]
pub type TreeId = u64;

pub type MemtableId = u64;

pub type SealedMemtables = BTreeMap<MemtableId, Arc<MemTable>>;

pub fn get_next_tree_id() -> TreeId {
    static TREE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
    TREE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub struct TreeInner {
    pub id: TreeId,

    pub(crate) segment_id_counter: Arc<AtomicU64>,

    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<RwLock<SealedMemtables>>,

    /// Level manifest
    #[doc(hidden)]
    pub levels: Arc<RwLock<LevelManifest>>,

    /// Tree configuration
    pub config: PersistedConfig,

    /// Block cache
    pub block_cache: Arc<BlockCache>,

    /// File descriptor cache table
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Keeps track of open snapshots
    pub(crate) open_snapshots: SnapshotCounter,

    /// Compaction may take a while; setting the signal to `true`
    /// will interrupt the compaction and kill the worker.
    pub(crate) stop_signal: StopSignal,
}

impl TreeInner {
    pub(crate) fn create_new(config: Config) -> crate::Result<Self> {
        let levels = LevelManifest::create_new(
            config.inner.level_count,
            config.inner.path.join(LEVELS_MANIFEST_FILE),
        )?;

        Ok(Self {
            id: get_next_tree_id(),
            segment_id_counter: Arc::new(AtomicU64::default()),
            config: config.inner,
            block_cache: config.block_cache,
            descriptor_table: config.descriptor_table,
            active_memtable: Arc::default(),
            sealed_memtables: Arc::default(),
            levels: Arc::new(RwLock::new(levels)),
            open_snapshots: SnapshotCounter::default(),
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
