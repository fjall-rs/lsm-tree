// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    compaction::state::CompactionState,
    config::Config,
    memtable::Memtable,
    stop_signal::StopSignal,
    tree::sealed::SealedMemtables,
    version::{persist_version, Version},
    SeqNo, SequenceNumberCounter, TableId,
};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};

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

#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version,

    pub(crate) seqno: SeqNo,
}

pub struct SuperVersions(VecDeque<SuperVersion>);

impl SuperVersions {
    pub fn new(version: Version) -> Self {
        Self(
            vec![SuperVersion {
                active_memtable: Arc::default(),
                sealed_memtables: Arc::default(),
                version,
                seqno: 0,
            }]
            .into(),
        )
    }

    pub fn free_list_len(&self) -> usize {
        self.0.len().saturating_sub(1)
    }

    pub(crate) fn maintenance(&mut self, folder: &Path, gc_watermark: SeqNo) -> crate::Result<()> {
        log::trace!("Running manifest GC with watermark={gc_watermark}");

        // todo!();
        // TODO: 3.0.0 restore in SuperVersions
        loop {
            if self.free_list_len() == 0 {
                break;
            }

            let Some(head) = self.0.front() else {
                break;
            };

            if head.seqno < gc_watermark {
                let path = folder.join(format!("v{}", head.version.id()));
                if path.try_exists()? {
                    std::fs::remove_file(path)?;
                }
                self.0.pop_front();
            } else {
                break;
            }
        }

        log::trace!("Manifest GC done, version length now {}", self.0.len());

        Ok(())
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk.
    pub(crate) fn upgrade_version<F: FnOnce(&SuperVersion) -> crate::Result<SuperVersion>>(
        &mut self,
        tree_path: &Path,
        f: F,
        seqno: &SequenceNumberCounter,
    ) -> crate::Result<()> {
        // NOTE: Copy-on-write...
        //
        // Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let mut next_version = f(&self.latest_version())?;
        next_version.seqno = seqno.next();
        log::trace!("Next version seqno={}", next_version.seqno);

        persist_version(tree_path, &next_version.version)?;
        self.append_version(next_version);

        Ok(())
    }

    pub fn append_version(&mut self, version: SuperVersion) {
        self.0.push_back(version);
    }

    pub fn latest_version(&self) -> SuperVersion {
        self.0
            .iter()
            .last()
            .cloned()
            .expect("should always have a SuperVersion")
    }

    pub fn get_version_for_snapshot(&self, seqno: SeqNo) -> SuperVersion {
        self.0
            .iter()
            .rev()
            .find(|version| version.seqno < seqno)
            .cloned()
            .expect("should always find a SuperVersion")
    }

    pub fn append_sealed_memtable(&mut self, id: MemtableId, memtable: Arc<Memtable>) {
        let mut copy = self.latest_version();
        copy.sealed_memtables = Arc::new(copy.sealed_memtables.add(id, memtable));
        self.0.push_back(copy);
    }
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
