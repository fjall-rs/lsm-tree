// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    abstract_tree::NewFlushToTablesResult,
    compaction::worker::NewCompactionResult,
    version::{SuperVersionSet, Version},
    AbstractTree, MemtableId, SeqNo, SequenceNumberCounter, VersionSetProvider,
};
use std::sync::{Arc, RwLock};

/// This is only used for ephemeral tests.
pub struct MemoryVersionSetProvider {
    pub inner: Arc<RwLock<SuperVersionSet>>,
    memtable_id_generator: Arc<SequenceNumberCounter>,
}

impl Default for MemoryVersionSetProvider {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SuperVersionSet::new(Version::new(
                0,
                crate::TreeType::Standard,
            )))),
            memtable_id_generator: Arc::new(SequenceNumberCounter::default()),
        }
    }
}

impl MemoryVersionSetProvider {
    pub fn new(inner: Arc<RwLock<SuperVersionSet>>) -> Self {
        Self {
            inner,
            ..Default::default()
        }
    }

    /// Helper function to rotate and flush the active memtable.
    #[expect(clippy::expect_used)]
    pub fn flush_active_memtable(
        &self,
        tree: &impl AbstractTree,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        if self.rotate_active_memtable().is_none() {
            return Ok(());
        }

        let flush_result = tree
            .new_flush(gc_watermark)?
            .expect("should flush to table files");

        let _flushed_bytes = self.register_flush(flush_result, version_seqno);

        Ok(())
    }

    /// Helper function to run and register a compaction.
    pub fn compact(
        &self,
        tree: &impl AbstractTree,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        let compaction_result = tree.run_compaction(strategy, gc_watermark)?;

        self.register_compaction(compaction_result, version_seqno)?;
        // TODO: maybe we should then pass compaction_result into tree again so it can cleanup deleted files/mark as deleted etc?

        Ok(())
    }

    #[expect(clippy::unwrap_used)]
    #[expect(clippy::significant_drop_tightening)]
    fn register_compaction(
        &self,
        result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> crate::Result<()> {
        let mut lock = self.inner.write().unwrap();

        let version = lock
            .latest_version()
            .register_compaction(result, version_seqno)?;

        lock.append_version(version);
        Ok(())
    }

    #[expect(clippy::unwrap_used)]
    #[expect(clippy::must_use_candidate)]
    pub fn register_flush(&self, flush_result: NewFlushToTablesResult, seqno: SeqNo) -> u64 {
        let mut lock = self.inner.write().unwrap();
        let mut version = lock.latest_version().register_flush(flush_result);
        version.seqno = seqno;
        lock.append_version(version);
        0
    }

    #[expect(clippy::unwrap_used)]
    #[expect(clippy::significant_drop_tightening)]
    #[expect(clippy::must_use_candidate)]
    pub fn rotate_active_memtable(&self) -> Option<MemtableId> {
        log::trace!("Rotating active memtable");

        let mut lock = self.inner.write().unwrap();

        let memtable_id = self.memtable_id_generator.next();

        if let Some(version) = lock.latest_version().rotate_active_memtable(memtable_id) {
            lock.replace_latest_version(version);
            Some(memtable_id)
        } else {
            None
        }
    }
}

#[expect(clippy::unwrap_used)]
impl VersionSetProvider for MemoryVersionSetProvider {
    fn version_history_len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    fn latest_version(&self) -> crate::version::SuperVersion {
        self.inner.read().unwrap().latest_version()
    }

    fn get_version_for_seqno(&self, seqno: crate::SeqNo) -> crate::version::SuperVersion {
        self.inner.read().unwrap().get_version_for_snapshot(seqno)
    }
}
