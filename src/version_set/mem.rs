// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    abstract_tree::NewFlushToTablesResult,
    compaction::{drop_range::OwnedBounds, worker::NewCompactionResult},
    memtable::Memtable,
    version::{SuperVersion, SuperVersionSet, Version},
    AbstractTree, MemtableId, SeqNo, SequenceNumberCounter, Slice, VersionSetProvider,
};
use std::{
    ops::{Bound, RangeBounds},
    sync::{Arc, RwLock},
};

fn range_bounds_to_owned_bounds<K: AsRef<[u8]>, R: RangeBounds<K>>(
    range: &R,
) -> (OwnedBounds, bool) {
    let start = match range.start_bound() {
        Bound::Included(key) => Bound::Included(Slice::from(key.as_ref())),
        Bound::Excluded(key) => Bound::Excluded(Slice::from(key.as_ref())),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Bound::Included(Slice::from(key.as_ref())),
        Bound::Excluded(key) => Bound::Excluded(Slice::from(key.as_ref())),
        Bound::Unbounded => Bound::Unbounded,
    };

    let is_empty = if let (
        Bound::Included(lo) | Bound::Excluded(lo),
        Bound::Included(hi) | Bound::Excluded(hi),
    ) = (&start, &end)
    {
        lo.as_ref() > hi.as_ref()
    } else {
        false
    };

    (OwnedBounds { start, end }, is_empty)
}

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
            memtable_id_generator: Arc::new(SequenceNumberCounter::new(1)),
        }
    }
}

impl MemoryVersionSetProvider {
    fn new(inner: Arc<RwLock<SuperVersionSet>>) -> Self {
        Self {
            inner,
            ..Default::default()
        }
    }

    #[expect(clippy::unwrap_used)]
    pub fn overwrite(&self, super_version: SuperVersion) {
        let mut lock = self.inner.write().unwrap();
        lock.replace_latest_version(super_version);
    }

    #[must_use]
    pub fn with_initial_version(version: Version) -> Self {
        Self::new(Arc::new(RwLock::new(SuperVersionSet::new(version))))
    }

    #[expect(clippy::unwrap_used)]
    pub fn prune(&self) {
        let mut lock = self.inner.write().unwrap();

        let seqno = lock.latest_version().seqno;

        lock.maintenance(seqno);
    }

    /// Helper function to rotate and flush the active memtable.
    #[expect(clippy::expect_used)]
    pub fn flush_active_memtable(
        &self,
        tree: &impl AbstractTree,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<u64> {
        if self.rotate_active_memtable().is_none() {
            return Ok(0);
        }

        let flush_result = tree
            .new_flush(gc_watermark)?
            .expect("should flush to table files");

        Ok(self.register_flush(flush_result, version_seqno))
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

    /// Runs and registers a drop-range compaction.
    pub fn drop_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        tree: &impl AbstractTree,
        range: R,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<()> {
        let (bounds, is_empty) = range_bounds_to_owned_bounds(&range);

        if is_empty {
            return Ok(());
        }

        let strategy = Arc::new(crate::compaction::drop_range::Strategy::new(bounds));
        let compaction_result = tree.run_compaction(strategy, gc_watermark)?;

        self.register_compaction(compaction_result, version_seqno)
    }

    /// Appends a fresh, empty version while preserving the existing version history.
    #[expect(clippy::unwrap_used)]
    #[expect(clippy::significant_drop_tightening)]
    pub fn clear(&self, tree: &impl AbstractTree, version_seqno: SeqNo) -> crate::Result<()> {
        let mut lock = self.inner.write().unwrap();
        let previous_version = lock.latest_version();

        lock.append_version(SuperVersion {
            active_memtable: Arc::new(Memtable::new(self.memtable_id_generator.next())),
            sealed_memtables: Arc::default(),
            version: Version::new(previous_version.version.id() + 1, tree.tree_type()),
            seqno: version_seqno,
        });

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

        let (version, deleted) = lock
            .latest_version()
            .register_compaction(result, version_seqno)?;

        lock.append_version(version);

        // Only mark files as deleted once version is actually stable
        deleted.commit();

        Ok(())
    }

    #[expect(clippy::unwrap_used)]
    #[expect(clippy::must_use_candidate)]
    pub fn register_flush(&self, flush_result: NewFlushToTablesResult, seqno: SeqNo) -> u64 {
        let mut lock = self.inner.write().unwrap();
        let Some(mut version) = lock.latest_version().register_flush(flush_result) else {
            return 0;
        };
        version.seqno = seqno;
        lock.append_version(version);
        0 // TODO:
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
