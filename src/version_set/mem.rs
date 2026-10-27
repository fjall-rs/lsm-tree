// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    abstract_tree::NewFlushToTablesResult,
    compaction::{drop_range::OwnedBounds, worker::NewCompactionResult},
    memtable::Memtable,
    version::{SuperVersion, SuperVersionSet, Version},
    AbstractTree, MemtableId, SeqNo, Slice, VersionSetProvider,
};
use std::{
    ops::{Bound, RangeBounds},
    sync::{Arc, RwLock},
};

pub fn range_bounds_to_owned_bounds<K: AsRef<[u8]>, R: RangeBounds<K>>(
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

pub struct Inner(SuperVersionSet);

impl Default for Inner {
    fn default() -> Self {
        Self(SuperVersionSet::new(Version::new(
            0,
            crate::TreeType::Standard,
        )))
    }
}

impl Inner {
    fn new(vset: SuperVersionSet) -> Self {
        Self(vset)
    }

    fn overwrite(&mut self, super_version: SuperVersion) {
        self.0.replace_latest_version(super_version);
    }

    #[must_use]
    fn with_initial_version(version: Version) -> Self {
        Self::new(SuperVersionSet::new(version))
    }

    fn prune(&mut self) {
        let seqno = self.0.latest_version().seqno;
        self.0.maintenance(seqno);
    }

    /// Appends a fresh, empty version while preserving the existing version history.
    fn clear(&mut self, tree: &impl AbstractTree, version_seqno: SeqNo) {
        let previous_version = self.0.latest_version();

        self.0.append_version(SuperVersion {
            active_memtable: Arc::new(Memtable::new(tree.inner().memtable_id_counter.next())),
            sealed_memtables: Arc::default(),
            version: Version::new(previous_version.version.id() + 1, tree.tree_type()),
            seqno: version_seqno,
        });
    }

    fn register_compaction(
        &mut self,
        result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> crate::Result<()> {
        let (version, deleted) = self
            .0
            .latest_version()
            .register_compaction(result, version_seqno)?;

        self.0.append_version(version);

        // Only mark files as deleted once version is actually stable
        deleted.commit();

        Ok(())
    }

    fn register_flush(&mut self, flush_result: NewFlushToTablesResult, seqno: SeqNo) -> u64 {
        log::trace!("Registering flush result");

        let Some(mut version) = self.0.latest_version().register_flush(flush_result) else {
            return 0;
        };
        version.seqno = seqno;
        self.0.append_version(version);
        0 // TODO:
    }

    fn rotate_active_memtable(&mut self, tree: &impl AbstractTree) -> Option<MemtableId> {
        log::trace!("Rotating active memtable");

        let memtable_id = tree.inner().memtable_id_counter.next();

        if let Some(version) = self.0.latest_version().rotate_active_memtable(memtable_id) {
            self.0.replace_latest_version(version);
            Some(memtable_id)
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn latest_version(&self) -> crate::version::SuperVersion {
        self.0.latest_version()
    }

    fn get_version_for_snapshot(&self, seqno: SeqNo) -> crate::version::SuperVersion {
        self.0.get_version_for_snapshot(seqno)
    }
}

/// This is only used for ephemeral tests.
#[derive(Default)]
pub struct MemoryVersionSetProvider {
    inner: Arc<RwLock<Inner>>,
}

#[expect(clippy::unwrap_used, clippy::must_use_candidate)]
impl MemoryVersionSetProvider {
    pub fn new(vset: SuperVersionSet) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::new(vset))),
        }
    }

    pub fn overwrite(&self, super_version: SuperVersion) {
        self.inner.write().unwrap().overwrite(super_version);
    }

    #[must_use]
    pub fn with_initial_version(version: Version) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::with_initial_version(version))),
        }
    }

    pub fn prune(&self) {
        self.inner.write().unwrap().prune();
    }

    pub fn flush_active_memtable(
        &self,
        tree: &impl AbstractTree,
        version_seqno: SeqNo,
        gc_watermark: SeqNo,
    ) -> crate::Result<u64> {
        if self.rotate_active_memtable(tree).is_none() {
            return Ok(0);
        }

        log::trace!("Flushing write buffer");
        let flush_result = tree
            .new_flush(gc_watermark)?
            .expect("should flush to table files");

        Ok(self.register_flush(flush_result, version_seqno))
    }

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

    pub fn clear(&self, tree: &impl AbstractTree, version_seqno: SeqNo) -> crate::Result<()> {
        self.inner.write().unwrap().clear(tree, version_seqno);
        Ok(())
    }

    pub fn register_compaction(
        &self,
        result: NewCompactionResult,
        version_seqno: SeqNo,
    ) -> crate::Result<()> {
        self.inner
            .write()
            .unwrap()
            .register_compaction(result, version_seqno)
    }

    pub fn register_flush(&self, flush_result: NewFlushToTablesResult, seqno: SeqNo) -> u64 {
        self.inner
            .write()
            .unwrap()
            .register_flush(flush_result, seqno)
    }

    pub fn rotate_active_memtable(&self, tree: &impl AbstractTree) -> Option<MemtableId> {
        self.inner.write().unwrap().rotate_active_memtable(tree)
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

    fn get_version_for_seqno(&self, seqno: SeqNo) -> crate::version::SuperVersion {
        self.inner.read().unwrap().get_version_for_snapshot(seqno)
    }
}
