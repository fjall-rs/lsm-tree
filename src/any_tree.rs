// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    abstract_tree::FlushToTablesResult,
    compaction::worker::NewCompactionResult,
    iter_guard::IterGuardImpl,
    table::Table,
    version::{SuperVersion, Version},
    vlog::BlobFile,
    AbstractTree, BlobTree, Config, InternalValue, Memtable, SeqNo, TableId, Tree, UserKey,
    UserValue,
};
use std::{
    ops::RangeBounds,
    sync::{Arc, MutexGuard},
};

/// May be a standard [`Tree`] or a [`BlobTree`]
#[derive(Clone)]
pub enum AnyTree {
    /// Standard LSM-tree, see [`Tree`]
    Standard(Tree),

    /// Key-value separated LSM-tree, see [`BlobTree`]
    Blob(BlobTree),
}

impl From<Tree> for AnyTree {
    fn from(tree: Tree) -> Self {
        Self::Standard(tree)
    }
}

impl From<BlobTree> for AnyTree {
    fn from(tree: BlobTree) -> Self {
        Self::Blob(tree)
    }
}

/// Calls the same method on whichever tree type is inside
macro_rules! dispatch {
    ($self:expr, $tree:ident => $call:expr) => {
        match $self {
            AnyTree::Standard($tree) => $call,
            AnyTree::Blob($tree) => $call,
        }
    };
}

// NOTE: Every method is forwarded, including the ones with a default implementation,
// because `Tree` and `BlobTree` may override them
impl AbstractTree for AnyTree {
    fn print_trace(&self, key: &[u8]) -> crate::Result<()> {
        dispatch!(self, t => t.print_trace(key))
    }

    fn table_file_cache_size(&self) -> usize {
        dispatch!(self, t => t.table_file_cache_size())
    }

    fn version_memtable_size_sum(&self) -> u64 {
        dispatch!(self, t => t.version_memtable_size_sum())
    }

    fn inner(&self) -> &crate::tree::inner::TreeInner {
        dispatch!(self, t => t.inner())
    }

    fn clear_active_memtable(&self) -> Option<SuperVersion> {
        dispatch!(self, t => t.clear_active_memtable())
    }

    fn active_memtable(&self) -> Arc<Memtable> {
        dispatch!(self, t => t.active_memtable())
    }

    fn next_table_id(&self) -> TableId {
        dispatch!(self, t => t.next_table_id())
    }

    fn id(&self) -> crate::TreeId {
        dispatch!(self, t => t.id())
    }

    fn get_internal_entry(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        dispatch!(self, t => t.get_internal_entry(key, seqno))
    }

    fn current_super_version(&self) -> SuperVersion {
        dispatch!(self, t => t.current_super_version())
    }

    fn current_version(&self) -> Version {
        dispatch!(self, t => t.current_version())
    }

    fn iter(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        dispatch!(self, t => t.iter(seqno, index))
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        prefix: K,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        dispatch!(self, t => t.prefix(prefix, seqno, index))
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Box<dyn DoubleEndedIterator<Item = IterGuardImpl> + Send + 'static> {
        dispatch!(self, t => t.range(range, seqno, index))
    }

    fn tombstone_count(&self) -> u64 {
        dispatch!(self, t => t.tombstone_count())
    }

    fn weak_tombstone_count(&self) -> u64 {
        dispatch!(self, t => t.weak_tombstone_count())
    }

    fn weak_tombstone_reclaimable_count(&self) -> u64 {
        dispatch!(self, t => t.weak_tombstone_reclaimable_count())
    }

    fn stale_blob_bytes(&self) -> u64 {
        dispatch!(self, t => t.stale_blob_bytes())
    }

    fn filter_size(&self) -> u64 {
        dispatch!(self, t => t.filter_size())
    }

    fn pinned_filter_size(&self) -> usize {
        dispatch!(self, t => t.pinned_filter_size())
    }

    fn pinned_block_index_size(&self) -> usize {
        dispatch!(self, t => t.pinned_block_index_size())
    }

    fn version_set_len(&self) -> usize {
        dispatch!(self, t => t.version_set_len())
    }

    #[cfg(feature = "metrics")]
    fn metrics(&self) -> &Arc<crate::Metrics> {
        dispatch!(self, t => t.metrics())
    }

    fn run_compaction(
        &self,
        strategy: Arc<dyn crate::compaction::CompactionStrategy>,
        gc_watermark: SeqNo,
    ) -> crate::Result<NewCompactionResult> {
        dispatch!(self, t => t.run_compaction(strategy, gc_watermark))
    }

    fn flush(&self, gc_watermark: SeqNo) -> crate::Result<Option<FlushToTablesResult>> {
        dispatch!(self, t => t.flush(gc_watermark))
    }

    fn get_flush_lock(&self) -> MutexGuard<'_, ()> {
        dispatch!(self, t => t.get_flush_lock())
    }

    #[allow(clippy::type_complexity)]
    fn flush_to_tables(
        &self,
        stream: impl Iterator<Item = crate::Result<InternalValue>>,
    ) -> crate::Result<Option<(Vec<Table>, Option<Vec<BlobFile>>)>> {
        dispatch!(self, t => t.flush_to_tables(stream))
    }

    fn sealed_memtable_count(&self) -> usize {
        dispatch!(self, t => t.sealed_memtable_count())
    }

    fn get_next_table_id(&self) -> TableId {
        dispatch!(self, t => t.get_next_table_id())
    }

    fn tree_config(&self) -> &Config {
        dispatch!(self, t => t.tree_config())
    }

    fn get_highest_seqno(&self) -> Option<SeqNo> {
        dispatch!(self, t => t.get_highest_seqno())
    }

    fn tree_type(&self) -> crate::TreeType {
        dispatch!(self, t => t.tree_type())
    }

    fn table_count(&self) -> usize {
        dispatch!(self, t => t.table_count())
    }

    fn level_table_count(&self, idx: usize) -> Option<usize> {
        dispatch!(self, t => t.level_table_count(idx))
    }

    fn l0_run_count(&self) -> usize {
        dispatch!(self, t => t.l0_run_count())
    }

    fn blob_file_count(&self) -> usize {
        dispatch!(self, t => t.blob_file_count())
    }

    fn approximate_len(&self) -> usize {
        dispatch!(self, t => t.approximate_len())
    }

    fn disk_space(&self) -> u64 {
        dispatch!(self, t => t.disk_space())
    }

    fn get_highest_memtable_seqno(&self) -> Option<SeqNo> {
        dispatch!(self, t => t.get_highest_memtable_seqno())
    }

    fn get_flush_watermark(&self) -> Option<SeqNo> {
        dispatch!(self, t => t.get_flush_watermark())
    }

    fn len(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<usize> {
        dispatch!(self, t => t.len(seqno, index))
    }

    fn is_empty(&self, seqno: SeqNo, index: Option<(Arc<Memtable>, SeqNo)>) -> crate::Result<bool> {
        dispatch!(self, t => t.is_empty(seqno, index))
    }

    fn first_key_value(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        dispatch!(self, t => t.first_key_value(seqno, index))
    }

    fn last_key_value(
        &self,
        seqno: SeqNo,
        index: Option<(Arc<Memtable>, SeqNo)>,
    ) -> Option<IterGuardImpl> {
        dispatch!(self, t => t.last_key_value(seqno, index))
    }

    fn size_of<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<u32>> {
        dispatch!(self, t => t.size_of(key, seqno))
    }

    fn get<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<Option<UserValue>> {
        dispatch!(self, t => t.get(key, seqno))
    }

    fn contains_key<K: AsRef<[u8]>>(&self, key: K, seqno: SeqNo) -> crate::Result<bool> {
        dispatch!(self, t => t.contains_key(key, seqno))
    }

    fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
        seqno: SeqNo,
    ) -> (u64, u64) {
        dispatch!(self, t => t.insert(key, value, seqno))
    }

    fn remove<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        dispatch!(self, t => t.remove(key, seqno))
    }

    fn remove_weak<K: Into<UserKey>>(&self, key: K, seqno: SeqNo) -> (u64, u64) {
        dispatch!(self, t => t.remove_weak(key, seqno))
    }
}
