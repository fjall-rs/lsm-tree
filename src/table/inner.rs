// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use super::{block_index::BlockIndexImpl, meta::ParsedMeta, regions::ParsedRegions};
use crate::{
    cache::Cache,
    descriptor_table::DescriptorTable,
    table::{filter::block::FilterBlock, IndexBlock},
    tree::inner::TreeId,
    Checksum, GlobalTableId, SeqNo,
};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, OnceLock},
};

pub struct Inner {
    pub path: Arc<PathBuf>,

    pub(crate) tree_id: TreeId,

    #[doc(hidden)]
    pub descriptor_table: Arc<DescriptorTable>,

    /// Parsed metadata
    #[doc(hidden)]
    pub metadata: ParsedMeta,

    /// Parsed region block handles
    #[doc(hidden)]
    pub regions: ParsedRegions,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    #[doc(hidden)]
    pub block_index: Arc<BlockIndexImpl>,

    /// Block cache
    ///
    /// Stores index and data blocks
    #[doc(hidden)]
    pub cache: Arc<Cache>,

    /// Pinned filter index (in case of partitioned filters)
    pub(super) pinned_filter_index: Option<IndexBlock>,

    /// Pinned AMQ filter
    pub pinned_filter_block: Option<FilterBlock>,

    /// True when the table was compacted away or dropped
    ///
    /// May be kept alive until all Arcs to the table have been dropped (to facilitate snapshots)
    pub is_deleted: AtomicBool,

    pub(super) checksum: Checksum,

    pub(super) global_seqno: SeqNo,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,

    /// Cached sum of referenced blob file bytes for this table.
    /// Lazily computed on first access to avoid repeated I/O in compaction decisions.
    pub(crate) cached_blob_bytes: OnceLock<u64>,
}

impl Inner {
    /// Gets the global table ID.
    #[must_use]
    pub(super) fn global_id(&self) -> GlobalTableId {
        (self.tree_id, self.metadata.id).into()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            let global_id = self.global_id();

            log::trace!("Cleanup deleted table {global_id:?} at {:?}", self.path);

            if let Err(e) = std::fs::remove_file(&*self.path) {
                log::warn!(
                    "Failed to cleanup deleted table {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }

            self.descriptor_table.remove_for_table(&global_id);
        }
    }
}
