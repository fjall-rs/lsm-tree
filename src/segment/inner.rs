// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use super::{block_index::BlockIndexImpl, meta::ParsedMeta, regions::ParsedRegions, Block};
use crate::{
    cache::Cache, descriptor_table::DescriptorTable, prefix::SharedPrefixExtractor,
    tree::inner::TreeId, GlobalSegmentId,
};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
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

    /// Pinned AMQ filter
    pub pinned_filter_block: Option<Block>,

    /// Prefix extractor for filters
    pub prefix_extractor: Option<SharedPrefixExtractor>,

    /// Whether the prefix extractor is compatible with the one used during segment creation
    /// If false, prefix filter should not be used for this segment
    pub prefix_extractor_compatible: bool,

    // /// Pinned filter
    // #[doc(hidden)]
    // pub bloom_filter: Option<crate::bloom::BloomFilter>,
    pub is_deleted: AtomicBool,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let global_id: GlobalSegmentId = (self.tree_id, self.metadata.id).into();

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            log::trace!("Cleanup deleted segment {global_id:?} at {:?}", self.path);

            if let Err(e) = std::fs::remove_file(&*self.path) {
                log::warn!(
                    "Failed to cleanup deleted segment {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }
        }
    }
}
