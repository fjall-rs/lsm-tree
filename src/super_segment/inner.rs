// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    block_index::NewBlockIndexImpl, filter::standard_bloom::StandardBloomFilter, meta::ParsedMeta,
    trailer::Trailer,
};
use crate::{
    new_cache::NewCache, new_descriptor_table::NewDescriptorTable, tree::inner::TreeId,
    GlobalSegmentId,
};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

pub struct Inner {
    pub path: PathBuf,

    pub(crate) tree_id: TreeId,

    #[doc(hidden)]
    pub descriptor_table: Arc<NewDescriptorTable>,

    /// Segment metadata object
    #[doc(hidden)]
    pub metadata: ParsedMeta,

    pub(crate) trailer: Trailer, // TODO: remove...?

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    #[doc(hidden)]
    pub block_index: Arc<NewBlockIndexImpl>,

    /// Block cache
    ///
    /// Stores index and data blocks
    #[doc(hidden)]
    pub cache: Arc<NewCache>,

    /// Pinned AMQ filter
    pub pinned_filter: Option<StandardBloomFilter>,

    // /// Pinned filter
    // #[doc(hidden)]
    // pub bloom_filter: Option<crate::bloom::BloomFilter>,
    pub is_deleted: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let global_id: GlobalSegmentId = (self.tree_id, self.metadata.id).into();

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            log::trace!("Cleanup deleted segment {global_id:?} at {:?}", self.path);

            if let Err(e) = std::fs::remove_file(&self.path) {
                log::warn!(
                    "Failed to cleanup deleted segment {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }
        }
    }
}
