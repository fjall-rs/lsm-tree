// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{block_index::BlockIndexImpl, file_offsets::FileOffsets, meta::Metadata};
use crate::{
    cache::Cache, descriptor_table::FileDescriptorTable, prefix::SharedPrefixExtractor,
    tree::inner::TreeId,
};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

pub struct Inner {
    pub path: PathBuf,

    pub(crate) tree_id: TreeId,

    #[doc(hidden)]
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Segment metadata object
    #[doc(hidden)]
    pub metadata: Metadata,

    pub(crate) offsets: FileOffsets,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    #[doc(hidden)]
    pub block_index: Arc<BlockIndexImpl>,

    /// Block cache
    ///
    /// Stores index and data blocks
    #[doc(hidden)]
    pub cache: Arc<Cache>,

    /// Bloom filter
    #[doc(hidden)]
    pub bloom_filter: Option<crate::bloom::BloomFilter>,

    /// Prefix extractor used for bloom filter
    pub(crate) prefix_extractor: Option<SharedPrefixExtractor>,

    pub is_deleted: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let global_id = (self.tree_id, self.metadata.id).into();

        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            if let Err(e) = std::fs::remove_file(&self.path) {
                log::warn!(
                    "Failed to cleanup deleted segment {global_id:?} at {:?}: {e:?}",
                    self.path,
                );
            }

            log::trace!("Closing file handles of deleted segment file {global_id:?}");
            self.descriptor_table.remove(global_id);
        }
    }
}
