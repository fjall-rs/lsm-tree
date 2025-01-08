// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{block_index::BlockIndexImpl, file_offsets::FileOffsets, meta::Metadata};
use crate::{block_cache::BlockCache, descriptor_table::FileDescriptorTable, tree::inner::TreeId};
use std::sync::Arc;

pub struct Inner {
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
    pub block_cache: Arc<BlockCache>,

    /// Bloom filter
    #[cfg(feature = "bloom")]
    #[doc(hidden)]
    pub bloom_filter: Option<crate::bloom::BloomFilter>,
}
