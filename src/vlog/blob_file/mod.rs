// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod merge;
pub mod meta;
pub mod multi_writer;
pub mod reader;
pub mod scanner;
// pub mod trailer;
pub mod writer;

use crate::vlog::BlobFileId;
// pub use gc_stats::GcStats;
pub use meta::Metadata;
use std::{path::PathBuf, sync::Arc};

/// A blob file is an immutable, sorted, contiguous file that contains large key-value pairs (blobs)
#[derive(Debug)]
pub(crate) struct Inner {
    /// Blob file ID
    pub id: BlobFileId,

    /// File path
    pub path: PathBuf,

    /// Statistics
    pub meta: Metadata,
    // TODO: is_deleted, on Drop, like SST segments
}

/// A blob file stores large values and is part of the value log
#[derive(Clone)]
pub struct BlobFile(pub(crate) Arc<Inner>);

impl Eq for BlobFile {}

impl PartialEq for BlobFile {
    fn eq(&self, other: &Self) -> bool {
        self.id().eq(&other.id())
    }
}

impl std::hash::Hash for BlobFile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl BlobFile {
    /// Returns the blob file ID.
    #[must_use]
    pub fn id(&self) -> BlobFileId {
        self.0.id
    }

    /// Returns the number of items in the blob file.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.0.meta.item_count
    }
}
