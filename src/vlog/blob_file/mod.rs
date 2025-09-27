// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod gc_stats;
pub mod merge;
pub mod meta;
pub mod multi_writer;
pub mod reader;
pub mod trailer;
pub mod writer;

use crate::vlog::BlobFileId;
pub use gc_stats::GcStats;
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

    /// Runtime stats for garbage collection
    pub gc_stats: GcStats,
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

    /// Returns a scanner that can iterate through the blob file.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn scan(&self) -> crate::Result<reader::Reader> {
        reader::Reader::new(&self.0.path, self.id())
    }

    /// Returns the number of items in the blob file.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.0.meta.item_count
    }

    /// Marks the blob file as fully stale.
    pub(crate) fn mark_as_stale(&self) {
        self.0.gc_stats.set_stale_items(self.0.meta.item_count);

        self.0
            .gc_stats
            .set_stale_bytes(self.0.meta.total_uncompressed_bytes);
    }

    /// Returns `true` if the blob file is fully stale.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.0.gc_stats.stale_items() == self.0.meta.item_count
    }

    /// Returns the percent of dead items in the blob file.
    // NOTE: Precision is not important here
    #[allow(clippy::cast_precision_loss)]
    #[must_use]
    pub fn stale_ratio(&self) -> f32 {
        let dead = self.0.gc_stats.stale_items() as f32;
        if dead == 0.0 {
            return 0.0;
        }

        dead / self.0.meta.item_count as f32
    }
}
