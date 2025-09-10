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

use crate::vlog::{BlobFileId, Compressor};
use gc_stats::GcStats;
use meta::Metadata;
use std::{marker::PhantomData, path::PathBuf};

/// A blob file is an immutable, sorted, contiguous file that contains large key-value pairs (blobs)
#[derive(Debug)]
pub struct BlobFile<C: Compressor + Clone> {
    /// Blob file ID
    pub id: BlobFileId,

    /// File path
    pub path: PathBuf,

    /// Statistics
    pub meta: Metadata,

    /// Runtime stats for garbage collection
    pub gc_stats: GcStats,

    pub(crate) _phantom: PhantomData<C>,
}

impl<C: Compressor + Clone> BlobFile<C> {
    /// Returns a scanner that can iterate through the blob file.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn scan(&self) -> crate::Result<reader::Reader<C>> {
        reader::Reader::new(&self.path, self.id)
    }

    /// Returns the amount of items in the blob file.
    pub fn len(&self) -> u64 {
        self.meta.item_count
    }

    /// Marks the blob file as fully stale.
    pub(crate) fn mark_as_stale(&self) {
        self.gc_stats.set_stale_items(self.meta.item_count);

        self.gc_stats
            .set_stale_bytes(self.meta.total_uncompressed_bytes);
    }

    /// Returns `true` if the blob file is fully stale.
    pub fn is_stale(&self) -> bool {
        self.gc_stats.stale_items() == self.meta.item_count
    }

    /// Returns the percent of dead items in the blob file.
    // NOTE: Precision is not important here
    #[allow(clippy::cast_precision_loss)]
    pub fn stale_ratio(&self) -> f32 {
        let dead = self.gc_stats.stale_items() as f32;
        if dead == 0.0 {
            return 0.0;
        }

        dead / self.meta.item_count as f32
    }
}
