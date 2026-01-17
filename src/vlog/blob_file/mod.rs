// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod merge;
pub mod meta;
pub mod multi_writer;
pub mod reader;
pub mod scanner;
pub mod writer;

use crate::{blob_tree::FragmentationMap, file_accessor::FileAccessor, vlog::BlobFileId, Checksum};
pub use meta::Metadata;
use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

/// A blob file is an immutable, sorted, contiguous file that contains large key-value pairs (blobs)
#[derive(Debug)]
pub struct Inner {
    /// Blob file ID
    pub id: BlobFileId,

    /// File path
    pub path: PathBuf,

    /// Statistics
    pub meta: Metadata,

    /// Whether this blob file is deleted (logically)
    pub is_deleted: AtomicBool,

    pub checksum: Checksum,

    pub file_accessor: FileAccessor,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            log::trace!(
                "Cleanup deleted blob file {:?} at {}",
                self.id,
                self.path.display(),
            );

            if let Err(e) = std::fs::remove_file(&*self.path) {
                log::warn!(
                    "Failed to cleanup deleted blob file {:?} at {}: {e:?}",
                    self.id,
                    self.path.display(),
                );
            }
        }
    }
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
    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Returns the blob file ID.
    #[must_use]
    pub fn id(&self) -> BlobFileId {
        self.0.id
    }

    /// Returns the full blob file checksum.
    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.0.checksum
    }

    /// Returns the blob file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// Returns the blob file accessor.
    #[must_use]
    pub fn file_accessor(&self) -> &FileAccessor {
        &self.0.file_accessor
    }

    /// Returns the number of items in the blob file.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.0.meta.item_count
    }

    /// Returns `true` if the blob file is stale (based on the given staleness threshold).
    pub(crate) fn is_stale(&self, frag_map: &FragmentationMap, threshold: f32) -> bool {
        frag_map.get(&self.id()).is_some_and(|x| {
            #[expect(
                clippy::cast_precision_loss,
                reason = "ok to lose precision as this is an approximate calculation"
            )]
            let stale_bytes = x.bytes as f32;
            #[expect(
                clippy::cast_precision_loss,
                reason = "ok to lose precision as this is an approximate calculation"
            )]
            let all_bytes = self.0.meta.total_uncompressed_bytes as f32;
            let ratio = stale_bytes / all_bytes;
            ratio >= threshold
        })
    }

    /// Returns `true` if the blob file has no more incoming references, and can be safely removed from a Version.
    pub(crate) fn is_dead(&self, frag_map: &FragmentationMap) -> bool {
        frag_map.get(&self.id()).is_some_and(|x| {
            let stale_bytes = x.bytes;
            let all_bytes = self.0.meta.total_uncompressed_bytes;
            stale_bytes == all_bytes
        })
    }
}
