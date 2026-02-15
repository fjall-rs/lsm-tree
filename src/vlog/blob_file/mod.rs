// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod merge;
pub mod meta;
pub mod multi_writer;
pub mod reader;
pub mod scanner;
pub mod writer;

use crate::{
    blob_tree::FragmentationMap, file_accessor::FileAccessor, fs::FileSystem, vlog::BlobFileId,
    Checksum, GlobalTableId, TreeId,
};
pub use meta::Metadata;
use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

/// A blob file is an immutable, sorted, contiguous file that contains large key-value pairs (blobs)
pub struct Inner<F: FileSystem> {
    /// Blob file ID
    pub id: BlobFileId,

    pub tree_id: TreeId,

    /// File path
    pub path: PathBuf,
    pub(crate) phantom: PhantomData<F>,

    /// Statistics
    pub meta: Metadata,

    /// Whether this blob file is deleted (logically)
    pub is_deleted: AtomicBool,

    pub checksum: Checksum,

    pub(crate) file_accessor: FileAccessor<F>,
}

impl<F: FileSystem> Inner<F> {
    fn global_id(&self) -> GlobalTableId {
        GlobalTableId::from((self.tree_id, self.id))
    }
}

impl<F: FileSystem> std::fmt::Debug for Inner<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("meta", &self.meta)
            .field("is_deleted", &self.is_deleted)
            .field("checksum", &self.checksum)
            .finish()
    }
}

impl<F: FileSystem> Drop for Inner<F> {
    fn drop(&mut self) {
        if self.is_deleted.load(std::sync::atomic::Ordering::Acquire) {
            log::trace!(
                "Cleanup deleted blob file {:?} at {}",
                self.id,
                self.path.display(),
            );

            if let Err(e) = F::remove_file(&self.path) {
                log::warn!(
                    "Failed to cleanup deleted blob file {:?} at {}: {e:?}",
                    self.id,
                    self.path.display(),
                );
            }

            self.file_accessor
                .as_descriptor_table()
                .inspect(|d| d.remove_for_blob_file(&self.global_id()));
        }
    }
}

/// A blob file stores large values and is part of the value log
pub struct BlobFile<F: FileSystem>(pub(crate) Arc<Inner<F>>);

impl<F: FileSystem> Clone for BlobFile<F> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<F: FileSystem> Eq for BlobFile<F> {}

impl<F: FileSystem> PartialEq for BlobFile<F> {
    fn eq(&self, other: &Self) -> bool {
        self.id().eq(&other.id())
    }
}

impl<F: FileSystem> std::hash::Hash for BlobFile<F> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl<F: FileSystem> BlobFile<F> {
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
    pub(crate) fn file_accessor(&self) -> &FileAccessor<F> {
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
