// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{Cache, CompressionType, DescriptorTable};
use std::sync::Arc;

/// Value log configuration
pub struct Config {
    /// Target size of vLog blob files
    pub(crate) blob_file_size_bytes: u64,

    /// Blob cache to use
    pub(crate) blob_cache: Arc<Cache>,

    /// File descriptor cache to use
    pub(crate) fd_cache: Arc<DescriptorTable>,

    /// Compression to use
    pub(crate) compression: CompressionType,
}

impl Config {
    /// Creates a new configuration builder.
    pub fn new(blob_cache: Arc<Cache>, fd_cache: Arc<DescriptorTable>) -> Self {
        Self {
            blob_cache,
            fd_cache,
            compression: CompressionType::None,
            blob_file_size_bytes: 128 * 1_024 * 1_024,
        }
    }

    /// Sets the compression & decompression scheme.
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the blob cache.
    ///
    /// You can create a global [`Cache`] and share it between multiple
    /// value logs to cap global cache memory usage.
    #[must_use]
    pub fn blob_cache(mut self, blob_cache: Arc<Cache>) -> Self {
        self.blob_cache = blob_cache;
        self
    }

    /// Sets the maximum size of value log blob files.
    ///
    /// This influences space amplification, as
    /// space reclamation works on a per-file basis.
    ///
    /// Larger files results in less files on disk and thus less file descriptors that may have to be obtained or cached.
    ///
    /// Like `blob_file_size` in `RocksDB`.
    ///
    /// Default = 256 MiB
    #[must_use]
    pub fn blob_file_size_bytes(mut self, bytes: u64) -> Self {
        self.blob_file_size_bytes = bytes;
        self
    }
}
