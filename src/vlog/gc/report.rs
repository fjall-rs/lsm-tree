// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::path::PathBuf;

/// Statistics report for garbage collection
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct GcReport {
    /// Path of value log
    pub path: PathBuf,

    /// Blob file count
    pub blob_file_count: usize,

    /// Blob files that have 100% stale blobs
    pub stale_blob_file_count: usize,

    /// Amount of stored bytes
    pub total_bytes: u64,

    /// Amount of bytes that could be freed
    pub stale_bytes: u64,

    /// Number of stored blobs
    pub total_blobs: u64,

    /// Number of blobs that could be freed
    pub stale_blobs: u64,
}

impl std::fmt::Display for GcReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "--- GC report for vLog @ {} ---", self.path.display())?;
        writeln!(f, "# files    : {}", self.blob_file_count)?;
        writeln!(f, "# stale    : {}", self.stale_blob_file_count)?;
        writeln!(f, "Total bytes: {}", self.total_bytes)?;
        writeln!(f, "Stale bytes: {}", self.stale_bytes)?;
        writeln!(f, "Total blobs: {}", self.total_blobs)?;
        writeln!(f, "Stale blobs: {}", self.stale_blobs)?;
        writeln!(f, "Stale ratio: {}", self.stale_ratio())?;
        writeln!(f, "Space amp  : {}", self.space_amp())?;
        writeln!(f, "--- GC report done ---")?;
        Ok(())
    }
}

impl GcReport {
    /// Calculates the space amplification factor.
    #[must_use]
    pub fn space_amp(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }

        let alive_bytes = self.total_bytes - self.stale_bytes;
        if alive_bytes == 0 {
            return 0.0;
        }

        self.total_bytes as f32 / alive_bytes as f32
    }

    /// Calculates the stale ratio (percentage).
    #[must_use]
    pub fn stale_ratio(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }

        if self.stale_bytes == 0 {
            return 0.0;
        }

        self.stale_bytes as f32 / self.total_bytes as f32
    }
}
