// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod full;
mod partitioned;

pub use full::FullFilterWriter;
pub use partitioned::PartitionedFilterWriter;

use crate::{
    checksum::ChecksummedWriter, config::BloomConstructionPolicy, CompressionType, UserKey,
};
use std::{fs::File, io::BufWriter};

pub trait FilterWriter<W: std::io::Write> {
    // NOTE: We purposefully use a UserKey instead of &[u8]
    // so we can clone it without heap allocation, if needed
    /// Registers a key in the filter by hashing it.
    fn register_key(&mut self, key: &UserKey) -> crate::Result<()>;

    /// Registers arbitrary bytes into the filter (used for prefix entries).
    /// Implementations should hash the bytes identically to full keys.
    fn register_bytes(&mut self, bytes: &[u8]) -> crate::Result<()>;

    /// Informs the filter writer about the current user key for partition boundary
    /// tracking without adding its hash to the filter. This is needed when a prefix
    /// extractor is configured: only extracted prefixes are hashed (via `register_bytes`),
    /// but the partitioned filter writer still needs the actual user key to create
    /// correct top-level index entries. No-op for non-partitioned filters.
    fn notify_key(&mut self, _key: &UserKey) {}

    /// Writes the filter to a file.
    ///
    /// Returns the number of filter blocks written (always 1 in case of full filter block).
    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
    ) -> crate::Result<usize>;

    fn set_filter_policy(
        self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>>;

    fn use_tli_compression(
        self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>>;

    fn use_partition_size(self: Box<Self>, size: u32) -> Box<dyn FilterWriter<W>>;
}
