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

    /// Enables hash deduplication at flush time. Should be called when a prefix
    /// extractor is configured, since multiple keys can produce the same prefix
    /// hash. Without a prefix extractor, each key produces a unique hash and
    /// dedup is unnecessary.
    fn enable_dedup(&mut self) {}

    /// Informs the filter writer that a new user key is about to be registered.
    /// Implementations may use this to spill an oversized buffered partition
    /// on key boundaries, so a partition's TLI key always corresponds to a
    /// key whose hashes are fully committed to that partition. No-op for
    /// non-partitioned filters.
    ///
    /// # Errors
    ///
    /// Returns an error if a partition spill triggered by this call fails.
    /// Only possible for partitioned implementations, which perform I/O
    /// during spills.
    fn notify_key(&mut self, _key: &UserKey) -> crate::Result<()> {
        Ok(())
    }

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
