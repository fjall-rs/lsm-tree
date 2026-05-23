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

/// Sink for hashes to be written into an on-disk filter (Bloom or otherwise).
///
/// # Calling protocol
///
/// The [`crate::table::writer::Writer`] follows this exact order for every
/// user key it writes. Implementations rely on this protocol — calling these
/// methods in a different order is a contract violation that can produce
/// filters whose TLI key does not match the keys whose hashes are actually
/// in the partition (silent data loss on read).
///
/// **Per user key:**
///
/// 1. [`notify_key`](FilterWriter::notify_key) is called first, with the new
///    user key. Partitioned implementations may spill the current buffered
///    partition here (using the *previous* user key as the TLI boundary).
///    Full-filter implementations no-op.
///
/// 2. Zero or more calls to [`register_bytes`](FilterWriter::register_bytes)
///    add prefix hashes (one per prefix returned by the configured
///    [`crate::prefix::PrefixExtractor`]). `register_bytes` MUST NOT spill —
///    intra-key spills would split a user key's hashes across two partitions.
///
/// 3. Optionally [`register_key`](FilterWriter::register_key) adds the
///    full-key hash. Whether this is called depends on `whole_key_filtering`
///    and whether an extractor is configured. Partitioned implementations
///    may spill here (using the *current* user key as the TLI boundary —
///    safe because all of this key's hashes are now buffered).
///
/// **At end of write:**
///
/// 4. [`finish`](FilterWriter::finish) is called once; it consumes the
///    writer and writes the filter to the file. Returns the number of filter
///    blocks written (always 1 for a full filter, ≥0 for partitioned).
///
/// # Dedup
///
/// When a prefix extractor is configured, many keys can produce the same
/// prefix hash. Implementations are expected to deduplicate hashes before
/// sizing the Bloom filter, gated by [`enable_dedup`](FilterWriter::enable_dedup).
/// The [`crate::table::writer::Writer`] enables dedup automatically when an
/// extractor is configured.
///
/// # Concurrency
///
/// All methods take `&mut self`. The writer is single-threaded by
/// construction.
pub trait FilterWriter<W: std::io::Write> {
    // NOTE: We purposefully use a UserKey instead of &[u8]
    // so we can clone it without heap allocation, if needed
    /// Registers a key in the filter by hashing it.
    ///
    /// Implementations MUST add the full-key hash to the buffer. Partitioned
    /// implementations MAY spill at this point (the current key is a safe
    /// TLI boundary because all of its hashes — prefixes and full-key — are
    /// now buffered).
    fn register_key(&mut self, key: &UserKey) -> crate::Result<()>;

    /// Registers arbitrary bytes into the filter (used for prefix entries).
    /// Implementations should hash the bytes identically to full keys.
    ///
    /// **Must NOT spill** even if the partition size threshold is exceeded.
    /// Spilling at this point would split a user key's prefix hashes across
    /// two partitions, breaking the TLI-boundary invariant. Spills are
    /// deferred to the next [`notify_key`](Self::notify_key) (or
    /// [`register_key`](Self::register_key) for the same user key).
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
