// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::FilterWriter;
use crate::{
    checksum::ChecksummedWriter,
    config::BloomConstructionPolicy,
    table::{filter::standard_bloom::Builder, Block},
    CompressionType, UserKey,
};
use std::{fs::File, io::BufWriter};

pub struct FullFilterWriter {
    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,

    bloom_policy: BloomConstructionPolicy,

    /// When true, sort+dedup the hash buffer at finish time to eliminate
    /// duplicate prefix hashes. Enabled by `enable_dedup()` when a prefix
    /// extractor is configured.
    needs_dedup: bool,
}

impl FullFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            bloom_hash_buffer: Vec::new(),
            bloom_policy,
            needs_dedup: false,
        }
    }
}

impl<W: std::io::Write + std::io::Seek> FilterWriter<W> for FullFilterWriter {
    fn use_partition_size(self: Box<Self>, _: u32) -> Box<dyn FilterWriter<W>> {
        self
    }

    fn use_tli_compression(self: Box<Self>, _: CompressionType) -> Box<dyn FilterWriter<W>> {
        self
    }

    fn set_filter_policy(
        mut self: Box<Self>,
        policy: BloomConstructionPolicy,
    ) -> Box<dyn FilterWriter<W>> {
        self.bloom_policy = policy;
        self
    }

    fn register_key(&mut self, key: &UserKey) -> crate::Result<()> {
        self.bloom_hash_buffer.push(Builder::get_hash(key));
        Ok(())
    }

    fn register_bytes(&mut self, bytes: &[u8]) -> crate::Result<()> {
        self.bloom_hash_buffer.push(Builder::get_hash(bytes));
        Ok(())
    }

    fn enable_dedup(&mut self) {
        self.needs_dedup = true;
    }

    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
    ) -> crate::Result<usize> {
        if self.bloom_hash_buffer.is_empty() {
            log::trace!("Filter writer has no buffered hashes - not building filter");
            return Ok(0);
        }

        file_writer.start("filter")?;

        let mut hashes = self.bloom_hash_buffer;

        // When a prefix extractor is configured, multiple keys can produce the
        // same prefix hash. Sort + dedup so the filter is sized for the
        // true number of unique prefixes. Skipped for the full-key path where
        // each hash is already unique (the Writer deduplicates at the user-key
        // level before calling register_key).
        if self.needs_dedup {
            hashes.sort_unstable();
            hashes.dedup();
        }

        let n = hashes.len();

        log::trace!(
            "Constructing Bloom filter with {n} entries: {:?}",
            self.bloom_policy,
        );

        let start = std::time::Instant::now();

        let filter_bytes = {
            let mut builder = self.bloom_policy.init(n);

            for hash in hashes {
                builder.set_with_hash(hash);
            }

            builder.build()
        };

        log::trace!(
            "Built Bloom filter ({}B) in {:?}",
            filter_bytes.len(),
            start.elapsed(),
        );

        Block::write_into(
            file_writer,
            &filter_bytes,
            crate::table::block::BlockType::Filter,
            CompressionType::None,
        )?;

        Ok(1)
    }
}
