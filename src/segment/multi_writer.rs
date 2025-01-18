// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    trailer::SegmentFileTrailer,
    writer::{Options, Writer},
};
use crate::{value::InternalValue, CompressionType};
use std::sync::{atomic::AtomicU64, Arc};

#[cfg(feature = "bloom")]
use super::writer::BloomConstructionPolicy;

/// Like `Writer` but will rotate to a new segment, once a segment grows larger than `target_size`
///
/// This results in a sorted "run" of segments
#[allow(clippy::module_name_repetitions)]
pub struct MultiWriter {
    /// Target size of segments in bytes
    ///
    /// If a segment reaches the target size, a new one is started,
    /// resulting in a sorted "run" of segments
    pub target_size: u64,

    pub opts: Options,
    results: Vec<SegmentFileTrailer>,

    segment_id_generator: Arc<AtomicU64>,
    current_segment_id: u64,

    pub writer: Writer,

    pub compression: CompressionType,

    #[cfg(feature = "bloom")]
    bloom_policy: BloomConstructionPolicy,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(
        segment_id_generator: Arc<AtomicU64>,
        target_size: u64,
        opts: Options,
    ) -> crate::Result<Self> {
        let current_segment_id =
            segment_id_generator.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let writer = Writer::new(Options {
            segment_id: current_segment_id,
            folder: opts.folder.clone(),
            data_block_size: opts.data_block_size,
            index_block_size: opts.index_block_size,
            prefix_extractor: opts.prefix_extractor.clone(),
        })?;

        Ok(Self {
            target_size,
            results: Vec::with_capacity(10),
            opts,
            segment_id_generator,
            current_segment_id,
            writer,

            compression: CompressionType::None,

            #[cfg(feature = "bloom")]
            bloom_policy: BloomConstructionPolicy::default(),
        })
    }

    #[must_use]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self.writer = self.writer.use_compression(compression);
        self
    }

    #[must_use]
    #[cfg(feature = "bloom")]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.writer = self.writer.use_bloom_policy(bloom_policy);
        self
    }

    fn get_next_segment_id(&mut self) -> u64 {
        self.current_segment_id = self
            .segment_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.current_segment_id
    }

    /// Flushes the current writer, stores its metadata, and sets up a new writer for the next segment
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating segment writer");

        let new_segment_id = self.get_next_segment_id();

        // NOTE: Feature-dependent
        #[allow(unused_mut)]
        let mut new_writer = Writer::new(Options {
            segment_id: new_segment_id,
            folder: self.opts.folder.clone(),
            data_block_size: self.opts.data_block_size,
            index_block_size: self.opts.index_block_size,
            prefix_extractor: self.opts.prefix_extractor.clone(),
        })?
        .use_compression(self.compression);

        #[cfg(feature = "bloom")]
        {
            new_writer = new_writer.use_bloom_policy(self.bloom_policy);
        }

        let mut old_writer = std::mem::replace(&mut self.writer, new_writer);

        if let Some(result) = old_writer.finish()? {
            self.results.push(result);
        }

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        self.writer.write(item)?;

        if *self.writer.meta.file_pos >= self.target_size && self.writer.can_rotate() {
            self.rotate()?;
        }

        Ok(())
    }

    /// Finishes the last segment, making sure all data is written durably
    ///
    /// Returns the metadata of created segments
    pub fn finish(mut self) -> crate::Result<Vec<SegmentFileTrailer>> {
        if let Some(last_writer_result) = self.writer.finish()? {
            self.results.push(last_writer_result);
        }

        Ok(self.results)
    }
}

#[cfg(test)]
mod tests {
    use crate::{AbstractTree, Config};
    use test_log::test;

    // NOTE: Tests that versions of the same key stay
    // in the same segment even if it needs to be rotated
    // This avoids segments' key ranges overlapping
    #[test]
    fn segment_multi_writer_same_key_norotate() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = Config::new(&folder).open()?;

        tree.insert("a", "a1".repeat(4_000), 0);
        tree.insert("a", "a2".repeat(4_000), 1);
        tree.insert("a", "a3".repeat(4_000), 2);
        tree.insert("a", "a4".repeat(4_000), 3);
        tree.insert("a", "a5".repeat(4_000), 4);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.segment_count());

        tree.major_compact(1_024, 0)?;
        assert_eq!(1, tree.segment_count());

        Ok(())
    }
}
