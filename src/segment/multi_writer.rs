// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{filter::BloomConstructionPolicy, writer::Writer};
use crate::{value::InternalValue, CompressionType, SegmentId, UserKey};
use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
};

/// Like `Writer` but will rotate to a new segment, once a segment grows larger than `target_size`
///
/// This results in a sorted "run" of segments
#[allow(clippy::module_name_repetitions)]
pub struct MultiWriter {
    base_path: PathBuf,

    data_block_hash_ratio: f32,
    data_block_size: u32,
    data_block_restart_interval: u8,

    /// Target size of segments in bytes
    ///
    /// If a segment reaches the target size, a new one is started,
    /// resulting in a sorted "run" of segments
    pub target_size: u64,

    results: Vec<SegmentId>,

    segment_id_generator: Arc<AtomicU64>,
    current_segment_id: u64,

    pub writer: Writer,

    pub data_block_compression: CompressionType,

    bloom_policy: BloomConstructionPolicy,

    current_key: Option<UserKey>,
}

impl MultiWriter {
    /// Sets up a new `MultiWriter` at the given segments folder
    pub fn new(
        base_path: PathBuf,
        segment_id_generator: Arc<AtomicU64>,
        target_size: u64,
    ) -> crate::Result<Self> {
        let current_segment_id =
            segment_id_generator.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let path = base_path.join(current_segment_id.to_string());
        let writer = Writer::new(path, current_segment_id)?;

        Ok(Self {
            base_path,

            data_block_hash_ratio: 0.0,
            data_block_size: 4_096,
            data_block_restart_interval: 16,

            target_size,
            results: Vec::with_capacity(10),
            segment_id_generator,
            current_segment_id,
            writer,

            data_block_compression: CompressionType::None,

            bloom_policy: BloomConstructionPolicy::default(),

            current_key: None,
        })
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        self.data_block_restart_interval = interval;
        self.writer = self.writer.use_data_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self.writer = self.writer.use_data_block_hash_ratio(ratio);
        self
    }

    #[must_use]
    pub(crate) fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self.writer = self.writer.use_data_block_size(size);
        self
    }

    #[must_use]
    pub fn use_data_block_compression(mut self, compression: CompressionType) -> Self {
        self.data_block_compression = compression;
        self.writer = self.writer.use_data_block_compression(compression);
        self
    }

    #[must_use]
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
        let path = self.base_path.join(new_segment_id.to_string());

        let new_writer = Writer::new(path, new_segment_id)?
            .use_data_block_compression(self.data_block_compression)
            .use_data_block_size(self.data_block_size)
            .use_bloom_policy(self.bloom_policy)
            .use_data_block_hash_ratio(self.data_block_hash_ratio);

        let old_writer = std::mem::replace(&mut self.writer, new_writer);

        if let Some(segment_id) = old_writer.finish()? {
            self.results.push(segment_id);
        }

        Ok(())
    }

    /// Writes an item
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let is_next_key = self.current_key.as_ref() < Some(&item.key.user_key);

        if is_next_key {
            self.current_key = Some(item.key.user_key.clone());

            if *self.writer.meta.file_pos >= self.target_size {
                self.rotate()?;
            }
        }

        self.writer.write(item)?;

        Ok(())
    }

    /// Finishes the last segment, making sure all data is written durably
    ///
    /// Returns the metadata of created segments
    pub fn finish(mut self) -> crate::Result<Vec<SegmentId>> {
        if let Some(last_writer_result) = self.writer.finish()? {
            self.results.push(last_writer_result);
        }

        Ok(self.results)
    }
}

#[cfg(test)]
mod tests {
    use crate::{AbstractTree, Config, SeqNo};
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
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        tree.major_compact(1_024, 0)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        Ok(())
    }

    #[test]
    fn segment_multi_writer_same_key_norotate_2() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = Config::new(&folder).open()?;

        tree.insert("a", "a1".repeat(4_000), 0);
        tree.insert("a", "a1".repeat(4_000), 1);
        tree.insert("a", "a1".repeat(4_000), 2);
        tree.insert("b", "a1".repeat(4_000), 0);
        tree.insert("c", "a1".repeat(4_000), 0);
        tree.insert("c", "a1".repeat(4_000), 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.segment_count());
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        tree.major_compact(1_024, 0)?;
        assert_eq!(3, tree.segment_count());
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        Ok(())
    }
}
