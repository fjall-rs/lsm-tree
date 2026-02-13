// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{filter::BloomConstructionPolicy, writer::Writer};
use crate::{
    blob_tree::handle::BlobIndirection, fs::FileSystem, table::writer::LinkedFile,
    value::InternalValue, vlog::BlobFileId, Checksum, CompressionType, HashMap,
    SequenceNumberCounter, TableId, UserKey,
};
use std::path::PathBuf;

/// Like `Writer` but will rotate to a new table, once a table grows larger than `target_size`
///
/// This results in a sorted "run" of tables
pub struct MultiWriter<F: FileSystem> {
    pub(crate) base_path: PathBuf,

    data_block_hash_ratio: f32,

    data_block_size: u32,

    data_block_restart_interval: u8,
    index_block_restart_interval: u8,

    use_partitioned_index: bool,
    use_partitioned_filter: bool,

    /// Target size of tables in bytes
    ///
    /// If a table reaches the target size, a new one is started,
    /// resulting in a sorted "run" of tables
    pub target_size: u64,

    results: Vec<(TableId, Checksum)>,

    table_id_generator: SequenceNumberCounter,

    pub writer: Writer<F>,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,

    bloom_policy: BloomConstructionPolicy,

    current_key: Option<UserKey>,

    linked_blobs: HashMap<BlobFileId, LinkedFile>,

    /// Level the tables are written to
    initial_level: u8,
}

impl<F: FileSystem> MultiWriter<F> {
    /// Sets up a new `MultiWriter` at the given tables folder
    pub fn new(
        base_path: PathBuf,
        table_id_generator: SequenceNumberCounter,
        target_size: u64,
        initial_level: u8,
    ) -> crate::Result<Self> {
        let current_table_id = table_id_generator.next();

        let path = base_path.join(current_table_id.to_string());
        let writer = Writer::<F>::new(path, current_table_id, initial_level)?;

        Ok(Self {
            initial_level,

            base_path,

            data_block_hash_ratio: 0.0,

            data_block_size: 4_096,

            data_block_restart_interval: 16,
            index_block_restart_interval: 1,

            target_size,
            results: Vec::new(),
            table_id_generator,
            writer,

            data_block_compression: CompressionType::None,
            index_block_compression: CompressionType::None,

            use_partitioned_index: false,
            use_partitioned_filter: false,

            bloom_policy: BloomConstructionPolicy::default(),

            current_key: None,

            linked_blobs: HashMap::default(),
        })
    }

    pub fn register_blob(&mut self, indirection: BlobIndirection) {
        self.linked_blobs
            .entry(indirection.vhandle.blob_file_id)
            .and_modify(|entry| {
                entry.bytes += u64::from(indirection.size);
                entry.on_disk_bytes += u64::from(indirection.vhandle.on_disk_size);
                entry.len += 1;
            })
            .or_insert_with(|| LinkedFile {
                blob_file_id: indirection.vhandle.blob_file_id,
                bytes: u64::from(indirection.size),
                on_disk_bytes: u64::from(indirection.vhandle.on_disk_size),
                len: 1,
            });
    }

    #[must_use]
    pub fn use_partitioned_index(mut self) -> Self {
        self.use_partitioned_index = true;
        self.writer = self.writer.use_partitioned_index();
        self
    }

    #[must_use]
    pub fn use_partitioned_filter(mut self) -> Self {
        self.use_partitioned_filter = true;
        self.writer = self.writer.use_partitioned_filter();
        self
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        self.data_block_restart_interval = interval;
        self.writer = self.writer.use_data_block_restart_interval(interval);
        self
    }

    #[must_use]
    pub fn use_index_block_restart_interval(mut self, interval: u8) -> Self {
        self.index_block_restart_interval = interval;
        self.writer = self.writer.use_index_block_restart_interval(interval);
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
    pub fn use_index_block_compression(mut self, compression: CompressionType) -> Self {
        self.index_block_compression = compression;
        self.writer = self.writer.use_index_block_compression(compression);
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.writer = self.writer.use_bloom_policy(bloom_policy);
        self
    }

    /// Flushes the current writer, stores its metadata, and sets up a new writer for the next table
    fn rotate(&mut self) -> crate::Result<()> {
        log::debug!("Rotating table writer");

        let new_table_id = self.table_id_generator.next();
        let path = self.base_path.join(new_table_id.to_string());

        let mut new_writer = Writer::<F>::new(path, new_table_id, self.initial_level)?
            .use_data_block_compression(self.data_block_compression)
            .use_index_block_compression(self.index_block_compression)
            .use_data_block_size(self.data_block_size)
            .use_data_block_restart_interval(self.data_block_restart_interval)
            .use_index_block_restart_interval(self.index_block_restart_interval)
            .use_bloom_policy(self.bloom_policy)
            .use_data_block_hash_ratio(self.data_block_hash_ratio);

        if self.use_partitioned_index {
            new_writer = new_writer.use_partitioned_index();
        }
        if self.use_partitioned_filter {
            new_writer = new_writer.use_partitioned_filter();
        }

        let mut old_writer = std::mem::replace(&mut self.writer, new_writer);

        for linked in self.linked_blobs.values() {
            old_writer.link_blob_file(
                linked.blob_file_id,
                linked.len,
                linked.bytes,
                linked.on_disk_bytes,
            );
        }
        self.linked_blobs.clear();

        if let Some((table_id, checksum)) = old_writer.finish()? {
            self.results.push((table_id, checksum));
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

    /// Finishes the last table, making sure all data is written durably
    ///
    /// Returns the metadata of created tables
    pub fn finish(mut self) -> crate::Result<Vec<(TableId, Checksum)>> {
        for linked in self.linked_blobs.values() {
            self.writer.link_blob_file(
                linked.blob_file_id,
                linked.len,
                linked.bytes,
                linked.on_disk_bytes,
            );
        }

        if let Some((table_id, checksum)) = self.writer.finish()? {
            self.results.push((table_id, checksum));
        }

        Ok(self.results)
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::CompressionPolicy, AbstractTree, Config, SeqNo, SequenceNumberCounter};
    use test_log::test;

    // NOTE: Tests that versions of the same key stay
    // in the same table even if it needs to be rotated
    //
    // This avoids tables' key ranges overlapping
    //
    // http://github.com/fjall-rs/lsm-tree/commit/f46b6fe26a1e90113dc2dbb0342db160a295e616
    #[test]
    fn table_multi_writer_same_key_norotate() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .index_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .open()?;

        tree.insert("a", "a1".repeat(4_000), 0);
        tree.insert("a", "a2".repeat(4_000), 1);
        tree.insert("a", "a3".repeat(4_000), 2);
        tree.insert("a", "a4".repeat(4_000), 3);
        tree.insert("a", "a5".repeat(4_000), 4);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        tree.major_compact(1_024, 0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(1, tree.len(SeqNo::MAX, None)?);

        Ok(())
    }

    // NOTE: Follow-up fix for non-disjoint output
    //
    // https://github.com/fjall-rs/lsm-tree/commit/1609a57c2314420b858d826790ecd1442aa76720
    #[test]
    fn table_multi_writer_same_key_norotate_2() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;

        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .index_block_compression_policy(CompressionPolicy::all(crate::CompressionType::None))
        .open()?;

        tree.insert("a", "a1".repeat(4_000), 0);
        tree.insert("a", "a1".repeat(4_000), 1);
        tree.insert("a", "a1".repeat(4_000), 2);
        tree.insert("b", "a1".repeat(4_000), 0);
        tree.insert("c", "a1".repeat(4_000), 0);
        tree.insert("c", "a1".repeat(4_000), 1);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.table_count());
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        tree.major_compact(1_024, 0)?;
        assert_eq!(3, tree.table_count());
        assert_eq!(3, tree.len(SeqNo::MAX, None)?);

        Ok(())
    }
}
