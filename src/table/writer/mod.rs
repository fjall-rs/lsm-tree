// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod filter;
mod index;
mod meta;

use super::{
    block::Header as BlockHeader, filter::BloomConstructionPolicy, Block, BlockOffset, DataBlock,
    KeyedBlockHandle,
};
use crate::{
    checksum::{ChecksumType, ChecksummedWriter},
    coding::Encode,
    file::fsync_directory,
    table::{
        writer::{
            filter::{FilterWriter, FullFilterWriter},
            index::FullIndexWriter,
        },
        BlockHandle,
    },
    time::unix_timestamp,
    vlog::BlobFileId,
    Checksum, CompressionType, InternalValue, TableId, UserKey, ValueType,
};
use index::BlockIndexWriter;
use std::{fs::File, io::BufWriter, path::PathBuf};

#[derive(Copy, Clone, PartialEq, Eq, Debug, std::hash::Hash)]
pub struct LinkedFile {
    pub blob_file_id: BlobFileId,
    pub bytes: u64,
    pub on_disk_bytes: u64,
    pub len: usize,
}

/// Serializes and compresses values into blocks and writes them to disk as a table
pub struct Writer {
    /// Table file path
    pub(crate) path: PathBuf,

    table_id: TableId,

    data_block_restart_interval: u8,
    index_block_restart_interval: u8,

    meta_partition_size: u32,

    data_block_size: u32,

    data_block_hash_ratio: f32,

    /// Compression to use for data blocks
    data_block_compression: CompressionType,

    /// Compression to use for data blocks
    index_block_compression: CompressionType,

    /// Buffer to serialize blocks into
    block_buffer: Vec<u8>,

    /// File writer
    #[expect(clippy::struct_field_names)]
    file_writer: sfa::Writer<ChecksummedWriter<BufWriter<File>>>,

    /// Writer of index blocks
    #[expect(clippy::struct_field_names)]
    index_writer: Box<dyn BlockIndexWriter<BufWriter<File>>>,

    /// Writer of filter
    #[expect(clippy::struct_field_names)]
    filter_writer: Box<dyn FilterWriter<BufWriter<File>>>,

    /// Buffer of KVs
    chunk: Vec<InternalValue>,
    chunk_size: usize,

    pub(crate) meta: meta::Metadata,

    /// Stores the previous block position (used for creating back links)
    prev_pos: (BlockOffset, BlockOffset),

    current_key: Option<UserKey>,

    bloom_policy: BloomConstructionPolicy,

    /// Tracks the previously written item to detect weak tombstone/value pairs
    previous_item: Option<(UserKey, ValueType)>,

    linked_blob_files: Vec<LinkedFile>,

    initial_level: u8,
}

impl Writer {
    pub fn new(path: PathBuf, table_id: TableId, initial_level: u8) -> crate::Result<Self> {
        let writer = BufWriter::with_capacity(u16::MAX.into(), File::create_new(&path)?);
        let writer = ChecksummedWriter::new(writer);
        let mut writer = sfa::Writer::from_writer(writer);
        writer.start("data")?;

        Ok(Self {
            initial_level,

            meta: meta::Metadata::default(),

            table_id,

            data_block_restart_interval: 16,
            index_block_restart_interval: 1,

            data_block_hash_ratio: 0.0,

            meta_partition_size: 4_096,

            data_block_size: 4_096,

            data_block_compression: CompressionType::None,
            index_block_compression: CompressionType::None,

            path: std::path::absolute(path)?,

            index_writer: Box::new(FullIndexWriter::new()),
            filter_writer: Box::new(FullFilterWriter::new(BloomConstructionPolicy::default())),

            block_buffer: Vec::new(),
            file_writer: writer,
            chunk: Vec::new(),

            prev_pos: (BlockOffset(0), BlockOffset(0)),

            chunk_size: 0,

            current_key: None,

            bloom_policy: BloomConstructionPolicy::default(),

            previous_item: None,

            linked_blob_files: Vec::new(),
        })
    }

    pub fn link_blob_file(
        &mut self,
        blob_file_id: BlobFileId,
        len: usize,
        bytes: u64,
        on_disk_bytes: u64,
    ) {
        self.linked_blob_files.push(LinkedFile {
            blob_file_id,
            bytes,
            on_disk_bytes,
            len,
        });
    }

    #[must_use]
    pub fn use_partitioned_filter(mut self) -> Self {
        self.filter_writer = Box::new(filter::PartitionedFilterWriter::new(self.bloom_policy))
            .use_tli_compression(self.index_block_compression);
        self
    }

    #[must_use]
    pub fn use_partitioned_index(mut self) -> Self {
        self.index_writer = Box::new(index::PartitionedIndexWriter::new())
            .use_compression(self.index_block_compression);
        self
    }

    #[must_use]
    pub fn use_data_block_restart_interval(mut self, interval: u8) -> Self {
        self.data_block_restart_interval = interval;
        self
    }

    #[must_use]
    pub fn use_index_block_restart_interval(mut self, interval: u8) -> Self {
        self.index_block_restart_interval = interval;
        self
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self
    }

    #[must_use]
    pub fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self
    }

    #[must_use]
    pub fn use_meta_partition_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.meta_partition_size = size;
        self.index_writer = self.index_writer.use_partition_size(size);
        self.filter_writer = self.filter_writer.use_partition_size(size);
        self
    }

    #[must_use]
    pub fn use_data_block_compression(mut self, compression: CompressionType) -> Self {
        self.data_block_compression = compression;
        self
    }

    #[must_use]
    pub fn use_index_block_compression(mut self, compression: CompressionType) -> Self {
        self.index_block_compression = compression;
        self.index_writer = self.index_writer.use_compression(compression);
        self.filter_writer = self.filter_writer.use_tli_compression(compression);
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self.filter_writer = self.filter_writer.set_filter_policy(bloom_policy);
        self
    }

    /// Writes an item.
    ///
    /// # Note
    ///
    /// It's important that the incoming stream of items is correctly
    /// sorted as described by the [`UserKey`], otherwise the block layout will
    /// be non-sense.
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        let value_type = item.key.value_type;
        let seqno = item.key.seqno;
        let user_key = item.key.user_key.clone();
        let value_len = item.value.len();

        if item.is_tombstone() {
            self.meta.tombstone_count += 1;
        }

        if value_type == ValueType::WeakTombstone {
            self.meta.weak_tombstone_count += 1;
        }

        if value_type == ValueType::Value {
            if let Some((prev_key, prev_type)) = &self.previous_item {
                if prev_type == &ValueType::WeakTombstone && prev_key.as_ref() == user_key.as_ref()
                {
                    self.meta.weak_tombstone_reclaimable_count += 1;
                }
            }
        }

        // NOTE: Check if we visit a new key
        if Some(&user_key) != self.current_key.as_ref() {
            self.meta.key_count += 1;
            self.current_key = Some(user_key.clone());

            // IMPORTANT: Do not buffer *every* item's key
            // because there may be multiple versions
            // of the same key

            if self.bloom_policy.is_active() {
                self.filter_writer.register_key(&user_key)?;
            }
        }

        if self.meta.first_key.is_none() {
            self.meta.first_key = Some(user_key.clone());
        }

        self.chunk_size += user_key.len() + value_len;
        self.chunk.push(item);
        self.previous_item = Some((user_key, value_type));

        if self.chunk_size >= self.data_block_size as usize {
            self.spill_block()?;
        }

        self.meta.lowest_seqno = self.meta.lowest_seqno.min(seqno);
        self.meta.highest_seqno = self.meta.highest_seqno.max(seqno);

        Ok(())
    }

    /// Writes a compressed block to disk.
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`.
    ///
    /// Should only be called when the block has items in it.
    pub(crate) fn spill_block(&mut self) -> crate::Result<()> {
        let Some(last) = self.chunk.last() else {
            return Ok(());
        };

        self.block_buffer.clear();

        DataBlock::encode_into(
            &mut self.block_buffer,
            &self.chunk,
            self.data_block_restart_interval,
            self.data_block_hash_ratio,
        )?;

        let header = Block::write_into(
            &mut self.file_writer,
            &self.block_buffer,
            super::block::BlockType::Data,
            self.data_block_compression,
        )?;

        self.meta.uncompressed_size += u64::from(header.uncompressed_length);

        #[expect(
            clippy::cast_possible_truncation,
            reason = "block header is a couple of bytes only, so cast is fine"
        )]
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        self.index_writer
            .register_data_block(KeyedBlockHandle::new(
                last.key.user_key.clone(),
                last.key.seqno,
                BlockHandle::new(self.meta.file_pos, bytes_written),
            ))?;

        // Adjust metadata
        self.meta.file_pos += u64::from(bytes_written);
        self.meta.item_count += self.chunk.len();
        self.meta.data_block_count += 1;

        // Back link stuff
        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += u64::from(bytes_written);

        // Set last key
        self.meta.last_key = Some(
            // NOTE: We are allowed to remove the last item
            // to get ownership of it, because the chunk is cleared after
            // this anyway
            #[expect(clippy::expect_used, reason = "chunk is not empty")]
            self.chunk
                .pop()
                .expect("chunk should not be empty")
                .key
                .user_key,
        );

        // IMPORTANT: Clear chunk after everything else
        self.chunk.clear();
        self.chunk_size = 0;

        Ok(())
    }

    // TODO: split meta writing into new function
    #[expect(clippy::too_many_lines)]
    /// Finishes the table, making sure all data is written durably
    pub fn finish(mut self) -> crate::Result<Option<(TableId, Checksum)>> {
        use std::io::Write;

        self.spill_block()?;

        // No items written! Just delete table file and return nothing
        if self.meta.item_count == 0 {
            std::fs::remove_file(&self.path)?;
            return Ok(None);
        }

        // Write index
        log::trace!("Finishing index writer");
        let index_block_count = self.index_writer.finish(&mut self.file_writer)?;

        // Write filter
        log::trace!("Finishing filter writer");
        let filter_block_count = self.filter_writer.finish(&mut self.file_writer)?;

        if !self.linked_blob_files.is_empty() {
            use byteorder::{WriteBytesExt, LE};

            self.file_writer.start("linked_blob_files")?;

            #[expect(
                clippy::cast_possible_truncation,
                reason = "there are never 4 billion blob files linked to a single table"
            )]
            self.file_writer
                .write_u32::<LE>(self.linked_blob_files.len() as u32)?;

            for file in self.linked_blob_files {
                self.file_writer.write_u64::<LE>(file.blob_file_id)?;
                self.file_writer.write_u64::<LE>(file.len as u64)?;
                self.file_writer.write_u64::<LE>(file.bytes)?;
                self.file_writer.write_u64::<LE>(file.on_disk_bytes)?;
            }
        }

        self.file_writer.start("table_version")?;
        self.file_writer.write_all(&[0x3])?;

        // Write metadata
        self.file_writer.start("meta")?;

        {
            fn meta(key: &str, value: &[u8]) -> InternalValue {
                InternalValue::from_components(key, value, 0, crate::ValueType::Value)
            }

            let meta_items = [
                meta(
                    "block_count#data",
                    &(self.meta.data_block_count as u64).to_le_bytes(),
                ),
                meta(
                    "block_count#filter",
                    &(filter_block_count as u64).to_le_bytes(),
                ),
                meta(
                    "block_count#index",
                    &(index_block_count as u64).to_le_bytes(),
                ),
                meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
                meta(
                    "compression#data",
                    &self.data_block_compression.encode_into_vec(),
                ),
                meta(
                    "compression#index",
                    &self.index_block_compression.encode_into_vec(),
                ),
                meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
                meta("created_at", &unix_timestamp().as_nanos().to_le_bytes()),
                meta(
                    "data_block_hash_ratio",
                    &self.data_block_hash_ratio.to_le_bytes(),
                ),
                meta("file_size", &self.meta.file_pos.to_le_bytes()),
                meta("filter_hash_type", &[u8::from(ChecksumType::Xxh3)]),
                meta("index_keys_have_seqno", &[0x1]),
                meta("initial_level", &self.initial_level.to_le_bytes()),
                meta("item_count", &(self.meta.item_count as u64).to_le_bytes()),
                meta(
                    "key#max",
                    // NOTE: At the beginning we check that we have written at least 1 item, so last_key must exist
                    #[expect(clippy::expect_used)]
                    self.meta.last_key.as_ref().expect("should exist"),
                ),
                meta(
                    "key#min",
                    // NOTE: At the beginning we check that we have written at least 1 item, so first_key must exist
                    #[expect(clippy::expect_used)]
                    self.meta.first_key.as_ref().expect("should exist"),
                ),
                meta("key_count", &(self.meta.key_count as u64).to_le_bytes()),
                meta("prefix_truncation#data", &[1]), // NOTE: currently prefix truncation can not be disabled
                meta("prefix_truncation#index", &[1]), // NOTE: currently prefix truncation can not be disabled
                meta(
                    "restart_interval#data",
                    &self.data_block_restart_interval.to_le_bytes(),
                ),
                meta(
                    "restart_interval#index",
                    &self.index_block_restart_interval.to_le_bytes(),
                ),
                meta("seqno#max", &self.meta.highest_seqno.to_le_bytes()),
                meta("seqno#min", &self.meta.lowest_seqno.to_le_bytes()),
                meta("table_id", &self.table_id.to_le_bytes()),
                meta("table_version", &[3u8]),
                meta(
                    "tombstone_count",
                    &(self.meta.tombstone_count as u64).to_le_bytes(),
                ),
                meta("user_data_size", &self.meta.uncompressed_size.to_le_bytes()),
                meta(
                    "weak_tombstone_count",
                    &(self.meta.weak_tombstone_count as u64).to_le_bytes(),
                ),
                meta(
                    "weak_tombstone_reclaimable",
                    &(self.meta.weak_tombstone_reclaimable_count as u64).to_le_bytes(),
                ),
            ];

            // NOTE: Just to make sure the items are definitely sorted
            #[cfg(debug_assertions)]
            {
                let is_sorted = meta_items.iter().is_sorted_by_key(|kv| &kv.key);
                assert!(is_sorted, "meta items not sorted correctly");
            }

            self.block_buffer.clear();

            // TODO: disable binary index: https://github.com/fjall-rs/lsm-tree/issues/185
            DataBlock::encode_into(&mut self.block_buffer, &meta_items, 1, 0.0)?;

            Block::write_into(
                &mut self.file_writer,
                &self.block_buffer,
                crate::table::block::BlockType::Meta,
                CompressionType::None,
            )?;
        };

        // Write fixed-size trailer
        // and flush & fsync the table file
        let mut checksum = self.file_writer.into_inner()?;
        checksum.inner_mut().get_mut().sync_all()?;
        let checksum = checksum.checksum();

        // IMPORTANT: fsync folder on Unix

        #[expect(
            clippy::expect_used,
            reason = "if there's no parent folder, something has gone horribly wrong"
        )]
        fsync_directory(self.path.parent().expect("should have folder"))?;

        log::debug!(
            "Written {} items in {} blocks into new table file #{}, written {} MiB",
            self.meta.item_count,
            self.meta.data_block_count,
            self.table_id,
            *self.meta.file_pos / 1_024 / 1_024,
        );

        Ok(Some((self.table_id, checksum)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn table_writer_count() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("1");
        let mut writer = Writer::new(path, 1, 0)?;

        assert_eq!(0, writer.meta.key_count);
        assert_eq!(0, writer.chunk_size);

        writer.write(InternalValue::from_components(
            b"a",
            b"a",
            0,
            ValueType::Value,
        ))?;
        assert_eq!(1, writer.meta.key_count);
        assert_eq!(2, writer.chunk_size);

        writer.write(InternalValue::from_components(
            b"b",
            b"b",
            0,
            ValueType::Value,
        ))?;
        assert_eq!(2, writer.meta.key_count);
        assert_eq!(4, writer.chunk_size);

        writer.write(InternalValue::from_components(
            b"c",
            b"c",
            0,
            ValueType::Value,
        ))?;
        assert_eq!(3, writer.meta.key_count);
        assert_eq!(6, writer.chunk_size);

        writer.spill_block()?;
        assert_eq!(0, writer.chunk_size);

        Ok(())
    }
}
