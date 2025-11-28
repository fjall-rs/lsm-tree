// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::FilterWriter;
use crate::{
    checksum::ChecksummedWriter,
    config::BloomConstructionPolicy,
    table::{
        block::Header as BlockHeader, filter::standard_bloom::Builder, Block, BlockOffset,
        IndexBlock, KeyedBlockHandle,
    },
    CompressionType, UserKey,
};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
};

pub struct PartitionedFilterWriter {
    final_filter_buffer: Vec<u8>,

    tli_handles: Vec<KeyedBlockHandle>,

    /// Key hashes for AMQ filter
    pub bloom_hash_buffer: Vec<u64>,
    approx_filter_size: usize,

    partition_size: u32,

    bloom_policy: BloomConstructionPolicy,

    relative_file_pos: u64,

    last_key: Option<UserKey>,

    compression: CompressionType,
}

impl PartitionedFilterWriter {
    pub fn new(bloom_policy: BloomConstructionPolicy) -> Self {
        Self {
            final_filter_buffer: Vec::new(),

            bloom_hash_buffer: Vec::new(),
            approx_filter_size: 0,

            tli_handles: Vec::new(),
            partition_size: 4_096,
            bloom_policy,

            relative_file_pos: 0,

            last_key: None,

            compression: CompressionType::None,
        }
    }

    fn spill_filter_partition(&mut self, key: &UserKey) -> crate::Result<()> {
        let filter_bytes = {
            let mut builder = self.bloom_policy.init(self.bloom_hash_buffer.len());

            for hash in self.bloom_hash_buffer.drain(..) {
                builder.set_with_hash(hash);
            }

            builder.build()
        };

        let header = Block::write_into(
            &mut self.final_filter_buffer,
            &filter_bytes,
            crate::table::block::BlockType::Filter,
            CompressionType::None,
        )?;

        let bytes_written = (header.data_length as usize + BlockHeader::serialized_len()) as u32;

        self.tli_handles.push(KeyedBlockHandle::new(
            key.clone(),
            BlockOffset(self.relative_file_pos),
            bytes_written,
        ));

        self.bloom_hash_buffer.clear();
        self.approx_filter_size = 0;
        self.relative_file_pos += u64::from(bytes_written);

        log::trace!(
            "Built Bloom filter partition ({} B) with end_key={key:?}",
            filter_bytes.len(),
        );

        Ok(())
    }

    fn write_top_level_index(
        &mut self,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
        index_base_offset: BlockOffset,
    ) -> crate::Result<()> {
        file_writer.start("filter_tli")?;

        for item in &mut self.tli_handles {
            item.shift(index_base_offset);
        }

        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.tli_handles)?;

        let header = Block::write_into(
            file_writer,
            &bytes,
            crate::table::block::BlockType::Index,
            self.compression,
        )?;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "blocks never even approach u32 size"
        )]
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        debug_assert!(bytes_written > 0, "Top level index should never be empty");

        log::trace!(
            "Written filter top level index, with {} pointers ({bytes_written} bytes)",
            self.tli_handles.len(),
        );

        Ok(())
    }
}

impl<W: std::io::Write + std::io::Seek> FilterWriter<W> for PartitionedFilterWriter {
    fn use_tli_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn FilterWriter<W>> {
        self.compression = compression;
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

        self.approx_filter_size +=
            self.bloom_policy
                .estimated_key_bits(self.bloom_hash_buffer.len()) as usize;

        self.last_key = Some(key.clone());

        if self.approx_filter_size >= self.partition_size as usize {
            self.spill_filter_partition(key)?;
        }

        Ok(())
    }

    fn finish(
        mut self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
    ) -> crate::Result<usize> {
        if self.bloom_hash_buffer.is_empty() {
            log::trace!("Filter writer has no buffered hashes - not building filter");
            Ok(0)
        } else {
            let last_key = self.last_key.take().expect("last key should exist");
            self.spill_filter_partition(&last_key)?;

            let index_base_offset = BlockOffset(file_writer.get_mut().stream_position()?);

            file_writer.start("filter")?;
            file_writer.write_all(&self.final_filter_buffer)?;
            log::trace!("Concatted filter partitions onto blocks file");

            let block_count = self.tli_handles.len();

            self.write_top_level_index(file_writer, index_base_offset)?;

            Ok(block_count)
        }
    }
}
