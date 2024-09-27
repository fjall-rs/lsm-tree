// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{IndexBlock, KeyedBlockHandle};
use crate::{
    coding::Encode,
    segment::{block::header::Header as BlockHeader, meta::CompressionType},
    value::UserKey,
};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
};

pub struct Writer {
    file_pos: u64,

    prev_pos: (u64, u64),

    write_buffer: Vec<u8>,

    block_size: u32,
    compression: CompressionType,

    buffer_size: u32,

    block_handles: Vec<KeyedBlockHandle>,
    tli_pointers: Vec<KeyedBlockHandle>,

    pub block_count: usize,
}

impl Writer {
    pub fn new(block_size: u32) -> crate::Result<Self> {
        Ok(Self {
            file_pos: 0,
            prev_pos: (0, 0),
            write_buffer: Vec::with_capacity(block_size as usize),
            buffer_size: 0,
            block_size,
            compression: CompressionType::None,
            block_handles: Vec::with_capacity(1_000),
            tli_pointers: Vec::with_capacity(1_000),
            block_count: 0,
        })
    }

    #[must_use]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    fn write_block(&mut self) -> crate::Result<()> {
        // Write to file
        let (header, data) = IndexBlock::to_bytes_compressed(
            &self.block_handles,
            self.prev_pos.0,
            self.compression,
        )?;

        header.encode_into(&mut self.write_buffer)?;
        self.write_buffer.write_all(&data)?;

        let bytes_written = (BlockHeader::serialized_len() + data.len()) as u64;

        // NOTE: Expect is fine, because the chunk is not empty
        #[allow(clippy::expect_used)]
        let last = self
            .block_handles
            .last()
            .expect("Chunk should not be empty");

        let index_block_handle = KeyedBlockHandle {
            end_key: last.end_key.clone(),
            offset: self.file_pos,
        };

        self.tli_pointers.push(index_block_handle);

        self.buffer_size = 0;
        self.file_pos += bytes_written;

        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += bytes_written;

        self.block_count += 1;

        self.block_handles.clear();

        Ok(())
    }

    pub fn register_block(&mut self, end_key: UserKey, offset: u64) -> crate::Result<()> {
        // NOTE: Truncation is OK, because a key is bound by 65535 bytes, so can never exceed u32s
        #[allow(clippy::cast_possible_truncation)]
        let block_handle_size = (end_key.len() + std::mem::size_of::<KeyedBlockHandle>()) as u32;

        let block_handle = KeyedBlockHandle { end_key, offset };

        self.block_handles.push(block_handle);

        self.buffer_size += block_handle_size;

        if self.buffer_size >= self.block_size {
            self.write_block()?;
        }

        Ok(())
    }

    fn write_top_level_index(
        &mut self,
        block_file_writer: &mut BufWriter<File>,
        file_offset: u64,
    ) -> crate::Result<u64> {
        block_file_writer.write_all(&self.write_buffer)?;
        let tli_ptr = block_file_writer.stream_position()?;

        log::trace!("Concatted index blocks onto blocks file");

        for item in &mut self.tli_pointers {
            item.offset += file_offset;
        }

        // Write to file
        let (header, data) =
            IndexBlock::to_bytes_compressed(&self.tli_pointers, 0, self.compression)?;

        header.encode_into(block_file_writer)?;
        block_file_writer.write_all(&data)?;

        let bytes_written = BlockHeader::serialized_len() + data.len();

        block_file_writer.flush()?;
        block_file_writer.get_mut().sync_all()?;

        log::trace!(
            "Written top level index, with {} pointers ({} bytes)",
            self.tli_pointers.len(),
            bytes_written,
        );

        Ok(tli_ptr)
    }

    /// Returns the offset in the file to TLI
    pub fn finish(&mut self, block_file_writer: &mut BufWriter<File>) -> crate::Result<u64> {
        if self.buffer_size > 0 {
            self.write_block()?;
        }

        let index_block_ptr = block_file_writer.stream_position()?;
        let tli_ptr = self.write_top_level_index(block_file_writer, index_block_ptr)?;

        Ok(tli_ptr)
    }
}
