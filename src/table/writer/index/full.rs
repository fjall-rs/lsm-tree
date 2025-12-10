// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    checksum::ChecksummedWriter,
    table::{
        block::Header as BlockHeader, index_block::KeyedBlockHandle,
        writer::index::BlockIndexWriter, Block, IndexBlock,
    },
    CompressionType,
};
use std::{fs::File, io::BufWriter};

pub struct FullIndexWriter {
    compression: CompressionType,
    block_handles: Vec<KeyedBlockHandle>,
}

impl FullIndexWriter {
    pub fn new() -> Self {
        Self {
            compression: CompressionType::None,
            block_handles: Vec::new(),
        }
    }
}

impl<W: std::io::Write + std::io::Seek> BlockIndexWriter<W> for FullIndexWriter {
    fn use_compression(
        mut self: Box<Self>,
        compression: CompressionType,
    ) -> Box<dyn BlockIndexWriter<W>> {
        self.compression = compression;
        self
    }

    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()> {
        log::trace!(
            "Registering block at {:?} with size {} [end_key={:?}]",
            block_handle.offset(),
            block_handle.size(),
            block_handle.end_key(),
        );

        self.block_handles.push(block_handle);

        Ok(())
    }

    fn finish(
        self: Box<Self>,
        file_writer: &mut sfa::Writer<ChecksummedWriter<BufWriter<File>>>,
    ) -> crate::Result<usize> {
        file_writer.start("tli")?;

        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.block_handles)?;

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

        debug_assert!(bytes_written > 0, "Block index should never be empty");

        log::trace!(
            "Written top level index, with {} pointers ({bytes_written}B)",
            self.block_handles.len(),
        );

        Ok(1)
    }
}
