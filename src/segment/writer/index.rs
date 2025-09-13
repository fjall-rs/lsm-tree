// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    segment::{
        block::Header as BlockHeader,
        index_block::{BlockHandle, KeyedBlockHandle},
        Block, BlockOffset, IndexBlock,
    },
    CompressionType,
};

pub trait BlockIndexWriter<W: std::io::Write + std::io::Seek> {
    /// Registers a data block in the block index.
    fn register_data_block(&mut self, block_handle: KeyedBlockHandle) -> crate::Result<()>;

    /// Writes the block index to a file.
    ///
    /// Returns the (optional) index blocks handle and the TLI handle.
    fn finish(
        &mut self,
        block_file_writer: &mut W,
    ) -> crate::Result<(BlockHandle, Option<BlockHandle>)>;

    fn use_compression(self, compression: CompressionType) -> Self
    where
        Self: Sized;

    fn len(&self) -> usize;
}

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
    fn len(&self) -> usize {
        1
    }

    fn use_compression(mut self, compression: CompressionType) -> Self {
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
        &mut self,
        block_file_writer: &mut W,
    ) -> crate::Result<(BlockHandle, Option<BlockHandle>)> {
        let tli_ptr = BlockOffset(block_file_writer.stream_position()?);

        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.block_handles)?;

        let header = Block::write_into(
            block_file_writer,
            &bytes,
            crate::segment::block::BlockType::Index,
            self.compression,
        )?;

        // NOTE: We know that blocks never even approach u32 size
        #[allow(clippy::cast_possible_truncation)]
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        log::trace!(
            "Written top level index, with {} pointers ({bytes_written}B)",
            self.block_handles.len(),
        );

        Ok((BlockHandle::new(tli_ptr, bytes_written), None))
    }
}

// TODO: we need 2 index writers (enum dispatch or Box<dyn> then)
// TODO: -> FullIndexWriter
// TODO: -> PartitionedIndexWriter
//
// FullIndexWriter puts all block handles into the TLI, and sets the index blocks handle to NULL
// PartitionedIndexWriter works as Writer does currently
//
// That way, when index_blocks_handle == 0, TLI is a dense index

/* pub struct Writer {
    file_pos: BlockOffset,

    prev_pos: (BlockOffset, BlockOffset),

    write_buffer: Vec<u8>,

    block_size: u32,
    compression: CompressionType,

    buffer_size: u32,

    block_handles: Vec<KeyedBlockHandle>,
    tli_pointers: Vec<KeyedBlockHandle>,

    pub block_count: usize,
}

impl Writer {
    pub fn new(block_size: u32) -> Self {
        Self {
            file_pos: BlockOffset(0),
            prev_pos: (BlockOffset(0), BlockOffset(0)),
            write_buffer: Vec::with_capacity(u16::MAX.into()),
            buffer_size: 0,
            block_size,
            compression: CompressionType::None,
            block_handles: Vec::new(),
            tli_pointers: Vec::new(),
            block_count: 0,
        }
    }

    #[must_use]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    fn write_block(&mut self) -> crate::Result<()> {
        let bytes =
            IndexBlock::encode_items(&self.block_handles, 1 /* TODO: hard coded for now */)?;

        // TODO: prev block offset
        let _header = Block::to_writer(&mut self.write_buffer, &bytes, self.compression)?;

        let bytes_written = (BlockHeader::serialized_len() + bytes.len()) as u32;

        // NOTE: Expect is fine, because the chunk is not empty
        //
        // Also, we are allowed to remove the last item
        // to get ownership of it, because the chunk is cleared after
        // this anyway
        #[allow(clippy::expect_used)]
        let last = self.block_handles.pop().expect("Chunk should not be empty");

        let index_block_handle =
            KeyedBlockHandle::new(last.into_end_key(), self.file_pos, bytes_written);

        self.tli_pointers.push(index_block_handle);

        // Adjust metadata
        self.file_pos += bytes_written as u64;
        self.block_count += 1;

        // Back link stuff
        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += bytes_written as u64;

        // IMPORTANT: Clear buffer after everything else
        self.block_handles.clear();
        self.buffer_size = 0;

        Ok(())
    }

    pub fn register_block(
        &mut self,
        end_key: UserKey,
        offset: BlockOffset,
        size: u32,
    ) -> crate::Result<()> {
        log::trace!(
            "Registering block at 0x{:X?} with size {size} [end_key={:?}]",
            *offset,
            end_key,
        );

        // NOTE: Truncation is OK, because a key is bound by 65535 bytes, so can never exceed u32s
        #[allow(clippy::cast_possible_truncation)]
        let block_handle_size = (end_key.len() + std::mem::size_of::<KeyedBlockHandle>()) as u32;

        let block_handle = KeyedBlockHandle::new(end_key, offset, size);

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
        file_offset: BlockOffset,
    ) -> crate::Result<BlockHandle> {
        block_file_writer.write_all(&self.write_buffer)?;
        log::trace!("Wrote index blocks into segment file");

        let tli_ptr = BlockOffset(block_file_writer.stream_position()?);

        for item in &mut self.tli_pointers {
            item.shift(file_offset);
        }

        let bytes =
            IndexBlock::encode_items(&self.tli_pointers, 1 /* TODO: hard coded for now */)?;

        let _header = Block::to_writer(block_file_writer, &bytes, self.compression)?;

        // NOTE: We know that blocks never even approach u32 size
        #[allow(clippy::cast_possible_truncation)]
        let bytes_written = (BlockHeader::serialized_len() + bytes.len()) as u32;

        log::trace!(
            "Written top level index, with {} pointers ({} bytes)",
            self.tli_pointers.len(),
            bytes_written,
        );

        Ok(BlockHandle::new(tli_ptr, bytes_written))
    }

    /// Returns the offset in the file to TLI
    pub fn finish(
        &mut self,
        block_file_writer: &mut BufWriter<File>,
    ) -> crate::Result<BlockHandle> {
        if self.buffer_size > 0 {
            self.write_block()?;
        }

        let index_block_ptr = BlockOffset(block_file_writer.stream_position()?);
        let tli_handle = self.write_top_level_index(block_file_writer, index_block_ptr)?;

        Ok(tli_handle)
    }
} */
