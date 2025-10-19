use std::io::{Seek, Write};

use crate::{
    segment::{
        block::Header as BlockHeader, index_block::KeyedBlockHandle,
        writer::index::BlockIndexWriter, Block, BlockOffset, IndexBlock,
    },
    CompressionType,
};

pub struct PartitionedIndexWriter {
    relative_file_pos: u64,

    compression: CompressionType,

    tli_handles: Vec<KeyedBlockHandle>,
    data_block_handles: Vec<KeyedBlockHandle>,

    buffer_size: u64,
    block_size: u64,

    index_block_count: usize,

    block_buffer: Vec<u8>,

    final_write_buffer: Vec<u8>,
}

impl PartitionedIndexWriter {
    pub fn new() -> Self {
        Self {
            relative_file_pos: 0,
            buffer_size: 0,
            index_block_count: 0,

            block_size: 4_096,
            compression: CompressionType::None,

            tli_handles: Vec::new(),
            data_block_handles: Vec::new(),
            block_buffer: Vec::with_capacity(4_096),

            final_write_buffer: Vec::new(),
        }
    }

    fn cut_index_block(&mut self) -> crate::Result<()> {
        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.data_block_handles)?;

        let header = Block::write_into(
            &mut self.block_buffer,
            &bytes,
            crate::segment::block::BlockType::Index,
            self.compression,
        )?;

        // NOTE: We know that blocks never even approach u32 size
        #[allow(clippy::cast_possible_truncation)]
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        // NOTE: Expect is fine, because the chunk is not empty
        //
        // Also, we are allowed to remove the last item
        // to get ownership of it, because the chunk is cleared after
        // this anyway
        #[allow(clippy::expect_used)]
        let last = self
            .data_block_handles
            .pop()
            .expect("Chunk should not be empty");

        let index_block_handle = KeyedBlockHandle::new(
            last.end_key().clone(),
            BlockOffset(self.relative_file_pos),
            bytes_written,
        );

        self.tli_handles.push(index_block_handle);
        self.final_write_buffer.append(&mut self.block_buffer);

        // Adjust metadata
        self.index_block_count += 1;
        self.relative_file_pos += u64::from(bytes_written);

        // IMPORTANT: Clear buffer after everything else
        self.data_block_handles.clear();
        self.buffer_size = 0;

        Ok(())
    }

    fn write_top_level_index(
        &mut self,
        file_writer: &mut sfa::Writer,
        index_base_offset: BlockOffset,
    ) -> crate::Result<()> {
        file_writer.start("tli")?;

        for item in &mut self.tli_handles {
            item.shift(index_base_offset);
        }

        let mut bytes = vec![];
        IndexBlock::encode_into(&mut bytes, &self.tli_handles)?;

        let header = Block::write_into(
            file_writer,
            &bytes,
            crate::segment::block::BlockType::Index,
            self.compression,
        )?;

        // NOTE: We know that blocks never even approach u32 size
        #[allow(clippy::cast_possible_truncation)]
        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        debug_assert!(bytes_written > 0, "Top level index should never be empty");

        log::trace!(
            "Written top level index, with {} pointers ({bytes_written} bytes)",
            self.tli_handles.len(),
        );

        Ok(())
    }
}

impl<W: std::io::Write + std::io::Seek> BlockIndexWriter<W> for PartitionedIndexWriter {
    fn len(&self) -> usize {
        self.index_block_count + 1 // 1 = TLI
    }

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

        // NOTE: Truncation is OK, because a key is bound by 65535 bytes, so can never exceed u32s
        #[allow(clippy::cast_possible_truncation)]
        let block_handle_size =
            (block_handle.end_key().len() + std::mem::size_of::<KeyedBlockHandle>()) as u32;

        self.buffer_size += u64::from(block_handle_size);

        self.data_block_handles.push(block_handle);

        if self.buffer_size >= self.block_size {
            self.cut_index_block()?;
        }

        Ok(())
    }

    fn finish(&mut self, file_writer: &mut sfa::Writer) -> crate::Result<()> {
        if self.buffer_size > 0 {
            self.cut_index_block()?;
        }

        let index_base_offset = BlockOffset(file_writer.get_mut().stream_position()?);

        file_writer.start("index")?;
        file_writer.write_all(&self.final_write_buffer)?;
        log::trace!("Concatted index blocks onto blocks file");

        self.write_top_level_index(file_writer, index_base_offset)?;

        Ok(())
    }
}
