use super::block_handle::BlockHandle;
use crate::{disk_block::DiskBlock, file::TOP_LEVEL_INDEX_FILE};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

pub struct Writer {
    folder: PathBuf,

    /// Actual data block handles
    block_handles: Vec<BlockHandle>,

    /// Block size
    block_size: u32,

    /// File position
    ///
    /// IMPORTANT: needs to be set after writing data blocks
    /// to correctly track file position of index blocks
    pub file_pos: u64,
}

impl Writer {
    #[must_use]
    pub fn new(folder: PathBuf, block_size: u32) -> Self {
        Self {
            folder,
            block_handles: Vec::with_capacity(1_000),
            file_pos: 0,
            block_size,
        }
    }

    pub fn register_block(&mut self, block_handle: BlockHandle) {
        self.block_handles.push(block_handle);
    }

    fn write_index_block(
        &mut self,
        file_writer: &mut BufWriter<File>,
        index_blocks: Vec<BlockHandle>,
    ) -> crate::Result<BlockHandle> {
        // Prepare block
        let mut block = DiskBlock::<BlockHandle> {
            items: index_blocks.into(),
            crc: 0,
        };

        // Serialize block
        block.crc = DiskBlock::<BlockHandle>::create_crc(&block.items)?;
        let bytes = DiskBlock::to_bytes_compressed(&block);

        // Write to file
        file_writer.write_all(&bytes)?;

        // Expect is fine, because the chunk is not empty
        let first = block.items.first().expect("Chunk should not be empty");

        let bytes_written = bytes.len();

        let block_pos = self.file_pos;

        self.file_pos += bytes_written as u64;

        Ok(BlockHandle {
            start_key: first.start_key.clone(),
            offset: block_pos,
            size: bytes_written as u32,
        })
    }

    fn write_tli(&mut self, handles: Vec<BlockHandle>) -> crate::Result<()> {
        log::trace!("Writing TLI");

        let tli_path = self.folder.join(TOP_LEVEL_INDEX_FILE);
        let index_writer = File::create(&tli_path)?;
        let mut index_writer = BufWriter::new(index_writer);

        // Prepare block
        let mut block = DiskBlock::<BlockHandle> {
            items: handles.into(),
            crc: 0,
        };

        // Serialize block
        block.crc = DiskBlock::<BlockHandle>::create_crc(&block.items)?;
        let bytes = DiskBlock::to_bytes_compressed(&block);

        // Write to file
        index_writer.write_all(&bytes)?;
        index_writer.flush()?;

        log::trace!("Written top level index to {tli_path:?}");

        Ok(())
    }

    pub fn finish(&mut self, file_writer: &mut BufWriter<File>) -> crate::Result<()> {
        log::trace!(
            "Writing {} block handles into index blocks",
            self.block_handles.len()
        );

        let mut index_chunk = Vec::with_capacity(100);

        let mut index_blocks_count = 0;
        let mut index_blocks_chunk_size = 0;
        let mut index_blocks_chunk = vec![];

        for block_handle in std::mem::take(&mut self.block_handles) {
            let block_handle_size =
                (block_handle.start_key.len() + std::mem::size_of::<BlockHandle>()) as u32;

            index_blocks_chunk.push(block_handle);

            index_blocks_chunk_size += block_handle_size;

            if index_blocks_chunk_size >= self.block_size {
                let tli_entry =
                    self.write_index_block(file_writer, std::mem::take(&mut index_blocks_chunk))?;
                index_blocks_chunk_size = 0;

                // Buffer TLI entry
                index_chunk.push(tli_entry);

                index_blocks_count += 1;
            }
        }

        if index_blocks_chunk_size > 0 {
            let tli_entry =
                self.write_index_block(file_writer, std::mem::take(&mut index_blocks_chunk))?;

            // Buffer TLI entry
            index_chunk.push(tli_entry);

            index_blocks_count += 1;
        }

        log::trace!("Written {index_blocks_count} index blocks");

        // Write TLI
        self.write_tli(index_chunk)?;

        Ok(())
    }
}
