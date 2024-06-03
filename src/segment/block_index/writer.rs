use super::{IndexBlock, KeyedBlockHandle};
use crate::{
    segment::{block::header::Header as BlockHeader, meta::CompressionType},
    serde::Serializable,
    value::UserKey,
    SegmentId,
};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

fn pipe_file_into_writer<P: AsRef<Path>, W: Write>(src_path: P, sink: &mut W) -> crate::Result<()> {
    let reader = File::open(src_path)?;
    let mut reader = BufReader::new(reader);

    std::io::copy(&mut reader, sink)?;
    sink.flush()?;

    Ok(())
}

pub struct Writer {
    pub index_block_tmp_file_path: PathBuf,
    file_pos: u64,

    prev_pos: (u64, u64),

    block_writer: Option<BufWriter<File>>,
    block_size: u32,
    compression: CompressionType,
    block_counter: u32,
    block_handles: Vec<KeyedBlockHandle>,
    tli_pointers: Vec<KeyedBlockHandle>,
}

impl Writer {
    pub fn new<P: AsRef<Path>>(
        segment_id: SegmentId,
        folder: P,
        block_size: u32,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        let index_block_tmp_file_path = folder.as_ref().join(format!("tmp_ib{segment_id}"));

        let block_writer = File::create(&index_block_tmp_file_path)?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), block_writer);

        Ok(Self {
            index_block_tmp_file_path,
            file_pos: 0,
            prev_pos: (0, 0),
            block_writer: Some(block_writer),
            block_counter: 0,
            block_size,
            compression,
            block_handles: Vec::with_capacity(1_000),
            tli_pointers: Vec::with_capacity(1_000),
        })
    }

    fn write_block(&mut self) -> crate::Result<()> {
        let mut block_writer = self.block_writer.as_mut().expect("should exist");

        // Write to file
        let (header, data) = IndexBlock::to_bytes_compressed(
            &self.block_handles,
            self.prev_pos.0,
            self.compression,
        )?;

        header.serialize(&mut block_writer)?;
        block_writer.write_all(&data)?;

        let bytes_written = (BlockHeader::serialized_len() + data.len()) as u64;

        // Expect is fine, because the chunk is not empty
        let last = self
            .block_handles
            .last()
            .expect("Chunk should not be empty");

        let index_block_handle = KeyedBlockHandle {
            end_key: last.end_key.clone(),
            offset: self.file_pos,
        };

        self.tli_pointers.push(index_block_handle);

        self.block_counter = 0;
        self.file_pos += bytes_written;

        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += bytes_written;

        self.block_handles.clear();

        Ok(())
    }

    pub fn register_block(&mut self, start_key: UserKey, offset: u64) -> crate::Result<()> {
        let block_handle_size = (start_key.len() + std::mem::size_of::<KeyedBlockHandle>()) as u32;

        let block_handle = KeyedBlockHandle {
            end_key: start_key,
            offset,
        };

        self.block_handles.push(block_handle);

        self.block_counter += block_handle_size;

        if self.block_counter >= self.block_size {
            self.write_block()?;
        }

        Ok(())
    }

    fn write_top_level_index(
        &mut self,
        block_file_writer: &mut BufWriter<File>,
        file_offset: u64,
    ) -> crate::Result<u64> {
        // IMPORTANT: I hate this, but we need to drop the writer
        // so the file is closed
        // so it can be replaced when using Windows
        self.block_writer = None;

        pipe_file_into_writer(&self.index_block_tmp_file_path, block_file_writer)?;
        let tli_ptr = block_file_writer.stream_position()?;

        log::trace!("Concatted index blocks onto blocks file");

        for item in &mut self.tli_pointers {
            item.offset += file_offset;
        }

        // Write to file
        let (header, data) =
            IndexBlock::to_bytes_compressed(&self.tli_pointers, 0, self.compression)?;

        header.serialize(block_file_writer)?;
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
        if self.block_counter > 0 {
            self.write_block()?;
        }

        {
            let block_writer = self.block_writer.as_mut().expect("should exist");
            block_writer.flush()?;
            block_writer.get_mut().sync_all()?;
        }

        let index_block_ptr = block_file_writer.stream_position()?;
        let tli_ptr = self.write_top_level_index(block_file_writer, index_block_ptr)?;

        // TODO: add test to make sure writer is deleting this
        std::fs::remove_file(&self.index_block_tmp_file_path)?;

        Ok(tli_ptr)
    }
}
