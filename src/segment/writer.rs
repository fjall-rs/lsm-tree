use super::{trailer::SegmentFileTrailer, value_block::ValueBlock};
use crate::{
    file::fsync_directory,
    segment::{
        block::header::Header as BlockHeader, block_index::writer::Writer as IndexWriter,
        meta::Metadata,
    },
    serde::Serializable,
    value::{SeqNo, UserKey},
    SegmentId, Value,
};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::PathBuf,
};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

/// Serializes and compresses values into blocks and writes them to disk
///
/// Also takes care of creating the block index
pub struct Writer {
    pub opts: Options,

    segment_file_path: PathBuf,

    block_writer: BufWriter<File>,
    index_writer: IndexWriter,
    chunk: Vec<Value>,

    pub block_count: usize,
    pub item_count: usize,
    pub file_pos: u64,

    /// Only takes user data into account
    pub uncompressed_size: u64,

    pub first_key: Option<UserKey>,
    pub last_key: Option<UserKey>,
    pub tombstone_count: usize,
    pub chunk_size: usize,

    pub lowest_seqno: SeqNo,
    pub highest_seqno: SeqNo,

    pub key_count: usize,
    current_key: Option<UserKey>,

    /// Hashes for bloom filter
    ///
    /// using enhanced double hashing, so we got two u64s
    #[cfg(feature = "bloom")]
    bloom_hash_buffer: Vec<(u64, u64)>,
}

pub struct Options {
    pub folder: PathBuf,
    pub evict_tombstones: bool,
    pub block_size: u32,

    pub segment_id: SegmentId,

    #[cfg(feature = "bloom")]
    pub bloom_fp_rate: f32,
}

#[derive(Debug)]
pub struct FileOffsets {
    pub(crate) index_block_ptr: u64,
    pub(crate) tli_ptr: u64,
    pub(crate) bloom_ptr: u64,
    pub(crate) range_tombstone_ptr: u64,

    #[allow(unused)]
    pub(crate) metadata_ptr: u64,
}

impl Writer {
    /// Sets up a new `Writer` at the given folder
    pub fn new(opts: Options) -> crate::Result<Self> {
        let segment_file_path = opts.folder.join(opts.segment_id.to_string());

        let block_writer = File::create(&segment_file_path)?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), block_writer);

        let index_writer = IndexWriter::new(opts.segment_id, &opts.folder, opts.block_size)?;

        let chunk = Vec::with_capacity(10_000);

        Ok(Self {
            opts,

            segment_file_path,

            block_writer,
            index_writer,
            chunk,

            block_count: 0,
            item_count: 0,
            file_pos: 0,
            uncompressed_size: 0,

            first_key: None,
            last_key: None,
            chunk_size: 0,
            tombstone_count: 0,

            lowest_seqno: SeqNo::MAX,
            highest_seqno: 0,

            current_key: None,
            key_count: 0,

            #[cfg(feature = "bloom")]
            bloom_hash_buffer: Vec::with_capacity(10_000),
        })
    }

    /// Writes a compressed block to disk.
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`.
    pub(crate) fn write_block(&mut self) -> crate::Result<()> {
        debug_assert!(!self.chunk.is_empty());

        let uncompressed_chunk_size = self
            .chunk
            .iter()
            .map(|item| item.size() as u64)
            .sum::<u64>();

        self.uncompressed_size += uncompressed_chunk_size;

        // Write to file
        let (header, data) = ValueBlock::to_bytes_compressed(&self.chunk)?;

        header.serialize(&mut self.block_writer)?;
        self.block_writer.write_all(&data)?;

        let bytes_written = (BlockHeader::serialized_len() + data.len()) as u64;

        // NOTE: Expect is fine, because the chunk is not empty
        let last = self.chunk.last().expect("Chunk should not be empty");

        self.index_writer
            .register_block(last.key.clone(), self.file_pos)?;

        // Adjust metadata
        self.file_pos += bytes_written;
        self.item_count += self.chunk.len();
        self.block_count += 1;

        self.chunk.clear();

        Ok(())
    }

    /// Writes an item.
    ///
    /// # Note
    ///
    /// It's important that the incoming stream of items is correctly
    /// sorted as described by the [`UserKey`], otherwise the block layout will
    /// be non-sense.
    pub fn write(&mut self, item: Value) -> crate::Result<()> {
        if item.is_tombstone() {
            if self.opts.evict_tombstones {
                return Ok(());
            }

            self.tombstone_count += 1;
        }

        if Some(&item.key) != self.current_key.as_ref() {
            self.key_count += 1;
            self.current_key = Some(item.key.clone());

            // IMPORTANT: Do not buffer *every* item's key
            // because there may be multiple versions
            // of the same key
            #[cfg(feature = "bloom")]
            self.bloom_hash_buffer
                .push(BloomFilter::get_hash(&item.key));
        }

        let item_key = item.key.clone();
        let seqno = item.seqno;

        self.chunk_size += item.size();
        self.chunk.push(item);

        if self.chunk_size >= self.opts.block_size as usize {
            self.write_block()?;
            self.chunk_size = 0;
        }

        if self.first_key.is_none() {
            self.first_key = Some(item_key.clone());
        }
        self.last_key = Some(item_key);

        if self.lowest_seqno > seqno {
            self.lowest_seqno = seqno;
        }

        if self.highest_seqno < seqno {
            self.highest_seqno = seqno;
        }

        Ok(())
    }

    // TODO: should take mut self to avoid double finish

    /// Finishes the segment, making sure all data is written durably
    pub fn finish(&mut self) -> crate::Result<Option<SegmentFileTrailer>> {
        if !self.chunk.is_empty() {
            self.write_block()?;
        }

        // No items written! Just delete segment folder and return nothing
        if self.item_count == 0 {
            std::fs::remove_file(&self.segment_file_path)?;

            if let Err(e) = std::fs::remove_file(&self.index_writer.index_block_tmp_file_path) {
                debug_assert!(false, "should not happen");
                log::warn!("Failed to delete tmp file: {e:?}");
            };

            return Ok(None);
        }

        let index_block_ptr = self.block_writer.stream_position()?;
        log::trace!("index_block_ptr={index_block_ptr}");

        // Append index blocks to file
        let tli_ptr = self.index_writer.finish(&mut self.block_writer)?;
        log::trace!("tli_ptr={tli_ptr}");

        // Write bloom filter
        #[cfg(feature = "bloom")]
        let bloom_ptr = {
            let bloom_ptr = self.block_writer.stream_position()?;

            let n = self.bloom_hash_buffer.len();
            log::trace!("Writing bloom filter with {n} hashes");

            let mut filter = BloomFilter::with_fp_rate(n, self.opts.bloom_fp_rate);

            for hash in std::mem::take(&mut self.bloom_hash_buffer) {
                filter.set_with_hash(hash);
            }

            // NOTE: BloomFilter::write_to_file fsyncs internally
            // filter.write_to_file(self.opts.folder.join(BLOOM_FILTER_FILE))?;
            filter.serialize(&mut self.block_writer)?;

            bloom_ptr
        };

        #[cfg(not(feature = "bloom"))]
        let bloom_ptr = 0;
        log::trace!("bloom_ptr={bloom_ptr}");

        // TODO: Write range tombstones
        let range_tombstone_ptr = 0;
        log::trace!("range_tombstone_ptr={range_tombstone_ptr}");

        // Write metadata
        let metadata_ptr = self.block_writer.stream_position()?;

        let metadata = Metadata::from_writer(self.opts.segment_id, self)?;
        metadata.serialize(&mut self.block_writer)?;

        // Bundle all the file offsets
        let offsets = FileOffsets {
            index_block_ptr,
            tli_ptr,
            bloom_ptr,
            range_tombstone_ptr,
            metadata_ptr,
        };

        // Write trailer
        let trailer = SegmentFileTrailer { metadata, offsets };
        trailer.serialize(&mut self.block_writer)?;

        // Finally, flush & fsync the blocks file
        self.block_writer.flush()?;
        self.block_writer.get_mut().sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&self.opts.folder)?;

        log::debug!(
            "Written {} items in {} blocks into new segment file, written {} MB of data blocks",
            self.item_count,
            self.block_count,
            self.file_pos / 1_024 / 1_024
        );

        Ok(Some(trailer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor_table::FileDescriptorTable;
    use crate::value::ValueType;
    use crate::{
        block_cache::BlockCache,
        segment::{block_index::BlockIndex, reader::Reader},
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn segment_writer_write_read() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100;

        let folder = tempfile::tempdir()?.into_path();

        let segment_id = 532;

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            segment_id,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            Value::new(
                i.to_be_bytes(),
                nanoid::nanoid!().as_bytes(),
                0,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        let trailer = writer.finish()?.expect("should exist");

        assert_eq!(ITEM_COUNT, trailer.metadata.item_count);
        assert_eq!(ITEM_COUNT, trailer.metadata.key_count);

        let segment_file_path = folder.join(segment_id.to_string());

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, segment_id).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, segment_id).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);
        let iter = Reader::new(
            table,
            (0, segment_id).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
        );

        assert_eq!(ITEM_COUNT, iter.count() as u64);

        Ok(())
    }

    #[test]
    fn segment_writer_write_read_mvcc() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 1_000;
        const VERSION_COUNT: u64 = 5;

        let folder = tempfile::tempdir()?.into_path();

        let segment_id = 532;

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            segment_id,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        for key in 0u64..ITEM_COUNT {
            for seqno in (0..VERSION_COUNT).rev() {
                let value = Value::new(
                    key.to_be_bytes(),
                    nanoid::nanoid!().as_bytes(),
                    seqno,
                    ValueType::Value,
                );

                writer.write(value)?;
            }
        }

        let trailer = writer.finish()?.expect("should exist");

        assert_eq!(ITEM_COUNT * VERSION_COUNT, trailer.metadata.item_count);
        assert_eq!(ITEM_COUNT, trailer.metadata.key_count);

        let segment_file_path = folder.join(segment_id.to_string());

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, segment_id).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, segment_id).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

        let iter = Reader::new(
            table,
            (0, segment_id).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
        );

        assert_eq!(ITEM_COUNT * VERSION_COUNT, iter.count() as u64);

        Ok(())
    }
}
