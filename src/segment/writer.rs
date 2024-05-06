use super::block::ValueBlock;
use crate::{
    file::{fsync_directory, BLOCKS_FILE},
    segment::block_index::writer::Writer as IndexWriter,
    value::{SeqNo, UserKey},
    Value,
};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

#[cfg(feature = "bloom")]
use crate::file::BLOOM_FILTER_FILE;

/// Serializes and compresses values into blocks and writes them to disk
///
/// Also takes care of creating the block index
pub struct Writer {
    pub opts: Options,

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

    #[cfg(feature = "bloom")]
    pub bloom_fp_rate: f32,
}

impl Writer {
    /// Sets up a new `Writer` at the given folder
    pub fn new(opts: Options) -> crate::Result<Self> {
        std::fs::create_dir_all(&opts.folder)?;

        let block_writer = File::create(opts.folder.join(BLOCKS_FILE))?;
        let block_writer = BufWriter::with_capacity(512_000, block_writer);

        let index_writer = IndexWriter::new(&opts.folder, opts.block_size)?;

        let chunk = Vec::with_capacity(10_000);

        Ok(Self {
            opts,

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

    /// Writes a compressed block to disk
    ///
    /// This is triggered when a `Writer::write` causes the buffer to grow to the configured `block_size`
    fn write_block(&mut self) -> crate::Result<()> {
        debug_assert!(!self.chunk.is_empty());

        let uncompressed_chunk_size = self
            .chunk
            .iter()
            .map(|item| item.size() as u64)
            .sum::<u64>();

        self.uncompressed_size += uncompressed_chunk_size;

        // Prepare block
        let mut block = ValueBlock {
            items: std::mem::replace(&mut self.chunk, Vec::with_capacity(10_000))
                .into_boxed_slice(),
            crc: 0,
        };

        // Serialize block
        block.crc = ValueBlock::create_crc(&block.items)?;
        let bytes = ValueBlock::to_bytes_compressed(&block);

        // Write to file
        self.block_writer.write_all(&bytes)?;

        // NOTE: Blocks are never bigger than 4 GB anyway,
        // so it's fine to just truncate it
        #[allow(clippy::cast_possible_truncation)]
        let bytes_written = bytes.len() as u32;

        // NOTE: Expect is fine, because the chunk is not empty
        let first = block.items.first().expect("Chunk should not be empty");

        self.index_writer
            .register_block(first.key.clone(), self.file_pos, bytes_written)?;

        // Adjust metadata
        self.file_pos += u64::from(bytes_written);
        self.item_count += block.items.len();
        self.block_count += 1;

        Ok(())
    }

    /// Writes an item
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

    /// Finishes the segment, making sure all data is written durably
    pub fn finish(&mut self) -> crate::Result<()> {
        if !self.chunk.is_empty() {
            self.write_block()?;
        }

        // No items written! Just delete segment folder and return nothing
        if self.item_count == 0 {
            log::debug!(
                "Deleting empty segment folder ({}) because no items were written",
                self.opts.folder.display()
            );
            std::fs::remove_dir_all(&self.opts.folder)?;
            return Ok(());
        }

        // First, flush all data blocks
        self.block_writer.flush()?;

        // Append index blocks to file
        self.index_writer.finish(self.file_pos)?;

        // Then fsync the blocks file
        self.block_writer.get_mut().sync_all()?;

        // NOTE: BloomFilter::write_to_file fsyncs internally
        #[cfg(feature = "bloom")]
        {
            let n = self.bloom_hash_buffer.len();
            log::debug!("Writing bloom filter with {n} hashes");

            let mut filter = BloomFilter::with_fp_rate(n, self.opts.bloom_fp_rate);

            for hash in std::mem::take(&mut self.bloom_hash_buffer) {
                filter.set_with_hash(hash);
            }

            filter.write_to_file(self.opts.folder.join(BLOOM_FILTER_FILE))?;
        }

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&self.opts.folder)?;

        log::debug!(
            "Written {} items in {} blocks into new segment file, written {} MB",
            self.item_count,
            self.block_count,
            self.file_pos / 1_024 / 1_024
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::descriptor_table::FileDescriptorTable;
    use crate::value::ValueType;
    use crate::{
        block_cache::BlockCache,
        segment::{block_index::BlockIndex, meta::Metadata, reader::Reader},
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn test_write_and_read() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

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

        writer.finish()?;

        let segment_id = 532;

        let metadata = Metadata::from_writer(segment_id, writer)?;
        metadata.write_to_file(&folder)?;
        assert_eq!(ITEM_COUNT, metadata.item_count);
        assert_eq!(ITEM_COUNT, metadata.key_count);

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(folder.join(BLOCKS_FILE), (0, segment_id).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            (0, segment_id).into(),
            table.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);
        let iter = Reader::new(
            table,
            (0, segment_id).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
            None,
            None,
        );

        assert_eq!(ITEM_COUNT, iter.count() as u64);

        Ok(())
    }

    #[test]
    fn test_write_and_read_mvcc() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 1_000;
        const VERSION_COUNT: u64 = 5;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

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

        writer.finish()?;

        let segment_id = 532;

        let metadata = Metadata::from_writer(segment_id, writer)?;
        metadata.write_to_file(&folder)?;
        assert_eq!(ITEM_COUNT * VERSION_COUNT, metadata.item_count);
        assert_eq!(ITEM_COUNT, metadata.key_count);

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(folder.join(BLOCKS_FILE), (0, segment_id).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            (0, segment_id).into(),
            table.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        let iter = Reader::new(
            table,
            (0, segment_id).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
            None,
            None,
        );

        assert_eq!(ITEM_COUNT * VERSION_COUNT, iter.count() as u64);

        Ok(())
    }
}
