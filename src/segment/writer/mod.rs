// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod meta;

use super::{
    block::header::Header as BlockHeader,
    block_index::writer::Writer as IndexWriter,
    file_offsets::FileOffsets,
    meta::{CompressionType, Metadata},
    trailer::SegmentFileTrailer,
    value_block::ValueBlock,
};
use crate::{
    coding::Encode,
    file::fsync_directory,
    prefix_extractor::PrefixExtractor,
    segment::{block::ItemSize, value_block::BlockOffset},
    value::{InternalValue, UserKey},
    SegmentId,
};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::PathBuf,
    sync::Arc,
};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

/// Serializes and compresses values into blocks and writes them to disk as segment
pub struct Writer {
    pub(crate) opts: Options,

    /// Compression to use
    compression: CompressionType,

    /// Segment file
    segment_file_path: PathBuf,

    /// Writer of data blocks
    block_writer: BufWriter<File>,

    /// Writer of index blocks
    index_writer: IndexWriter,

    /// Buffer of KVs
    chunk: Vec<InternalValue>,
    chunk_size: usize,

    pub(crate) meta: meta::Metadata,

    /// Stores the previous block position (used for creating back links)
    prev_pos: (BlockOffset, BlockOffset),

    current_key: Option<UserKey>,

    can_rotate: bool,

    #[cfg(feature = "bloom")]
    bloom_policy: BloomConstructionPolicy,

    /// Hashes for bloom filter
    ///
    /// using enhanced double hashing, so we got two u64s
    #[cfg(feature = "bloom")]
    bloom_hash_buffer: Vec<(u64, u64)>,
}

#[derive(Copy, Clone, Debug)]
#[cfg(feature = "bloom")]
pub enum BloomConstructionPolicy {
    BitsPerKey(u8),
    FpRate(f32),
}

#[cfg(feature = "bloom")]
impl Default for BloomConstructionPolicy {
    fn default() -> Self {
        Self::BitsPerKey(10)
    }
}

#[cfg(feature = "bloom")]
impl BloomConstructionPolicy {
    #[must_use]
    pub fn build(&self, n: usize) -> BloomFilter {
        match self {
            Self::BitsPerKey(bpk) => BloomFilter::with_bpk(n, *bpk),
            Self::FpRate(fpr) => BloomFilter::with_fp_rate(n, *fpr),
        }
    }

    pub fn is_active(&self) -> bool {
        match self {
            Self::BitsPerKey(bpk) => *bpk > 0,
            Self::FpRate(_) => true,
        }
    }
}

pub struct Options {
    pub folder: PathBuf,
    pub data_block_size: u32,
    pub index_block_size: u32,
    pub segment_id: SegmentId,
    pub prefix_extractor: Option<Arc<dyn PrefixExtractor>>,
}

impl Writer {
    /// Sets up a new `Writer` at the given folder
    pub fn new(opts: Options) -> crate::Result<Self> {
        let segment_file_path = opts.folder.join(opts.segment_id.to_string());

        let block_writer = File::create(&segment_file_path)?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), block_writer);

        let index_writer = IndexWriter::new(opts.index_block_size)?;

        let chunk = Vec::new();

        Ok(Self {
            opts,
            meta: meta::Metadata::default(),

            compression: CompressionType::None,

            segment_file_path,

            block_writer,
            index_writer,
            chunk,

            prev_pos: (BlockOffset(0), BlockOffset(0)),

            chunk_size: 0,

            current_key: None,

            can_rotate: false,

            #[cfg(feature = "bloom")]
            bloom_policy: BloomConstructionPolicy::default(),

            #[cfg(feature = "bloom")]
            bloom_hash_buffer: Vec::new(),
        })
    }

    #[must_use]
    pub fn can_rotate(&self) -> bool {
        self.can_rotate
    }

    #[must_use]
    pub(crate) fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self.index_writer = self.index_writer.use_compression(compression);
        self
    }

    #[must_use]
    #[cfg(feature = "bloom")]
    pub(crate) fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self
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

        let (header, data) =
            ValueBlock::to_bytes_compressed(&self.chunk, self.prev_pos.0, self.compression)?;

        self.meta.uncompressed_size += u64::from(header.uncompressed_length);

        header.encode_into(&mut self.block_writer)?;

        // Write to file
        self.block_writer.write_all(&data)?;

        let bytes_written = (BlockHeader::serialized_len() + data.len()) as u64;

        self.index_writer
            .register_block(last.key.user_key.clone(), self.meta.file_pos)?;

        // Adjust metadata
        self.meta.file_pos += bytes_written;
        self.meta.item_count += self.chunk.len();
        self.meta.data_block_count += 1;

        // Back link stuff
        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += bytes_written;

        // Set last key
        self.meta.last_key = Some(
            // NOTE: Expect is fine, because the chunk is not empty
            //
            // Also, we are allowed to remove the last item
            // to get ownership of it, because the chunk is cleared after
            // this anyway
            #[allow(clippy::expect_used)]
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

    /// Writes an item.
    ///
    /// # Note
    ///
    /// It's important that the incoming stream of items is correctly
    /// sorted as described by the [`UserKey`], otherwise the block layout will
    /// be non-sense.
    pub fn write(&mut self, item: InternalValue) -> crate::Result<()> {
        if item.is_tombstone() {
            self.meta.tombstone_count += 1;
        }

        // NOTE: Check if we visit a new key
        if Some(&item.key.user_key) != self.current_key.as_ref() {
            // IMPORTANT: Check that we are not at the first key
            if self.current_key.is_some() {
                self.can_rotate = true;
            }

            self.meta.key_count += 1;
            self.current_key = Some(item.key.user_key.clone());

            // IMPORTANT: Do not buffer *every* item's key
            // because there may be multiple versions
            // of the same key
            #[cfg(feature = "bloom")]
            if self.bloom_policy.is_active() {
                let key: Option<&[u8]> = match &self.opts.prefix_extractor {
                    None => Some(&item.key.user_key),
                    Some(prefix_extractor) => {
                        let key = &item.key.user_key;
                        if prefix_extractor.in_domain(key) {
                            Some(prefix_extractor.transform(key))
                        } else {
                            None
                        }
                    }
                };
                if let Some(key) = key {
                    self.bloom_hash_buffer.push(BloomFilter::get_hash(key));
                };
            }
        }

        let seqno = item.key.seqno;

        if self.meta.first_key.is_none() {
            self.meta.first_key = Some(item.key.user_key.clone());
        }

        self.chunk_size += item.size();
        self.chunk.push(item);

        if self.chunk_size >= self.opts.data_block_size as usize {
            self.spill_block()?;
        }

        self.meta.lowest_seqno = self.meta.lowest_seqno.min(seqno);
        self.meta.highest_seqno = self.meta.highest_seqno.max(seqno);

        Ok(())
    }

    // TODO: should take mut self to avoid double finish

    /// Finishes the segment, making sure all data is written durably
    pub fn finish(&mut self) -> crate::Result<Option<SegmentFileTrailer>> {
        self.spill_block()?;

        // No items written! Just delete segment file and return nothing
        if self.meta.item_count == 0 {
            std::fs::remove_file(&self.segment_file_path)?;
            return Ok(None);
        }

        let index_block_ptr = BlockOffset(self.block_writer.stream_position()?);
        log::trace!("index_block_ptr={index_block_ptr}");

        // Append index blocks to file
        let tli_ptr = self.index_writer.finish(&mut self.block_writer)?;
        log::trace!("tli_ptr={tli_ptr}");

        self.meta.index_block_count = self.index_writer.block_count;

        // Write bloom filter
        #[cfg(feature = "bloom")]
        let bloom_ptr = {
            if self.bloom_hash_buffer.is_empty() {
                BlockOffset(0)
            } else {
                let bloom_ptr = self.block_writer.stream_position()?;

                let n = self.bloom_hash_buffer.len();
                log::trace!(
                    "Writing bloom filter with {n} hashes: {:?}",
                    self.bloom_policy
                );

                let mut filter = self.bloom_policy.build(n);

                for hash in std::mem::take(&mut self.bloom_hash_buffer) {
                    filter.set_with_hash(hash);
                }

                filter.encode_into(&mut self.block_writer)?;

                BlockOffset(bloom_ptr)
            }
        };

        #[cfg(not(feature = "bloom"))]
        let bloom_ptr = BlockOffset(0);
        log::trace!("bloom_ptr={bloom_ptr}");

        // TODO: #46 https://github.com/fjall-rs/lsm-tree/issues/46 - Write range filter
        let rf_ptr = BlockOffset(0);
        log::trace!("rf_ptr={rf_ptr}");

        // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2 - Write range tombstones
        let range_tombstones_ptr = BlockOffset(0);
        log::trace!("range_tombstones_ptr={range_tombstones_ptr}");

        // TODO:
        let pfx_ptr = BlockOffset(0);
        log::trace!("pfx_ptr={pfx_ptr}");

        // Write metadata
        let metadata_ptr = BlockOffset(self.block_writer.stream_position()?);

        let metadata = Metadata::from_writer(self.opts.segment_id, self)?;
        metadata.encode_into(&mut self.block_writer)?;

        // Bundle all the file offsets
        let offsets = FileOffsets {
            index_block_ptr,
            tli_ptr,
            bloom_ptr,
            range_filter_ptr: rf_ptr,
            range_tombstones_ptr,
            pfx_ptr,
            metadata_ptr,
        };

        // Write trailer
        let trailer = SegmentFileTrailer { metadata, offsets };
        trailer.encode_into(&mut self.block_writer)?;

        // Finally, flush & fsync the blocks file
        self.block_writer.flush()?;
        self.block_writer.get_mut().sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&self.opts.folder)?;

        log::debug!(
            "Written {} items in {} blocks into new segment file, written {} MB of data blocks",
            self.meta.item_count,
            self.meta.data_block_count,
            *self.meta.file_pos / 1_024 / 1_024
        );

        Ok(Some(trailer))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::block_cache::BlockCache;
    use crate::descriptor_table::FileDescriptorTable;
    use crate::segment::block_index::top_level::TopLevelIndex;
    use crate::segment::reader::Reader;
    use crate::value::{InternalValue, ValueType};
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn segment_writer_seqnos() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let segment_id = 532;

        let mut writer = Writer::new(Options {
            folder,
            data_block_size: 4_096,
            index_block_size: 4_096,
            segment_id,
            prefix_extractor: None,
        })?;

        writer.write(InternalValue::from_components(
            "a",
            nanoid::nanoid!().as_bytes(),
            7,
            ValueType::Value,
        ))?;
        writer.write(InternalValue::from_components(
            "b",
            nanoid::nanoid!().as_bytes(),
            5,
            ValueType::Value,
        ))?;
        writer.write(InternalValue::from_components(
            "c",
            nanoid::nanoid!().as_bytes(),
            8,
            ValueType::Value,
        ))?;
        writer.write(InternalValue::from_components(
            "d",
            nanoid::nanoid!().as_bytes(),
            10,
            ValueType::Value,
        ))?;

        let trailer = writer.finish()?.expect("should exist");

        assert_eq!(5, trailer.metadata.seqnos.0);
        assert_eq!(10, trailer.metadata.seqnos.1);

        Ok(())
    }

    #[test]
    #[cfg(feature = "bloom")]
    fn segment_writer_zero_bpk() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100;

        let folder = tempfile::tempdir()?.into_path();

        let segment_id = 532;

        let mut writer = Writer::new(Options {
            folder,
            data_block_size: 4_096,
            index_block_size: 4_096,
            segment_id,
            prefix_extractor: Option::None,
        })?
        .use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0));

        let items = (0u64..ITEM_COUNT).map(|i| {
            InternalValue::from_components(
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
        assert_eq!(trailer.offsets.bloom_ptr, BlockOffset(0));

        Ok(())
    }

    #[test]
    fn segment_writer_write_read() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100;

        let folder = tempfile::tempdir()?.into_path();

        let segment_id = 532;

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            data_block_size: 4_096,
            index_block_size: 4_096,
            segment_id,
            prefix_extractor: None,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            InternalValue::from_components(
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

        #[cfg(feature = "bloom")]
        assert!(*trailer.offsets.bloom_ptr > 0);

        let segment_file_path = folder.join(segment_id.to_string());

        // NOTE: The TLI is bound by the index block count, because we know the index block count is u32
        // the TLI length fits into u32 as well
        #[allow(clippy::cast_possible_truncation)]
        {
            let tli = TopLevelIndex::from_file(
                &segment_file_path,
                &trailer.metadata,
                trailer.offsets.tli_ptr,
            )?;

            assert_eq!(tli.len() as u32, trailer.metadata.index_block_count);
        }

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(segment_file_path, (0, segment_id).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        let iter = Reader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, segment_id).into(),
            block_cache,
            BlockOffset(0),
            None,
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
            data_block_size: 4_096,
            index_block_size: 4_096,
            segment_id,
            prefix_extractor: None,
        })?;

        for key in 0u64..ITEM_COUNT {
            for seqno in (0..VERSION_COUNT).rev() {
                let value = InternalValue::from_components(
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

        #[cfg(feature = "bloom")]
        assert!(*trailer.offsets.bloom_ptr > 0);

        let segment_file_path = folder.join(segment_id.to_string());

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(segment_file_path, (0, segment_id).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        let iter = Reader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, segment_id).into(),
            block_cache,
            BlockOffset(0),
            None,
        );

        assert_eq!(ITEM_COUNT * VERSION_COUNT, iter.count() as u64);

        Ok(())
    }
}
