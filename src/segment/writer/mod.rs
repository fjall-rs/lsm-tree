mod index;
mod meta;

use super::{
    block::Header as BlockHeader, filter::BloomConstructionPolicy, trailer::Trailer, Block,
    BlockOffset, DataBlock, KeyedBlockHandle,
};
use crate::{
    coding::Encode,
    file::fsync_directory,
    prefix::SharedPrefixExtractor,
    segment::{filter::standard_bloom::Builder, index_block::BlockHandle, regions::ParsedRegions},
    time::unix_timestamp,
    CompressionType, InternalValue, SegmentId, UserKey,
};
use index::{BlockIndexWriter, FullIndexWriter};
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::PathBuf,
};

/// Serializes and compresses values into blocks and writes them to disk as segment
pub struct Writer {
    /// Segment file
    pub(crate) path: PathBuf,

    segment_id: SegmentId,

    data_block_restart_interval: u8, // TODO:
    data_block_hash_ratio: f32,

    data_block_size: u32,
    index_block_size: u32, // TODO: implement

    /// Compression to use
    compression: CompressionType,

    /// Buffer to serialize blocks into
    block_buffer: Vec<u8>,

    /// Writer of data blocks
    #[allow(clippy::struct_field_names)]
    block_writer: BufWriter<File>,

    /// Writer of index blocks
    index_writer: Box<dyn BlockIndexWriter<BufWriter<File>>>,

    /// Buffer of KVs
    chunk: Vec<InternalValue>,
    chunk_size: usize,

    pub(crate) meta: meta::Metadata,

    /// Stores the previous block position (used for creating back links)
    prev_pos: (BlockOffset, BlockOffset),

    current_key: Option<UserKey>,

    bloom_policy: BloomConstructionPolicy,

    /// Hashes for bloom filter
    ///
    /// using enhanced double hashing, so we got two u64s
    pub bloom_hash_buffer: Vec<u64>,

    /// Prefix extractor for filters
    pub prefix_extractor: Option<SharedPrefixExtractor>,
}

impl Writer {
    pub fn new(path: PathBuf, segment_id: SegmentId) -> crate::Result<Self> {
        let block_writer = File::create_new(&path)?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), block_writer);

        Ok(Self {
            meta: meta::Metadata::default(),

            segment_id,

            data_block_restart_interval: 16,
            data_block_hash_ratio: 0.0,

            data_block_size: 4_096,
            index_block_size: 4_096,

            compression: CompressionType::None,

            path: std::path::absolute(path)?,

            index_writer: Box::new(FullIndexWriter::new()),

            block_buffer: Vec::new(),
            block_writer,
            chunk: Vec::new(),

            prev_pos: (BlockOffset(0), BlockOffset(0)),

            chunk_size: 0,

            current_key: None,

            bloom_policy: BloomConstructionPolicy::default(),

            bloom_hash_buffer: Vec::new(),
            prefix_extractor: None,
        })
    }

    #[must_use]
    pub fn use_data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self
    }

    #[must_use]
    pub fn use_data_block_size(mut self, size: u32) -> Self {
        assert!(
            size <= 4 * 1_024 * 1_024,
            "data block size must be <= 4 MiB",
        );
        self.data_block_size = size;
        self
    }

    #[must_use]
    pub fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self.index_writer.use_compression(compression);
        self
    }

    #[must_use]
    pub fn use_bloom_policy(mut self, bloom_policy: BloomConstructionPolicy) -> Self {
        self.bloom_policy = bloom_policy;
        self
    }

    #[must_use]
    pub fn use_prefix_extractor(mut self, extractor: SharedPrefixExtractor) -> Self {
        self.prefix_extractor = Some(extractor);
        self
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
            self.meta.key_count += 1;
            self.current_key = Some(item.key.user_key.clone());

            // IMPORTANT: Do not buffer *every* item's key
            // because there may be multiple versions
            // of the same key

            if self.bloom_policy.is_active() {
                if let Some(ref extractor) = self.prefix_extractor {
                    // Add all prefixes to filter
                    // If extract returns empty iterator (out of domain), nothing is added
                    for prefix in extractor.extract(&item.key.user_key) {
                        self.bloom_hash_buffer.push(Builder::get_hash(prefix));
                    }
                } else {
                    // Default behavior: add full key hash
                    self.bloom_hash_buffer
                        .push(Builder::get_hash(&item.key.user_key));
                }
            }
        }

        let seqno = item.key.seqno;

        if self.meta.first_key.is_none() {
            self.meta.first_key = Some(item.key.user_key.clone());
        }

        self.chunk_size += item.key.user_key.len() + item.value.len();
        self.chunk.push(item);

        if self.chunk_size >= self.data_block_size as usize {
            self.spill_block()?;
        }

        self.meta.lowest_seqno = self.meta.lowest_seqno.min(seqno);
        self.meta.highest_seqno = self.meta.highest_seqno.max(seqno);

        Ok(())
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

        self.block_buffer.clear();

        DataBlock::encode_into(
            &mut self.block_buffer,
            &self.chunk,
            self.data_block_restart_interval,
            self.data_block_hash_ratio,
        )?;

        // log::warn!("encoding {:?}", self.chunk);
        // log::warn!(
        //     "encoded 0x{:#X?} -> {:?}",
        //     self.meta.file_pos,
        //     self.block_buffer
        // );

        // TODO: prev block offset
        let header = Block::write_into(
            &mut self.block_writer,
            &self.block_buffer,
            super::block::BlockType::Data,
            self.compression,
        )?;

        self.meta.uncompressed_size += u64::from(header.uncompressed_length);

        let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

        self.index_writer
            .register_data_block(KeyedBlockHandle::new(
                last.key.user_key.clone(),
                self.meta.file_pos,
                bytes_written,
            ))?;

        // Adjust metadata
        self.meta.file_pos += bytes_written as u64;
        self.meta.item_count += self.chunk.len();
        self.meta.data_block_count += 1;

        // Back link stuff
        self.prev_pos.0 = self.prev_pos.1;
        self.prev_pos.1 += bytes_written as u64;

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

    // TODO: 3.0.0 split meta writing into new function
    #[allow(clippy::too_many_lines)]
    /// Finishes the segment, making sure all data is written durably
    pub fn finish(mut self) -> crate::Result<Option<SegmentId>> {
        self.spill_block()?;

        // No items written! Just delete segment file and return nothing
        if self.meta.item_count == 0 {
            std::fs::remove_file(&self.path)?;
            return Ok(None);
        }

        // // Append index blocks to file
        let (tli_handle, index_blocks_handle) = self.index_writer.finish(&mut self.block_writer)?;
        log::trace!("tli_ptr={tli_handle:?}");
        log::trace!("index_blocks_ptr={index_blocks_handle:?}");

        // Write filter
        let filter_handle = {
            if self.bloom_hash_buffer.is_empty() {
                None
            } else {
                let filter_ptr = self.block_writer.stream_position()?;
                let n = self.bloom_hash_buffer.len();

                log::trace!(
                    "Constructing Bloom filter with {n} entries: {:?}",
                    self.bloom_policy,
                );

                let start = std::time::Instant::now();

                let filter_bytes = {
                    let mut builder = self.bloom_policy.init(n);

                    for hash in self.bloom_hash_buffer {
                        builder.set_with_hash(hash);
                    }

                    builder.build()
                };

                log::trace!(
                    "Built Bloom filter ({} B) in {:?}",
                    filter_bytes.len(),
                    start.elapsed(),
                );

                let header = Block::write_into(
                    &mut self.block_writer,
                    &filter_bytes,
                    crate::segment::block::BlockType::Filter,
                    CompressionType::None,
                )?;

                let bytes_written = (BlockHeader::serialized_len() as u32) + header.data_length;

                Some(BlockHandle::new(BlockOffset(filter_ptr), bytes_written))
            }
        };
        log::trace!("filter_ptr={filter_handle:?}");

        // // TODO: #46 https://github.com/fjall-rs/lsm-tree/issues/46 - Write range filter
        // let rf_ptr = BlockOffset(0);
        // log::trace!("rf_ptr={rf_ptr}");

        // // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2 - Write range tombstones
        // let range_tombstones_ptr = BlockOffset(0);
        // log::trace!("range_tombstones_ptr={range_tombstones_ptr}");

        // // TODO:
        // let pfx_ptr = BlockOffset(0);
        // log::trace!("pfx_ptr={pfx_ptr}");

        // Write metadata
        let metadata_start = BlockOffset(self.block_writer.stream_position()?);

        let metadata_handle = {
            fn meta(key: &str, value: &[u8]) -> InternalValue {
                InternalValue::from_components(key, value, 0, crate::ValueType::Value)
            }

            let mut meta_items = vec![
                meta("#checksum_type", b"xxh3"),
                meta("#compression#data", &self.compression.encode_into_vec()),
                meta("#compression#index", &self.compression.encode_into_vec()),
                meta("#created_at", &unix_timestamp().as_nanos().to_le_bytes()),
                meta(
                    "#data_block_count",
                    &(self.meta.data_block_count as u64).to_le_bytes(),
                ),
                meta("#hash_type", b"xxh3"),
                meta("#id", &self.segment_id.to_le_bytes()),
                meta(
                    "#index_block_count",
                    &(self.index_writer.len() as u64).to_le_bytes(),
                ),
                meta("#item_count", &(self.meta.item_count as u64).to_le_bytes()),
                meta(
                    "#key#max",
                    self.meta.last_key.as_ref().expect("should exist"),
                ),
                meta(
                    "#key#min",
                    self.meta.first_key.as_ref().expect("should exist"),
                ),
                meta("#key_count", &(self.meta.key_count as u64).to_le_bytes()),
            ];

            if let Some(ref extractor) = self.prefix_extractor {
                meta_items.push(meta("#prefix_extractor", extractor.name().as_bytes()));
            }

            meta_items.extend([
                meta("#prefix_truncation#data", &[1]),
                meta("#prefix_truncation#index", &[0]),
                meta("#seqno#max", &self.meta.highest_seqno.to_le_bytes()),
                meta("#seqno#min", &self.meta.lowest_seqno.to_le_bytes()),
                meta("#size", &self.meta.file_pos.to_le_bytes()),
                meta(
                    "#tombstone_count",
                    &(self.meta.tombstone_count as u64).to_le_bytes(),
                ),
                meta(
                    "#user_data_size",
                    &self.meta.uncompressed_size.to_le_bytes(),
                ),
                meta("v#lsmt", env!("CARGO_PKG_VERSION").as_bytes()),
                meta("v#table", b"3"),
                // TODO: tli_handle_count
            ]);

            // NOTE: Just to make sure the items are definitely sorted
            #[cfg(debug_assertions)]
            {
                let is_sorted = meta_items.iter().is_sorted_by_key(|kv| &kv.key);
                assert!(is_sorted, "meta items not sorted correctly");
            }

            log::trace!("Encoding metadata block: {meta_items:#?}");

            self.block_buffer.clear();

            // TODO: no binary index
            DataBlock::encode_into(&mut self.block_buffer, &meta_items, 1, 0.0)?;

            let header = Block::write_into(
                &mut self.block_writer,
                &self.block_buffer,
                crate::segment::block::BlockType::Meta,
                CompressionType::None,
            )?;

            let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

            BlockHandle::new(metadata_start, bytes_written)
        };

        // Write regions block
        let regions_block_handle = {
            let regions_block_start = BlockOffset(self.block_writer.stream_position()?);

            let regions = ParsedRegions {
                tli: tli_handle,
                index: None,
                filter: filter_handle,
                metadata: metadata_handle,
            };

            log::trace!("Encoding regions: {regions:#?}");

            let bytes = regions.encode_into_vec()?;
            let header = Block::write_into(
                &mut self.block_writer,
                &bytes,
                crate::segment::block::BlockType::Regions,
                CompressionType::None,
            )?;

            let bytes_written = BlockHeader::serialized_len() as u32 + header.data_length;

            BlockHandle::new(regions_block_start, bytes_written as u32)
        };

        // Write fixed-size trailer
        let trailer = Trailer::from_handle(regions_block_handle);
        trailer.write_into(&mut self.block_writer)?;

        // Finally, flush & fsync the blocks file
        self.block_writer.flush()?;
        self.block_writer.get_mut().sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(self.path.parent().expect("should have folder"))?;

        log::debug!(
            "Written {} items in {} blocks into new segment file, written {} MiB",
            self.meta.item_count,
            self.meta.data_block_count,
            *self.meta.file_pos / 1_024 / 1_024,
        );

        Ok(Some(self.segment_id))
    }
}

// TODO: restore
/*
#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::cache::Cache;
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
    fn segment_writer_zero_bpk() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 100;

        let folder = tempfile::tempdir()?.into_path();

        let segment_id = 532;

        let mut writer = Writer::new(Options {
            folder,
            data_block_size: 4_096,
            index_block_size: 4_096,
            segment_id,
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

        let block_cache = Arc::new(Cache::with_capacity_bytes(10 * 1_024 * 1_024));

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

        assert!(*trailer.offsets.bloom_ptr > 0);

        let segment_file_path = folder.join(segment_id.to_string());

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(segment_file_path, (0, segment_id).into());

        let block_cache = Arc::new(Cache::with_capacity_bytes(10 * 1_024 * 1_024));

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
 */
