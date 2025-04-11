mod index;
mod meta;

use super::{block::Header as BlockHeader, trailer::Trailer, Block, BlockOffset, DataBlock};
use crate::{
    coding::Encode, file::fsync_directory, super_segment::index_block::NewBlockHandle,
    CompressionType, InternalValue, SegmentId, UserKey,
};
use index::Writer as IndexWriter;
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::PathBuf,
};

/// Serializes and compresses values into blocks and writes them to disk as segment
pub struct Writer {
    /// Segment file
    path: PathBuf,

    segment_id: SegmentId,

    data_block_size: u32,

    /// Compression to use
    compression: CompressionType,

    /// Writer of data blocks
    #[allow(clippy::struct_field_names)]
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
    // bloom_policy: BloomConstructionPolicy,

    // /// Hashes for bloom filter
    // ///
    // /// using enhanced double hashing, so we got two u64s
    // bloom_hash_buffer: Vec<(u64, u64)>,
}

impl Writer {
    pub fn new(path: PathBuf, segment_id: SegmentId) -> crate::Result<Self> {
        let block_writer = File::create(&path)?;
        let block_writer = BufWriter::with_capacity(u16::MAX.into(), block_writer);

        Ok(Self {
            meta: meta::Metadata::default(),

            segment_id,

            data_block_size: 4_096,

            compression: CompressionType::None,

            path: std::path::absolute(path)?,

            index_writer: IndexWriter::new(4_096 /* TODO: hard coded for now */),

            block_writer,
            chunk: Vec::new(),

            prev_pos: (BlockOffset(0), BlockOffset(0)),

            chunk_size: 0,

            current_key: None,
        })
    }

    // TODO: data_block_size setter

    #[must_use]
    pub(crate) fn use_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self.index_writer = self.index_writer.use_compression(compression);
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

            // TODO:
            // // IMPORTANT: Do not buffer *every* item's key
            // // because there may be multiple versions
            // // of the same key
            // if self.bloom_policy.is_active() {
            //     self.bloom_hash_buffer
            //         .push(BloomFilter::get_hash(&item.key.user_key));
            // }
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

        let bytes = DataBlock::encode_items(&self.chunk, 16, 0.75)?;

        // TODO: prev block offset
        let header = Block::to_writer(&mut self.block_writer, &bytes, self.compression)?;

        self.meta.uncompressed_size += u64::from(header.uncompressed_length);

        let bytes_written = (BlockHeader::serialized_len() + bytes.len()) as u32;

        self.index_writer.register_block(
            last.key.user_key.clone(),
            self.meta.file_pos,
            bytes_written,
        )?;

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

    /// Finishes the segment, making sure all data is written durably
    pub fn finish(mut self) -> crate::Result<Option<Trailer>> {
        self.spill_block()?;

        // No items written! Just delete segment file and return nothing
        if self.meta.item_count == 0 {
            std::fs::remove_file(&self.path)?;
            return Ok(None);
        }

        let index_block_start = BlockOffset(self.block_writer.stream_position()?);

        // // Append index blocks to file
        let tli_handle = self.index_writer.finish(&mut self.block_writer)?;

        let index_block_handle = NewBlockHandle::new(
            index_block_start,
            (*tli_handle.offset() - *index_block_start) as u32,
        );

        self.meta.index_block_count = self.index_writer.block_count;

        // // Write bloom filter
        // let bloom_ptr = {
        //     if self.bloom_hash_buffer.is_empty() {
        //         BlockOffset(0)
        //     } else {
        //         let bloom_ptr = self.block_writer.stream_position()?;
        //         let n = self.bloom_hash_buffer.len();

        //         log::trace!(
        //             "Constructing Bloom filter with {n} entries: {:?}",
        //             self.bloom_policy,
        //         );

        //         let start = std::time::Instant::now();

        //         let mut filter = self.bloom_policy.build(n);

        //         for hash in std::mem::take(&mut self.bloom_hash_buffer) {
        //             filter.set_with_hash(hash);
        //         }

        //         log::trace!("Built Bloom filter in {:?}", start.elapsed());

        //         filter.encode_into(&mut self.block_writer)?;

        //         BlockOffset(bloom_ptr)
        //     }
        // };
        // log::trace!("bloom_ptr={bloom_ptr}");

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

            let meta_items = [
                meta(
                    "#data_block_count",
                    &self.meta.data_block_count.to_le_bytes(),
                ),
                meta("#id", &self.segment_id.to_le_bytes()),
                meta(
                    "#index_block_count",
                    &self.meta.index_block_count.to_le_bytes(),
                ),
                meta("#item_count", &self.meta.item_count.to_le_bytes()),
                meta(
                    "#key#max",
                    self.meta.last_key.as_ref().expect("should exist"),
                ),
                meta(
                    "#key#min",
                    self.meta.first_key.as_ref().expect("should exist"),
                ),
                meta("#key_count", &self.meta.key_count.to_le_bytes()),
                meta("#seqno#max", &self.meta.highest_seqno.to_le_bytes()),
                meta("#seqno#min", &self.meta.lowest_seqno.to_le_bytes()),
                meta("#size", &self.meta.file_pos.to_le_bytes()),
                meta("#tombstone_count", &self.meta.tombstone_count.to_le_bytes()),
                meta(
                    "#user_data_size",
                    &self.meta.uncompressed_size.to_le_bytes(),
                ),
                meta("version#lsmt", env!("CARGO_PKG_VERSION").as_bytes()),
                meta("version#table", b"3.0"),
            ];

            #[cfg(debug_assertions)]
            {
                let mut sorted_copy = meta_items.clone();
                sorted_copy.sort();

                // Just to make sure the items are definitely sorted
                assert_eq!(meta_items, sorted_copy, "meta items not sorted correctly");
            }

            log::trace!(
                "Writing metadata to segment file {:?}: {meta_items:#?}",
                self.path,
            );

            // TODO: no binary index
            let bytes = DataBlock::encode_items(&meta_items, 1, 0.0)?;
            let _header = Block::to_writer(&mut self.block_writer, &bytes, CompressionType::None)?;

            let bytes_written = BlockHeader::serialized_len() + bytes.len();

            NewBlockHandle::new(metadata_start, bytes_written as u32)
        };

        // Bundle all the file offsets
        let trailer = Trailer {
            index_block: index_block_handle,
            tli: tli_handle,
            filter: NewBlockHandle::default(),
            metadata: metadata_handle,
            /* range_filter:range_filter_ptr: rf:rf_ptr,
            range_tombstones:range_tombstones_ptr,
            pfx:pfx_ptr, */
        };

        log::trace!(
            "Writing trailer to segment file {:?}: {trailer:#?}",
            self.path,
        );

        // Write trailer
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

        Ok(Some(trailer))
    }
}
