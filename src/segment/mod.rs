pub mod block;
pub mod block_index;
pub mod file_offsets;
pub mod id;
pub mod meta;
pub mod multi_reader;
pub mod multi_writer;
pub mod prefix;
pub mod range;
pub mod reader;
pub mod trailer;
pub mod value_block;
pub mod value_block_consumer;
pub mod writer;

use self::{
    block_index::BlockIndex, file_offsets::FileOffsets, prefix::PrefixedReader, range::Range,
};
use crate::{
    block_cache::BlockCache,
    descriptor_table::FileDescriptorTable,
    segment::{reader::Reader, value_block_consumer::ValueBlockConsumer},
    tree::inner::TreeId,
    value::{SeqNo, UserKey},
    Value,
};
use std::{ops::Bound, path::Path, sync::Arc};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

/// Disk segment (a.k.a. `SSTable`, `SST`, `sorted string table`) that is located on disk
///
/// A segment is an immutable list of key-value pairs, split into compressed blocks.
/// A reference to the block (`block handle`) is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to remove duplicate items, reducing disk space and improving read performance.
#[doc(alias = "sstable")]
pub struct Segment {
    pub(crate) tree_id: TreeId,

    #[doc(hidden)]
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Segment metadata object
    pub metadata: meta::Metadata,

    pub offsets: FileOffsets,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    #[doc(hidden)]
    pub block_index: Arc<BlockIndex>,

    /// Block cache
    ///
    /// Stores index and data blocks
    #[doc(hidden)]
    pub block_cache: Arc<BlockCache>,

    /// Bloom filter
    #[cfg(feature = "bloom")]
    #[doc(hidden)]
    pub bloom_filter: BloomFilter,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment:{}", self.metadata.id)
    }
}

impl Segment {
    pub(crate) fn verify(&self) -> crate::Result<usize> {
        use block_index::IndexBlock;
        use value_block::ValueBlock;

        let mut count = 0;
        let mut broken_count = 0;

        let guard = self
            .descriptor_table
            .access(&(self.tree_id, self.metadata.id).into())?
            .expect("should have gotten file");

        let mut file = guard.file.lock().expect("lock is poisoned");

        for handle in self.block_index.top_level_index.data.iter() {
            let block = IndexBlock::from_file_compressed(&mut *file, handle.offset)?;

            for handle in &*block.items {
                let value_block = match ValueBlock::from_file_compressed(&mut *file, handle.offset)
                {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "{handle:?} could not be loaded, it is probably corrupted: {e:?}"
                        );
                        broken_count += 1;
                        count += 1;
                        continue;
                    }
                };

                let expected_crc = value_block.header.crc;
                let actual_crc = ValueBlock::create_crc(&value_block.items)?;

                if expected_crc != actual_crc {
                    log::error!("{handle:?} is corrupt, invalid CRC value");
                    broken_count += 1;
                }

                count += 1;

                if broken_count % 10_000 == 0 {
                    log::info!("Checked {count} data blocks");
                }
            }
        }

        assert_eq!(count, self.metadata.block_count);

        Ok(broken_count)
    }

    /// Tries to recover a segment from a file.
    pub fn recover<P: AsRef<Path>>(
        file_path: P,
        tree_id: TreeId,
        block_cache: Arc<BlockCache>,
        descriptor_table: Arc<FileDescriptorTable>,
    ) -> crate::Result<Self> {
        use trailer::SegmentFileTrailer;

        let file_path = file_path.as_ref();

        log::debug!("Recovering segment from file {file_path:?}");
        let trailer = SegmentFileTrailer::from_file(file_path)?;

        log::debug!(
            "Creating block index, with tli_ptr={}",
            trailer.offsets.tli_ptr
        );
        let block_index = BlockIndex::from_file(
            file_path,
            trailer.offsets.tli_ptr,
            (tree_id, trailer.metadata.id).into(),
            descriptor_table.clone(),
            Arc::clone(&block_cache),
        )?;

        #[cfg(feature = "bloom")]
        let bloom_ptr = trailer.offsets.bloom_ptr;

        Ok(Self {
            tree_id,

            descriptor_table,
            metadata: trailer.metadata,
            offsets: trailer.offsets,

            block_index: Arc::new(block_index),
            block_cache,

            // TODO: as Bloom method
            #[cfg(feature = "bloom")]
            bloom_filter: {
                use crate::serde::Deserializable;
                use std::{
                    fs::File,
                    io::{Seek, SeekFrom},
                };

                assert!(bloom_ptr > 0, "can not find bloom filter block");

                let mut reader = File::open(file_path)?;
                reader.seek(SeekFrom::Start(bloom_ptr))?;
                BloomFilter::deserialize(&mut reader)?
            },
        })
    }

    #[cfg(feature = "bloom")]
    #[must_use]
    /// Gets the bloom filter size
    pub fn bloom_filter_size(&self) -> usize {
        self.bloom_filter.len()
    }

    /// Retrieves an item from the segment.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<Value>> {
        use value_block::{CachePolicy, ValueBlock};

        if let Some(seqno) = seqno {
            if self.metadata.seqnos.0 >= seqno {
                return Ok(None);
            }
        }

        if !self.metadata.key_range.contains_key(&key) {
            return Ok(None);
        }

        let key = key.as_ref();

        #[cfg(feature = "bloom")]
        {
            if !self.bloom_filter.contains(key) {
                return Ok(None);
            }
        }

        let Some(first_block_handle) = self
            .block_index
            .get_lowest_data_block_handle_containing_item(key.as_ref(), CachePolicy::Write)?
        else {
            return Ok(None);
        };

        let Some(block) = ValueBlock::load_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            (self.tree_id, self.metadata.id).into(),
            first_block_handle.offset,
            CachePolicy::Write,
        )?
        else {
            return Ok(None);
        };

        if seqno.is_none() {
            // NOTE: Fastpath for non-seqno reads (which are most common)
            // This avoids setting up a rather expensive block iterator
            // (see explanation for that below)
            // This only really works because sequence numbers are sorted
            // in descending order
            return Ok(block.get_latest(key.as_ref()).cloned());
        }

        let mut reader = Reader::new(
            self.offsets.index_block_ptr,
            self.descriptor_table.clone(),
            (self.tree_id, self.metadata.id).into(),
            self.block_cache.clone(),
            first_block_handle.offset,
            None,
        );
        reader.lo_block_size = block.header.data_length.into();
        reader.lo_block_items = Some(ValueBlockConsumer::with_bounds(
            block,
            &Some(key.into()),
            &None,
        ));
        reader.lo_initialized = true;

        // NOTE: For finding a specific seqno,
        // we need to use a reader
        // because nothing really prevents the version
        // we are searching for to be in the next block
        // after the one our key starts in, or the block after that
        //
        // Example (key:seqno), searching for a:2:
        //
        // [..., a:5, a:4] [a:3, a:2, b: 4, b:3]
        // ^               ^
        // Block A         Block B
        //
        // Based on get_lower_bound_block, "a" is in Block A
        // However, we are searching for A with seqno 2, which
        // unfortunately is in the next block
        for item in reader {
            let item = item?;

            // Just stop iterating once we go past our desired key
            if &*item.key != key {
                return Ok(None);
            }

            if let Some(seqno) = seqno {
                if item.seqno < seqno {
                    return Ok(Some(item));
                }
            } else {
                return Ok(Some(item));
            }
        }

        Ok(None)
    }

    /// Creates an iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> Range {
        Range::new(
            self.offsets.index_block_ptr,
            Arc::clone(&self.descriptor_table),
            (self.tree_id, self.metadata.id).into(),
            Arc::clone(&self.block_cache),
            Arc::clone(&self.block_index),
            (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        )
    }

    /// Creates a ranged iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    pub fn range(&self, range: (Bound<UserKey>, Bound<UserKey>)) -> Range {
        Range::new(
            self.offsets.index_block_ptr,
            Arc::clone(&self.descriptor_table),
            (self.tree_id, self.metadata.id).into(),
            Arc::clone(&self.block_cache),
            Arc::clone(&self.block_index),
            range,
        )
    }

    /// Creates a prefixed iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    pub fn prefix<K: Into<UserKey>>(&self, prefix: K) -> PrefixedReader {
        PrefixedReader::new(
            self.offsets.index_block_ptr,
            Arc::clone(&self.descriptor_table),
            (self.tree_id, self.metadata.id).into(),
            Arc::clone(&self.block_cache),
            Arc::clone(&self.block_index),
            prefix,
        )
    }

    /// Returns the highest sequence number in the segment.
    #[must_use]
    pub fn get_lsn(&self) -> SeqNo {
        self.metadata.seqnos.1
    }

    /// Returns the amount of tombstone markers in the `Segment`.
    #[must_use]
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Checks if a key range is (partially or fully) contained in this segment.
    pub(crate) fn check_key_range_overlap(
        &self,
        bounds: &(Bound<UserKey>, Bound<UserKey>),
    ) -> bool {
        self.metadata.key_range.overlaps_with_bounds(bounds)
    }
}
