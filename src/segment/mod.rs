// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block;
pub mod block_index;
pub mod file_offsets;
pub mod id;
pub mod level_reader;
pub mod meta;
pub mod multi_writer;
pub mod range;
pub mod reader;
pub mod trailer;
pub mod value_block;
pub mod value_block_consumer;
pub mod writer;

use crate::{
    block_cache::BlockCache,
    descriptor_table::FileDescriptorTable,
    mvcc_stream::MvccStream,
    segment::{reader::Reader, value_block_consumer::ValueBlockConsumer},
    tree::inner::TreeId,
    value::{InternalValue, SeqNo, UserKey},
    ValueType,
};
use block::checksum::Checksum;
use block_index::{two_level_index::TwoLevelBlockIndex, BlockIndexImpl};
use file_offsets::FileOffsets;
use range::Range;
use std::{ops::Bound, path::Path, sync::Arc};

#[cfg(feature = "bloom")]
use crate::bloom::{BloomFilter, CompositeHash};

/// Disk segment (a.k.a. `SSTable`, `SST`, `sorted string table`) that is located on disk
///
/// A segment is an immutable list of key-value pairs, split into compressed blocks.
/// A reference to the block (`block handle`) is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to improve read performance and reduce disk space by removing outdated item versions.
#[doc(alias("sstable", "sst", "sorted string table"))]
pub struct Segment {
    pub(crate) tree_id: TreeId,

    #[doc(hidden)]
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Segment metadata object
    #[doc(hidden)]
    pub metadata: meta::Metadata,

    pub(crate) offsets: FileOffsets,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    #[doc(hidden)]
    pub block_index: Arc<BlockIndexImpl>,

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
        write!(
            f,
            "Segment:{}({})",
            self.metadata.id, self.metadata.key_range
        )
    }
}

impl Segment {
    pub(crate) fn verify(&self) -> crate::Result<usize> {
        use block_index::IndexBlock;
        use value_block::ValueBlock;

        let mut data_block_count = 0;
        let mut broken_count = 0;

        let guard = self
            .descriptor_table
            .access(&(self.tree_id, self.metadata.id).into())?
            .expect("should have gotten file");

        let mut file = guard.file.lock().expect("lock is poisoned");

        todo!();
        /* // NOTE: TODO: because of 1.74.0
        #[allow(clippy::explicit_iter_loop)]
        for handle in self.block_index.top_level_index.iter() {
            let block = match IndexBlock::from_file(&mut *file, handle.offset) {
                Ok(v) => v,
                Err(e) => {
                    log::error!(
                        "index block {handle:?} could not be loaded, it is probably corrupted: {e:?}"
                    );
                    broken_count += 1;
                    continue;
                }
            };

            for handle in &*block.items {
                let value_block = match ValueBlock::from_file(&mut *file, handle.offset) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!(
                            "data block {handle:?} could not be loaded, it is probably corrupted: {e:?}"
                        );
                        broken_count += 1;
                        data_block_count += 1;
                        continue;
                    }
                };

                let (_, data) = ValueBlock::to_bytes_compressed(
                    &value_block.items,
                    value_block.header.previous_block_offset,
                    value_block.header.compression,
                )?;
                let actual_checksum = Checksum::from_bytes(&data);

                if value_block.header.checksum != actual_checksum {
                    log::error!("{handle:?} is corrupted, invalid checksum value");
                    broken_count += 1;
                }

                data_block_count += 1;

                if data_block_count % 1_000 == 0 {
                    log::debug!("Checked {data_block_count} data blocks");
                }
            }
        } */

        assert_eq!(
            data_block_count, self.metadata.data_block_count,
            "not all data blocks were visited"
        );

        Ok(broken_count)
    }

    // TODO: need to give recovery a flag to choose which block index to load

    /// Tries to recover a segment from a file.
    pub(crate) fn recover<P: AsRef<Path>>(
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
        let block_index = TwoLevelBlockIndex::from_file(
            file_path,
            &trailer.metadata,
            &trailer.offsets,
            (tree_id, trailer.metadata.id).into(),
            descriptor_table.clone(),
            block_cache.clone(),
        )?;
        let block_index = BlockIndexImpl::TwoLevel(block_index);

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
                use crate::coding::Decode;
                use std::{
                    fs::File,
                    io::{Seek, SeekFrom},
                };

                assert!(bloom_ptr > 0, "can not find bloom filter block");

                let mut reader = File::open(file_path)?;
                reader.seek(SeekFrom::Start(bloom_ptr))?;
                BloomFilter::decode_from(&mut reader)?
            },
        })
    }

    #[cfg(feature = "bloom")]
    #[must_use]
    /// Gets the bloom filter size
    pub fn bloom_filter_size(&self) -> usize {
        self.bloom_filter.len()
    }

    #[cfg(feature = "bloom")]
    pub fn get_with_hash<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
        hash: CompositeHash,
    ) -> crate::Result<Option<InternalValue>> {
        if !self.bloom_filter.contains_hash(hash) {
            return Ok(None);
        }

        if let Some(seqno) = seqno {
            if self.metadata.seqnos.0 >= seqno {
                return Ok(None);
            }
        }

        if !self.metadata.key_range.contains_key(&key) {
            return Ok(None);
        }

        self.point_read(key, seqno)
    }

    fn point_read<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<InternalValue>> {
        use block_index::BlockIndex;
        use value_block::{CachePolicy, ValueBlock};

        let key = key.as_ref();

        let Some(first_block_handle) = self
            .block_index
            .get_lowest_block_containing_key(key, CachePolicy::Write)?
        else {
            return Ok(None);
        };

        let Some(block) = ValueBlock::load_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            (self.tree_id, self.metadata.id).into(),
            first_block_handle,
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
            let Some(latest) = block.get_latest(key.as_ref()).cloned() else {
                return Ok(None);
            };

            if latest.key.value_type == ValueType::WeakTombstone {
                // NOTE: Continue in slow path
            } else {
                return Ok(Some(latest));
            }
        }

        let mut reader = Reader::new(
            self.offsets.index_block_ptr,
            self.descriptor_table.clone(),
            (self.tree_id, self.metadata.id).into(),
            self.block_cache.clone(),
            first_block_handle,
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
        //
        // Also because of weak tombstones, we may have to look further than the first item we encounter
        let reader = reader.filter(|x| {
            match x {
                Ok(entry) => {
                    // Check for seqno if needed
                    if let Some(seqno) = seqno {
                        entry.key.seqno < seqno
                    } else {
                        true
                    }
                }
                Err(_) => true,
            }
        });

        let Some(entry) = MvccStream::new(reader).next().transpose()? else {
            return Ok(None);
        };

        // NOTE: We are past the searched key, so don't return anything
        if &*entry.key.user_key > key {
            return Ok(None);
        }

        Ok(Some(entry))
    }

    // NOTE: Clippy false positive
    #[allow(unused)]
    /// Retrieves an item from the segment.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(crate) fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<InternalValue>> {
        let key = key.as_ref();

        if let Some(seqno) = seqno {
            if self.metadata.seqnos.0 >= seqno {
                return Ok(None);
            }
        }

        if !self.metadata.key_range.contains_key(key) {
            return Ok(None);
        }

        #[cfg(feature = "bloom")]
        {
            debug_assert!(false, "Use Segment::get_with_hash instead");

            if !self.bloom_filter.contains(key) {
                return Ok(None);
            }
        }

        self.point_read(key, seqno)
    }

    // TODO: move segment tests into module, then make pub(crate)

    /// Creates an iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    #[doc(hidden)]
    pub fn iter(&self) -> Range {
        self.range((std::ops::Bound::Unbounded, std::ops::Bound::Unbounded))
    }

    /// Creates a ranged iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    pub(crate) fn range(&self, range: (Bound<UserKey>, Bound<UserKey>)) -> Range {
        Range::new(
            self.offsets.index_block_ptr,
            self.descriptor_table.clone(),
            (self.tree_id, self.metadata.id).into(),
            self.block_cache.clone(),
            self.block_index.clone(),
            range,
        )
    }

    /// Returns the highest sequence number in the segment.
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
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
