// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block;
pub mod block_index;
pub mod file_offsets;
mod forward_reader;
pub mod id;
pub mod inner;
pub mod meta;
pub mod multi_writer;
pub mod range;
pub mod reader;
pub mod scanner;
pub mod trailer;
pub mod value_block;
pub mod value_block_consumer;
pub mod writer;

use crate::{
    block_cache::BlockCache,
    bloom::{BloomFilter, CompositeHash},
    descriptor_table::FileDescriptorTable,
    time::unix_timestamp,
    tree::inner::TreeId,
    value::{InternalValue, SeqNo, UserKey},
};
use block_index::BlockIndexImpl;
use forward_reader::ForwardReader;
use id::GlobalSegmentId;
use inner::Inner;
use meta::SegmentId;
use range::Range;
use scanner::Scanner;
use std::{
    ops::Bound,
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};

#[allow(clippy::module_name_repetitions)]
pub type SegmentInner = Inner;

/// Disk segment (a.k.a. `SSTable`, `SST`, `sorted string table`) that is located on disk
///
/// A segment is an immutable list of key-value pairs, split into compressed blocks.
/// A reference to the block (`block handle`) is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to improve read performance and reduce disk space by removing outdated item versions.
#[doc(alias("sstable", "sst", "sorted string table"))]
#[derive(Clone)]
pub struct Segment(Arc<Inner>);

impl From<Inner> for Segment {
    fn from(value: Inner) -> Self {
        Self(Arc::new(value))
    }
}

impl std::ops::Deref for Segment {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment:{}({})", self.id(), self.metadata.key_range)
    }
}

impl Segment {
    // TODO: in Leveled compaction, compact segments that live very long and have
    // many versions (possibly unnecessary space usage of old, stale versions)
    /// Calculates how many versions per key there are on average.
    #[must_use]
    pub fn version_factor(&self) -> f32 {
        self.metadata.item_count as f32 / self.metadata.key_count as f32
    }

    /// Gets the segment age in nanoseconds.
    #[must_use]
    pub fn age(&self) -> u128 {
        let now = unix_timestamp().as_nanos();
        let created_at = self.metadata.created_at * 1_000;
        now.saturating_sub(created_at)
    }

    /// Gets the global segment ID.
    #[must_use]
    pub fn global_id(&self) -> GlobalSegmentId {
        (self.tree_id, self.id()).into()
    }

    /// Gets the segment ID.
    ///
    /// The segment ID is unique for this tree, but not
    /// across multiple trees, use [`Segment::global_id`] for that.
    #[must_use]
    pub fn id(&self) -> SegmentId {
        self.metadata.id
    }

    pub(crate) fn verify(&self) -> crate::Result<usize> {
        use block::checksum::Checksum;
        use block_index::IndexBlock;
        use value_block::ValueBlock;

        let mut data_block_count = 0;
        let mut broken_count = 0;

        let guard = self
            .descriptor_table
            .access(&self.global_id())?
            .expect("should have gotten file");

        let mut file = guard.file.lock().expect("lock is poisoned");

        // TODO: maybe move to BlockIndexImpl::verify
        match &*self.block_index {
            BlockIndexImpl::Full(block_index) => {
                for handle in block_index.iter() {
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
            }
            BlockIndexImpl::TwoLevel(block_index) => {
                // NOTE: TODO: because of 1.74.0
                #[allow(clippy::explicit_iter_loop)]
                for handle in block_index.top_level_index.iter() {
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
                }
            }
        }

        if data_block_count != self.metadata.data_block_count {
            log::error!(
                "Not all data blocks were visited during verification of disk segment {:?}",
                self.id(),
            );
            broken_count += 1;
        }

        Ok(broken_count)
    }

    pub(crate) fn load_bloom(
        path: &Path,
        ptr: block::offset::BlockOffset,
    ) -> crate::Result<Option<BloomFilter>> {
        Ok(if *ptr > 0 {
            use crate::coding::Decode;
            use std::{
                fs::File,
                io::{Seek, SeekFrom},
            };

            let mut reader = File::open(path)?;
            reader.seek(SeekFrom::Start(*ptr))?;
            Some(BloomFilter::decode_from(&mut reader)?)
        } else {
            None
        })
    }

    /// Tries to recover a segment from a file.
    pub(crate) fn recover(
        file_path: &Path,
        tree_id: TreeId,
        block_cache: Arc<BlockCache>,
        descriptor_table: Arc<FileDescriptorTable>,
        use_full_block_index: bool,
    ) -> crate::Result<Self> {
        use block_index::{full_index::FullBlockIndex, two_level_index::TwoLevelBlockIndex};
        use trailer::SegmentFileTrailer;

        log::debug!("Recovering segment from file {file_path:?}");
        let trailer = SegmentFileTrailer::from_file(file_path)?;

        assert_eq!(
            0, *trailer.offsets.range_tombstones_ptr,
            "Range tombstones not supported"
        );

        log::debug!(
            "Creating block index, with tli_ptr={}",
            trailer.offsets.tli_ptr
        );

        let block_index = if use_full_block_index {
            let block_index =
                FullBlockIndex::from_file(file_path, &trailer.metadata, &trailer.offsets)?;

            BlockIndexImpl::Full(block_index)
        } else {
            let block_index = TwoLevelBlockIndex::from_file(
                file_path,
                &trailer.metadata,
                trailer.offsets.tli_ptr,
                (tree_id, trailer.metadata.id).into(),
                descriptor_table.clone(),
                block_cache.clone(),
            )?;
            BlockIndexImpl::TwoLevel(block_index)
        };

        let bloom_ptr = trailer.offsets.bloom_ptr;

        Ok(Self(Arc::new(Inner {
            path: file_path.into(),

            tree_id,

            descriptor_table,
            metadata: trailer.metadata,
            offsets: trailer.offsets,

            block_index: Arc::new(block_index),
            block_cache,

            bloom_filter: Self::load_bloom(file_path, bloom_ptr)?,

            is_deleted: AtomicBool::default(),
        })))
    }

    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[must_use]
    /// Gets the bloom filter size
    pub fn bloom_filter_size(&self) -> usize {
        self.bloom_filter
            .as_ref()
            .map(super::bloom::BloomFilter::len)
            .unwrap_or_default()
    }

    pub fn get(
        &self,
        key: &[u8],
        seqno: Option<SeqNo>,
        hash: CompositeHash,
    ) -> crate::Result<Option<InternalValue>> {
        if let Some(seqno) = seqno {
            if self.metadata.seqnos.0 >= seqno {
                return Ok(None);
            }
        }

        if let Some(bf) = &self.bloom_filter {
            if !bf.contains_hash(hash) {
                return Ok(None);
            }
        }

        self.point_read(key, seqno)
    }

    fn point_read(&self, key: &[u8], seqno: Option<SeqNo>) -> crate::Result<Option<InternalValue>> {
        use block_index::BlockIndex;
        use value_block::{CachePolicy, ValueBlock};
        use value_block_consumer::ValueBlockConsumer;

        let Some(first_block_handle) = self
            .block_index
            .get_lowest_block_containing_key(key, CachePolicy::Write)?
        else {
            return Ok(None);
        };

        let Some(block) = ValueBlock::load_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            self.global_id(),
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
            return Ok(block.get_latest(key).cloned());
        }

        let mut reader = ForwardReader::new(
            self.offsets.index_block_ptr,
            &self.descriptor_table,
            self.global_id(),
            &self.block_cache,
            first_block_handle,
        );
        reader.lo_block_size = block.header.data_length.into();
        reader.lo_block_items = Some(ValueBlockConsumer::with_bounds(block, Some(key), None));
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
        let mut reader = reader.filter(|x| {
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

        let Some(entry) = reader.next().transpose()? else {
            return Ok(None);
        };

        // NOTE: We are past the searched key, so don't return anything
        if &*entry.key.user_key > key {
            return Ok(None);
        }

        Ok(Some(entry))
    }

    #[must_use]
    pub fn is_key_in_key_range(&self, key: &[u8]) -> bool {
        self.metadata.key_range.contains_key(key)
    }

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

    #[doc(hidden)]
    pub fn scan<P: AsRef<Path>>(&self, base_folder: P) -> crate::Result<Scanner> {
        let segment_file_path = base_folder.as_ref().join(self.metadata.id.to_string());
        let block_count = self.metadata.data_block_count.try_into().expect("oops");
        Scanner::new(&segment_file_path, block_count)
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
            self.global_id(),
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
    #[doc(hidden)]
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns the ratio of tombstone markers in the `Segment`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_ratio(&self) -> f32 {
        self.metadata.tombstone_count as f32 / self.metadata.key_count as f32
    }

    /// Checks if a key range is (partially or fully) contained in this segment.
    pub(crate) fn check_key_range_overlap(
        &self,
        bounds: &(Bound<UserKey>, Bound<UserKey>),
    ) -> bool {
        self.metadata.key_range.overlaps_with_bounds(bounds)
    }
}
