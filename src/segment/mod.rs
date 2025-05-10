// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block;
mod block_index;
pub(crate) mod data_block;
pub mod filter;
mod id;
mod index_block;
mod inner;
mod meta;
pub(crate) mod multi_writer;
mod regions;
mod scanner;
mod trailer;
pub(crate) mod util;
mod writer;

pub use block::{Block, BlockOffset, Checksum};
pub use data_block::DataBlock;
pub use id::{GlobalSegmentId, SegmentId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
use regions::ParsedRegions;
pub use scanner::Scanner;
pub use writer::Writer;

use crate::{
    cache::Cache, descriptor_table::DescriptorTable, InternalValue, SeqNo, TreeId, UserKey,
};
use block_index::{BlockIndex, BlockIndexImpl, FullBlockIndex};
use filter::standard_bloom::{CompositeHash, StandardBloomFilter};
use inner::Inner;
use meta::ParsedMeta;
use std::{
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{atomic::AtomicBool, Arc},
};

// todo

// TODO: segment iter:
// TODO:    we only need to truncate items from blocks that are not the first and last block
// TODO:    because any block inbetween must (trivially) only contain relevant items

// TODO: in Leveled compaction, compact segments that live very long and have
// many versions (possibly unnecessary space usage of old, stale versions)

// TODO: move into module
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    /// Read cached blocks, but do not change cache
    Read,

    /// Read cached blocks, and update cache
    Write,
}

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
    /// Gets the global segment ID.
    #[must_use]
    pub fn global_id(&self) -> GlobalSegmentId {
        (self.tree_id, self.id()).into()
    }

    #[must_use]
    pub fn pinned_bloom_filter_size(&self) -> usize {
        self.pinned_filter_block
            .as_ref()
            .map(Block::size)
            .unwrap_or_default()
    }

    /// Gets the segment ID.
    ///
    /// The segment ID is unique for this tree, but not
    /// across multiple trees, use [`Segment::global_id`] for that.
    #[must_use]
    pub fn id(&self) -> SegmentId {
        self.metadata.id
    }

    fn load_block(
        &self,
        handle: &BlockHandle,
        compression: CompressionType,
    ) -> crate::Result<Block> {
        let id = self.global_id();

        if let Some(block) = self.cache.get_block(id, handle.offset()) {
            return Ok(block);
        }

        let cached_fd = self.descriptor_table.access_for_table(&id);
        let fd_cache_miss = cached_fd.is_none();

        let fd = if let Some(fd) = cached_fd {
            fd
        } else {
            Arc::new(std::fs::File::open(&self.path)?)
        };

        let block = Block::from_file(&fd, handle.offset(), handle.size(), compression)?;

        let id = self.global_id();

        // Cache FD
        if fd_cache_miss {
            self.descriptor_table.insert_for_table(id, fd);
        }

        self.cache.insert_block(id, handle.offset(), block.clone());

        Ok(block)
    }

    fn load_data_block(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        self.load_block(handle, self.metadata.data_block_compression)
            .map(DataBlock::new)
    }

    pub fn get(
        &self,
        key: &[u8],
        seqno: Option<SeqNo>,
        key_hash: CompositeHash,
    ) -> crate::Result<Option<InternalValue>> {
        if let Some(seqno) = seqno {
            if self.metadata.seqnos.0 >= seqno {
                return Ok(None);
            }
        }

        if let Some(block) = &self.pinned_filter_block {
            let filter = StandardBloomFilterReader::new(&block.data)?;

            if !filter.contains_hash(key_hash) {
                return Ok(None);
            }
        } else if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(filter_block_handle, CompressionType::None)?;
            let filter = StandardBloomFilterReader::new(&block.data)?;

            if !filter.contains_hash(key_hash) {
                return Ok(None);
            }
        }

        self.point_read(key, seqno)
    }

    fn point_read(&self, key: &[u8], seqno: Option<SeqNo>) -> crate::Result<Option<InternalValue>> {
        match seqno {
            None => {
                let Some(block_handle) = self
                    .block_index
                    .get_lowest_block_containing_key(key, CachePolicy::Write)?
                else {
                    return Ok(None);
                };

                let block = self.load_data_block(block_handle.as_ref())?;

                // NOTE: Fastpath for non-seqno reads
                return Ok(block.point_read(key, None));
            }
            Some(seqno) => {
                let BlockIndexImpl::Full(block_index) = &*self.block_index else {
                    todo!();
                };

                let Some(iter) = block_index.forward_reader(key) else {
                    return Ok(None);
                };

                for block_handle in iter {
                    // TODO: can this ever happen...?
                    if block_handle.end_key() < &key {
                        return Ok(None);
                    }

                    let block = self.load_data_block(block_handle.as_ref())?;

                    if let Some(item) = block.point_read(key, Some(seqno)) {
                        return Ok(Some(item));
                    }

                    // NOTE: If the last block key is higher than ours,
                    // our key cannot be in the next block
                    if block_handle.end_key() > &key {
                        return Ok(None);
                    }
                }
            }
        }

        Ok(None)
    }

    /// Creates a scanner over the `Segment`.
    ///
    /// The scanner is ĺogically the same as a normal iter(),
    /// however it uses its own file descriptor, does not look into the block cache
    /// and uses buffered I/O.
    ///
    /// Used for compactions and thus not available to a user.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn scan(&self) -> crate::Result<Scanner> {
        let block_count = self
            .metadata
            .data_block_count
            .try_into()
            .expect("data block count should fit");

        Scanner::new(
            &self.path,
            block_count,
            self.metadata.data_block_compression,
        )
    }

    /// Creates an iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    #[doc(hidden)]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + '_ {
        // self.range(..)
        todo!();

        std::iter::empty()
    }

    /// Creates a ranged iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    #[doc(hidden)]
    pub fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + '_ {
        // self.range((std::ops::Bound::Unbounded, std::ops::Bound::Unbounded))
        todo!();

        std::iter::empty()
    }

    /// Tries to recover a segment from a file.
    pub fn recover(
        file_path: &Path,
        tree_id: TreeId,
        cache: Arc<Cache>,
        descriptor_table: Arc<DescriptorTable>,
        pin_filter: bool,
    ) -> crate::Result<Self> {
        use trailer::Trailer;

        log::debug!("Recovering segment from file {file_path:?}");
        let mut file = std::fs::File::open(file_path)?;

        let trailer = Trailer::from_file(&mut file)?;
        log::trace!("Got trailer: {trailer:#?}");

        log::debug!(
            "Reading regions block, with region_ptr={:?}",
            trailer.regions_block_handle(),
        );
        let regions = ParsedRegions::load_with_handle(&file, trailer.regions_block_handle())?;

        log::debug!("Reading meta block, with meta_ptr={:?}", regions.metadata);
        let metadata = ParsedMeta::load_with_handle(&file, &regions.metadata)?;

        let tli_block = {
            log::debug!("Reading TLI block, with tli_ptr={:?}", regions.tli);

            let block = Block::from_file(
                &file,
                regions.tli.offset(),
                regions.tli.size(),
                metadata.data_block_compression, // TODO: index blocks may get their own compression level
            )?;

            IndexBlock::new(block)
        };

        let block_index = if let Some(index_block_handle) = regions.index {
            log::debug!(
                "Creating partitioned block index, with tli_ptr={:?}, index_block_ptr={index_block_handle:?}",
                regions.tli,
            );

            unimplemented!("partitioned index is not supported yet");

            // BlockIndexImpl::TwoLevel(tli_block, todo!())
        } else {
            log::debug!("Creating full block index, with tli_ptr={:?}", regions.tli);
            BlockIndexImpl::Full(FullBlockIndex::new(tli_block))
        };

        // TODO: load FilterBlock
        let pinned_filter_block = if pin_filter {
            regions
                .filter
                .map(|filter_handle| {
                    log::debug!(
                        "Loading and pinning filter block, with filter_ptr={filter_handle:?}"
                    );

                    Block::from_file(
                        &file,
                        filter_handle.offset(),
                        filter_handle.size(),
                        crate::CompressionType::None, // NOTE: We never write a filter block with compression
                    )
                })
                .transpose()?
        } else {
            None
        };

        // NOTE: We already have a file descriptor open, so let's just cache it immediately
        descriptor_table.insert_for_table((tree_id, metadata.id).into(), Arc::new(file));

        let segment = Self(Arc::new(Inner {
            path: file_path.into(),
            tree_id,

            metadata,
            regions,

            cache,

            descriptor_table,

            block_index: Arc::new(block_index),

            pinned_filter_block,

            is_deleted: AtomicBool::default(),
        }));

        Ok(segment)
    }

    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[must_use]
    pub fn is_key_in_key_range(&self, key: &[u8]) -> bool {
        self.metadata.key_range.contains_key(key)
    }

    /// Checks if a key range is (partially or fully) contained in this segment.
    pub(crate) fn check_key_range_overlap(
        &self,
        bounds: &(Bound<UserKey>, Bound<UserKey>),
    ) -> bool {
        self.metadata.key_range.overlaps_with_bounds(bounds)
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
        todo!()

        //  self.metadata.tombstone_count
    }

    /// Returns the ratio of tombstone markers in the `Segment`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_ratio(&self) -> f32 {
        todo!()

        //  self.metadata.tombstone_count as f32 / self.metadata.key_count as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_segment_recover() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("segment");

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?;
            writer.write(crate::InternalValue::from_components(
                b"abc",
                b"asdasdasd",
                3,
                crate::ValueType::Value,
            ))?;
            let _trailer = writer.finish()?;
        }

        {
            let segment = Segment::recover(
                &file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(1, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                None,
                segment.get(
                    b"def",
                    None,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
                )?
            );

            assert_eq!(
                segment.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"abc".into())),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_segment_scan() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("segment");

        let items = [
            crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
        ];

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

            for item in items.iter().cloned() {
                writer.write(item)?;
            }

            let _trailer = writer.finish()?;
        }

        {
            let segment = Segment::recover(
                &file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(3, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                b"def",
                &*segment
                    .get(
                        b"def",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                b"xyz",
                &*segment
                    .get(
                        b"xyz",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"xyz")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                None,
                segment.get(
                    b"____",
                    None,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
                )?
            );

            assert_eq!(items, &*segment.scan()?.flatten().collect::<Vec<_>>());

            assert_eq!(
                segment.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"xyz".into())),
            );
        }

        Ok(())
    }

    // TODO: when using stats cfg feature: check filter hits += 1
    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_segment_unpinned_filter() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("segment");

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?;
            writer.write(crate::InternalValue::from_components(
                b"abc",
                b"asdasdasd",
                3,
                crate::ValueType::Value,
            ))?;
            let _trailer = writer.finish()?;
        }

        {
            let segment = Segment::recover(
                &file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(1, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        None,
                        crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                    )?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                None,
                segment.get(
                    b"def",
                    None,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
                )?
            );

            assert_eq!(
                segment.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"abc".into())),
            );
        }

        Ok(())
    }
}
