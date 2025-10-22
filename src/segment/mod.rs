// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod block;
pub(crate) mod block_index;
pub mod data_block;
pub mod filter;
mod id;
mod index_block;
mod inner;
mod iter;
mod meta;
pub(crate) mod multi_writer;
mod regions;
mod scanner;
pub mod util;
pub mod writer;

pub use block::{Block, BlockOffset, Checksum};
pub use data_block::DataBlock;
pub use id::{GlobalSegmentId, SegmentId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
pub use scanner::Scanner;
pub use writer::Writer;

use crate::{
    cache::Cache,
    descriptor_table::DescriptorTable,
    segment::{
        block::BlockType,
        block_index::{BlockIndex, FullBlockIndex, TwoLevelBlockIndex, VolatileBlockIndex},
        regions::ParsedRegions,
        writer::LinkedFile,
    },
    CompressionType, InternalValue, SeqNo, TreeId, UserKey,
};
use block_index::BlockIndexImpl;
use inner::Inner;
use iter::Iter;
use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::Arc,
};
use util::load_block;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

// TODO: segment iter:
// TODO:    we only need to truncate items from blocks that are not the first and last block
// TODO:    because any block inbetween must (trivially) only contain relevant items

// TODO: in Leveled compaction, compact segments that live very long and have
// many versions (possibly unnecessary space usage of old, stale versions)

#[allow(clippy::module_name_repetitions)]
pub type SegmentInner = Inner;

/// A disk segment (a.k.a. `Table`, `SSTable`, `SST`, `sorted string table`) that is located on disk
///
/// A table is an immutable list of key-value pairs, split into compressed blocks.
/// A reference to the block (`block handle`) is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Tables can be merged together to improve read performance and free unneeded disk space by removing outdated item versions.
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
        write!(f, "Table:{}({:?})", self.id(), self.metadata.key_range)
    }
}

impl Segment {
    pub fn referenced_blob_bytes(&self) -> crate::Result<u64> {
        Ok(self
            .list_blob_file_references()?
            .iter()
            .map(|bf| bf.iter().map(|f| f.on_disk_bytes).sum::<u64>())
            .sum::<u64>())
    }

    pub fn list_blob_file_references(&self) -> crate::Result<Option<Vec<LinkedFile>>> {
        use byteorder::{ReadBytesExt, LE};

        Ok(if let Some(handle) = &self.regions.linked_blob_files {
            let reader = File::open(&*self.path)?;
            let mut reader = BufReader::new(reader);
            reader.seek(std::io::SeekFrom::Start(*handle.offset()))?;
            let mut reader = reader.take(u64::from(handle.size()));

            let mut blob_files = vec![];

            let len = reader.read_u32::<LE>()?;

            for _ in 0..len {
                let blob_file_id = reader.read_u64::<LE>()?;
                let len = reader.read_u64::<LE>()?;
                let bytes = reader.read_u64::<LE>()?;
                let on_disk_bytes = reader.read_u64::<LE>()?;

                blob_files.push(LinkedFile {
                    blob_file_id,
                    bytes,
                    len: len as usize,
                    on_disk_bytes,
                });
            }

            Some(blob_files)
        } else {
            None
        })
    }

    /// Gets the global segment ID.
    #[must_use]
    pub fn global_id(&self) -> GlobalSegmentId {
        (self.tree_id, self.id()).into()
    }

    #[must_use]
    pub fn filter_size(&self) -> usize {
        unimplemented!()
    }

    #[must_use]
    pub fn pinned_filter_size(&self) -> usize {
        self.pinned_filter_block
            .as_ref()
            .map(Block::size)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn pinned_block_index_size(&self) -> usize {
        match &*self.block_index {
            BlockIndexImpl::Full(full_block_index) => full_block_index.inner().inner.size(),
            BlockIndexImpl::VolatileFull(_) => 0,
            BlockIndexImpl::TwoLevel(two_level_block_index) => {
                two_level_block_index.top_level_index.inner.size()
            }
        }
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
        block_type: BlockType,
        compression: CompressionType,
    ) -> crate::Result<Block> {
        load_block(
            self.global_id(),
            &self.path,
            &self.descriptor_table,
            &self.cache,
            handle,
            block_type,
            compression,
            #[cfg(feature = "metrics")]
            &self.metrics,
        )
    }

    fn load_data_block(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        self.load_block(
            handle,
            BlockType::Data,
            self.metadata.data_block_compression,
        )
        .map(DataBlock::new)
    }

    /// Returns the (possibly compressed) file size.
    pub(crate) fn file_size(&self) -> u64 {
        self.metadata.file_size
    }

    pub fn get(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<InternalValue>> {
        use filter::standard_bloom::StandardBloomFilterReader;
        #[cfg(feature = "metrics")]
        use std::sync::atomic::Ordering::Relaxed;

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        if let Some(block) = &self.pinned_filter_block {
            let filter = StandardBloomFilterReader::new(&block.data)?;

            #[cfg(feature = "metrics")]
            self.metrics.filter_queries.fetch_add(1, Relaxed);

            if !filter.contains_hash(key_hash) {
                #[cfg(feature = "metrics")]
                self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);

                return Ok(None);
            }
        } else if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None, // NOTE: We never write a filter block with compression
            )?;
            let filter = StandardBloomFilterReader::new(&block.data)?;

            #[cfg(feature = "metrics")]
            self.metrics.filter_queries.fetch_add(1, Relaxed);

            if !filter.contains_hash(key_hash) {
                #[cfg(feature = "metrics")]
                self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);

                return Ok(None);
            }
        }

        self.point_read(key, seqno)
    }

    // TODO: maybe we can skip Fuse costs of the user key
    // TODO: because we just want to return the value
    // TODO: we would need to return something like ValueType + Value
    // TODO: so the caller can decide whether to return the value or not
    fn point_read(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        let Some(iter) = self.block_index.forward_reader(key) else {
            return Ok(None);
        };

        for block_handle in iter {
            let block_handle = block_handle?;

            // TODO: can this ever happen...?
            if block_handle.end_key() < &key {
                return Ok(None);
            }

            let block = self.load_data_block(block_handle.as_ref())?;

            if let Some(item) = block.point_read(key, seqno) {
                return Ok(Some(item));
            }

            // NOTE: If the last block key is higher than ours,
            // our key cannot be in the next block
            if block_handle.end_key() > &key {
                return Ok(None);
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
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> {
        self.range(..)
    }

    /// Creates a ranged iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    #[doc(hidden)]
    pub fn range<R: RangeBounds<UserKey>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> {
        use crate::fallible_clipping_iter::FallibleClippingIter;

        let index_iter = self.block_index.iter();

        let mut iter = Iter::new(
            self.global_id(),
            self.path.clone(),
            index_iter,
            self.descriptor_table.clone(),
            self.cache.clone(),
            self.metadata.data_block_compression,
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        );

        match range.start_bound() {
            Bound::Excluded(key) | Bound::Included(key) => {
                iter.set_lower_bound(key.clone());
            }
            Bound::Unbounded => {}
        }

        match range.end_bound() {
            Bound::Excluded(key) | Bound::Included(key) => {
                iter.set_upper_bound(key.clone());
            }
            Bound::Unbounded => {}
        }

        FallibleClippingIter::new(iter, range)
    }

    fn read_tli(
        regions: &ParsedRegions,
        file: &File,
        compression: CompressionType,
    ) -> crate::Result<IndexBlock> {
        log::trace!("Reading TLI block, with tli_ptr={:?}", regions.tli);

        let block = Block::from_file(file, regions.tli, compression)?;

        if block.header.block_type != BlockType::Index {
            return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            ))));
        }

        Ok(IndexBlock::new(block))
    }

    /// Tries to recover a segment from a file.
    pub fn recover(
        file_path: PathBuf,
        tree_id: TreeId,
        cache: Arc<Cache>,
        descriptor_table: Arc<DescriptorTable>,
        pin_filter: bool,
        pin_index: bool,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> crate::Result<Self> {
        use meta::ParsedMeta;
        use regions::ParsedRegions;
        use std::sync::atomic::AtomicBool;

        log::debug!("Recovering segment from file {}", file_path.display());
        let mut file = std::fs::File::open(&file_path)?;

        let trailer = sfa::Reader::from_reader(&mut file)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

        log::trace!("Reading meta block, with meta_ptr={:?}", regions.metadata);
        let metadata = ParsedMeta::load_with_handle(&file, &regions.metadata)?;

        let block_index = if regions.index.is_some() {
            log::trace!(
                "Creating partitioned block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(&regions, &file, metadata.index_block_compression)?;
            BlockIndexImpl::TwoLevel(TwoLevelBlockIndex {
                top_level_index: block,
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                descriptor_table: descriptor_table.clone(),
                path: file_path.clone(),
                segment_id: (tree_id, metadata.id).into(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        } else if pin_index {
            log::trace!(
                "Creating pinned, full block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(&regions, &file, metadata.index_block_compression)?;
            BlockIndexImpl::Full(FullBlockIndex::new(block))
        } else {
            log::trace!("Creating volatile, full block index");

            BlockIndexImpl::VolatileFull(VolatileBlockIndex {
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                descriptor_table: descriptor_table.clone(),
                handle: regions.tli,
                path: file_path.clone(),
                segment_id: (tree_id, metadata.id).into(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        };

        // TODO: FilterBlock newtype
        let pinned_filter_block = if pin_filter {
            regions
                .filter
                .map(|filter_handle| {
                    log::debug!(
                        "Loading and pinning filter block, with filter_ptr={filter_handle:?}"
                    );

                    Block::from_file(
                        &file,
                        filter_handle,
                        crate::CompressionType::None, // NOTE: We never write a filter block with compression
                    )
                    .and_then(|block| {
                        if block.header.block_type == BlockType::Filter {
                            Ok(block)
                        } else {
                            Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                                "BlockType",
                                block.header.block_type.into(),
                            ))))
                        }
                    })
                })
                .transpose()?
        } else {
            None
        };

        log::trace!("Table #{} recovered", metadata.id);

        let segment = Self(Arc::new(Inner {
            path: Arc::new(file_path),
            tree_id,

            metadata,
            regions,

            cache,

            descriptor_table,

            block_index: Arc::new(block_index),

            pinned_filter_block,

            is_deleted: AtomicBool::default(),

            #[cfg(feature = "metrics")]
            metrics,
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
    pub(crate) fn check_key_range_overlap(&self, bounds: &(Bound<&[u8]>, Bound<&[u8]>)) -> bool {
        self.metadata.key_range.overlaps_with_bounds(bounds)
    }

    /// Returns the highest sequence number in the segment.
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
        self.metadata.seqnos.1
    }

    /// Returns the number of tombstone markers in the `Segment`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns the number of weak (single delete) tombstones in the `Segment`.
    #[must_use]
    #[doc(hidden)]
    pub fn weak_tombstone_count(&self) -> u64 {
        self.metadata.weak_tombstone_count
    }

    /// Returns the number of value entries reclaimable once weak tombstones can be GC'd.
    #[must_use]
    #[doc(hidden)]
    pub fn weak_tombstone_reclaimable(&self) -> u64 {
        self.metadata.weak_tombstone_reclaimable
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
    fn segment_recover() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(1, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        SeqNo::MAX,
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
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
                )?
            );
            assert_eq!(
                None,
                segment.get(
                    b"____",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
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
    fn segment_volatile_index_point_read() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(1, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 2 because we use a full index
            assert!(segment.regions.index.is_none(), "should use full index");
            assert_eq!(
                0,
                segment.pinned_block_index_size(),
                "should not pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        SeqNo::MAX,
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
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
                )?
            );
            assert_eq!(
                None,
                segment.get(
                    b"____",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
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
    fn segment_partitioned_index_point_read() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_partitioned_index();

            writer.write(crate::InternalValue::from_components(
                b"abc",
                b"asdasdasd",
                3,
                crate::ValueType::Value,
            ))?;

            let _trailer = writer.finish()?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(1, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(2, segment.metadata.index_block_count); // 2 because we use a full index, + 1 2nd level index block
            assert!(segment.regions.index.is_some(), "should use 2-tier index");
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin TLI block",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        SeqNo::MAX,
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
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
                )?
            );
            assert_eq!(
                None,
                segment.get(
                    b"____",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
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
    fn segment_scan() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(3, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(items, &*segment.scan()?.flatten().collect::<Vec<_>>());

            assert_eq!(
                segment.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"xyz".into())),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_iter_simple() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(3, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(items, &*segment.iter().flatten().collect::<Vec<_>>());
            assert_eq!(
                items.iter().rev().cloned().collect::<Vec<_>>(),
                &*segment.iter().rev().flatten().collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_range_simple() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(3, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                items.iter().skip(1).cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..)
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..)
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_range_simple_volatile_index() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                false,
                #[cfg(feature = "metrics")]
                metrics,
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
                0,
                segment.pinned_block_index_size(),
                "should not pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                items.iter().skip(1).cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..)
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..)
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_range_ping_pong() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

        let items = (0u64..10)
            .map(|i| {
                InternalValue::from_components(i.to_be_bytes(), "", 0, crate::ValueType::Value)
            })
            .collect::<Vec<_>>();

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

            for item in items.iter().cloned() {
                writer.write(item)?;
            }

            let _trailer = writer.finish()?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(10, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            let mut iter = segment
                .range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()));

            let mut count = 0;

            for x in 0.. {
                if x % 2 == 0 {
                    let Some(_) = iter.next() else {
                        break;
                    };

                    count += 1;
                } else {
                    let Some(_) = iter.next_back() else {
                        break;
                    };

                    count += 1;
                }
            }

            assert_eq!(5, count);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_range_ping_pong_partitioned() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

        let items = (0u64..10)
            .map(|i| {
                InternalValue::from_components(i.to_be_bytes(), "", 0, crate::ValueType::Value)
            })
            .collect::<Vec<_>>();

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_partitioned_index();

            for item in items.iter().cloned() {
                writer.write(item)?;
            }

            let _trailer = writer.finish()?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(10, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(2, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_some(),
                "should use full index, so 2nd level index blocks should exist",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin top-level index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            let mut iter = segment
                .range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()));

            let mut count = 0;

            for x in 0.. {
                if x % 2 == 0 {
                    let Some(_) = iter.next() else {
                        break;
                    };

                    count += 1;
                } else {
                    let Some(_) = iter.next_back() else {
                        break;
                    };

                    count += 1;
                }
            }

            assert_eq!(5, count);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_range_multiple_data_blocks() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

        let items = [
            crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
        ];

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_data_block_size(1);

            for item in items.iter().cloned() {
                writer.write(item)?;
            }

            let _trailer = writer.finish()?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(5, segment.metadata.item_count);
            assert_eq!(5, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items
                    .iter()
                    .skip(1)
                    .take(3)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_range_multiple_data_blocks_partitioned_index() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

        let items = [
            crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
            crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
        ];

        {
            let mut writer = crate::segment::Writer::new(file.clone(), 5)?
                .use_data_block_size(1)
                .use_partitioned_index();

            for item in items.iter().cloned() {
                writer.write(item)?;
            }

            let _trailer = writer.finish()?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(5, segment.metadata.item_count);
            assert_eq!(5, segment.metadata.data_block_count);
            assert_eq!(2, segment.metadata.index_block_count); // 1 because we use a full index and a 2nd level index block
            assert!(
                segment.regions.index.is_some(),
                "should use partitioned index, so 2nd level index blocks should exist",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin top-level index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            assert_eq!(
                items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items
                    .iter()
                    .skip(1)
                    .take(3)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    // TODO: when using stats cfg feature: check filter hits += 1
    #[test]
    #[allow(clippy::unwrap_used)]
    fn segment_unpinned_filter() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("table");

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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(5, segment.id());
            assert_eq!(1, segment.metadata.item_count);
            assert_eq!(1, segment.metadata.data_block_count);
            assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
            assert!(
                segment.regions.index.is_none(),
                "should use full index, so only TLI exists",
            );
            assert!(
                segment.pinned_block_index_size() > 0,
                "should pin block index",
            );
            assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

            assert_eq!(
                b"abc",
                &*segment
                    .get(
                        b"abc",
                        SeqNo::MAX,
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
                    SeqNo::MAX,
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
