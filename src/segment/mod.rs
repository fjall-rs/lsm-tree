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
mod trailer;
pub mod util;
mod writer;

pub use block::{Block, BlockOffset, Checksum};
pub use data_block::DataBlock;
pub use id::{GlobalSegmentId, SegmentId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
pub use scanner::Scanner;
pub use writer::Writer;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

use crate::{
    cache::Cache,
    descriptor_table::DescriptorTable,
    prefix::{PrefixExtractor, SharedPrefixExtractor},
    segment::block::{BlockType, ParsedItem},
    CompressionType, InternalValue, SeqNo, TreeId, UserKey,
};
use block_index::BlockIndexImpl;
use inner::Inner;
use iter::Iter;
use std::{
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::Arc,
};
use util::load_block;

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
        write!(f, "Segment:{}({:?})", self.id(), self.metadata.key_range)
    }
}

impl Segment {
    /// Gets the global segment ID.
    #[must_use]
    pub fn global_id(&self) -> GlobalSegmentId {
        (self.tree_id, self.id()).into()
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
            BlockIndexImpl::VolatileFull => 0,
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

            let may_contain = if let Some(ref extractor) = self.prefix_extractor {
                if self.prefix_extractor_compatible {
                    #[cfg(feature = "metrics")]
                    self.metrics.filter_queries.fetch_add(1, Relaxed);

                    // If prefix extractor is configured and compatible, use prefix-based filtering
                    // None means out-of-domain - these keys bypass filter
                    filter
                        .contains_prefix(key, extractor.as_ref())
                        .unwrap_or(true)
                } else {
                    // Extractor exists but is incompatible - disable filter entirely
                    true
                }
            } else {
                // No prefix extractor configured now
                if self.prefix_extractor_compatible {
                    #[cfg(feature = "metrics")]
                    self.metrics.filter_queries.fetch_add(1, Relaxed);

                    // Segment was also created without prefix extractor - use standard hash-based filtering
                    filter.contains_hash(key_hash)
                } else {
                    // Segment was created with prefix extractor, but none configured now - disable filter
                    true
                }
            };

            if !may_contain {
                #[cfg(feature = "metrics")]
                self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);

                return Ok(None);
            }
        } else if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None,
            )?;
            let filter = StandardBloomFilterReader::new(&block.data)?;

            let may_contain = if let Some(ref extractor) = self.prefix_extractor {
                if self.prefix_extractor_compatible {
                    #[cfg(feature = "metrics")]
                    self.metrics.filter_queries.fetch_add(1, Relaxed);

                    // If prefix extractor is configured and compatible, use prefix-based filtering
                    // None means out-of-domain - these keys bypass filter
                    filter
                        .contains_prefix(key, extractor.as_ref())
                        .unwrap_or(true)
                } else {
                    // Extractor exists but is incompatible - disable filter entirely
                    true
                }
            } else {
                // No prefix extractor configured now
                if self.prefix_extractor_compatible {
                    #[cfg(feature = "metrics")]
                    self.metrics.filter_queries.fetch_add(1, Relaxed);

                    // Segment was also created without prefix extractor - use standard hash-based filtering
                    filter.contains_hash(key_hash)
                } else {
                    // Segment was created with prefix extractor, but none configured now - disable filter
                    true
                }
            };

            if !may_contain {
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
        // TODO: enum_dispatch BlockIndex::iter
        let index_block = match &*self.block_index {
            BlockIndexImpl::Full(index) => index.inner(),
            BlockIndexImpl::VolatileFull => {
                &IndexBlock::new(self.load_block(
                    &self.regions.tli,
                    BlockType::Index,
                    self.metadata.data_block_compression, // TODO: maybe index compression
                )?)
            }
        };

        let iter = {
            let mut iter = index_block.iter();

            if iter.seek(key) {
                Some(iter.map(|x| x.materialize(&index_block.inner.data)))
            } else {
                None
            }
        };

        let Some(iter) = iter else {
            return Ok(None);
        };

        for block_handle in iter {
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
    pub fn iter(&self) -> Option<impl DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
        self.range(..)
    }

    /// Returns true if the prefix filter indicates the prefix doesn't exist.
    /// This is used to potentially skip segments during range queries.
    /// Only works when a prefix extractor is configured.
    fn should_skip_by_prefix_filter(&self, key: &[u8]) -> bool {
        use filter::standard_bloom::StandardBloomFilterReader;
        #[cfg(feature = "metrics")]
        use std::sync::atomic::Ordering::Relaxed;

        let Some(ref prefix_extractor) = self.prefix_extractor else {
            return false;
        };

        // Don't use prefix filtering if extractor is incompatible
        if !self.prefix_extractor_compatible {
            return false;
        }

        // Try pinned filter block first
        if let Some(block) = &self.pinned_filter_block {
            if let Ok(filter) = StandardBloomFilterReader::new(&block.data) {
                #[cfg(feature = "metrics")]
                self.metrics.filter_queries.fetch_add(1, Relaxed);

                // Returns true if prefix is NOT in filter (should skip)
                return !filter
                    .contains_prefix(key, prefix_extractor.as_ref())
                    .unwrap_or(true);
            }
        }

        // Fall back to loading filter block from disk
        if let Some(filter_block_handle) = &self.regions.filter {
            if let Ok(block) = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None,
            ) {
                if let Ok(filter) = StandardBloomFilterReader::new(&block.data) {
                    #[cfg(feature = "metrics")]
                    self.metrics.filter_queries.fetch_add(1, Relaxed);

                    return !filter
                        .contains_prefix(key, prefix_extractor.as_ref())
                        .unwrap_or(true);
                }
            }
        }

        false
    }

    /// Extracts the common prefix from a range's start and end bounds for filter checking.
    /// Returns the prefix as a slice if both bounds share the same prefix.
    fn extract_common_prefix_for_filter<'a, R: RangeBounds<UserKey>>(
        &self,
        range: &'a R,
        prefix_extractor: &dyn PrefixExtractor,
    ) -> Option<&'a [u8]> {
        let (start_key, end_key) = match (range.start_bound(), range.end_bound()) {
            (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
                (s.as_ref(), Some(e.as_ref()))
            }
            (Bound::Included(s) | Bound::Excluded(s), Bound::Unbounded) => (s.as_ref(), None),
            (Bound::Unbounded, Bound::Included(e) | Bound::Excluded(e)) => (e.as_ref(), None),
            _ => return None,
        };

        // For single bound or when end is unbounded, use that key's prefix
        if end_key.is_none() {
            return prefix_extractor.extract(start_key).next();
        }

        // Both bounds exist - check if they share the same prefix
        if let Some(end) = end_key {
            let start_prefix = prefix_extractor.extract(start_key).next()?;
            let end_prefix = prefix_extractor.extract(end).next()?;

            if start_prefix == end_prefix {
                return Some(start_prefix);
            }
        }

        None
    }

    /// Checks if this segment can be skipped for the given range based on prefix filter.
    /// Returns true if the segment should be skipped.
    /// Only applicable when a prefix extractor is configured.
    fn should_skip_range_by_prefix_filter<R: RangeBounds<UserKey>>(&self, range: &R) -> bool {
        // Early return if no prefix extractor is configured
        let Some(ref prefix_extractor) = self.prefix_extractor else {
            return false;
        };

        // Don't use prefix filtering if extractor is incompatible
        if !self.prefix_extractor_compatible {
            return false;
        }

        // First try: Check filter using common prefix from range bounds
        if let Some(common_prefix) =
            self.extract_common_prefix_for_filter(range, &**prefix_extractor)
        {
            if self.should_skip_by_prefix_filter(common_prefix) {
                #[cfg(feature = "metrics")]
                self.metrics
                    .io_skipped_by_filter
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return true;
            }
        } else {
            // Second try: No common prefix, but we can still try to optimize using the start bound
            if let Some(start_key) = match range.start_bound() {
                Bound::Included(key) | Bound::Excluded(key) => Some(key.as_ref()),
                Bound::Unbounded => None,
            } {
                // Extract prefix from start bound
                if let Some(start_prefix) = prefix_extractor.extract(start_key).next() {
                    // Check if this segment's minimum key would fall in the prefix range
                    // If the segment's min key >= start_key and the start prefix doesn't exist,
                    // we can potentially skip this segment
                    let min_key = self.metadata.key_range.min();

                    // Extract prefix from segment's minimum key
                    if let Some(min_prefix) = prefix_extractor.extract(min_key).next() {
                        if min_prefix == start_prefix
                            && self.should_skip_by_prefix_filter(start_prefix)
                        {
                            #[cfg(feature = "metrics")]
                            self.metrics
                                .io_skipped_by_filter
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    /// Creates a ranged iterator over the `Segment`.
    /// Returns None if the filter indicates no keys with the common prefix exist.
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
    ) -> Option<impl DoubleEndedIterator<Item = crate::Result<InternalValue>>> {
        use crate::fallible_clipping_iter::FallibleClippingIter;
        use block_index::iter::create_index_block_reader;

        // Check prefix filter to see if we can skip this segment entirely
        if self.should_skip_range_by_prefix_filter(&range) {
            return None;
        }

        // TODO: enum_dispatch BlockIndex::iter
        let index_block = match &*self.block_index {
            BlockIndexImpl::Full(idx) => idx.inner(),
            BlockIndexImpl::VolatileFull => {
                &IndexBlock::new(
                    // TODO: handle error
                    self.load_block(
                        &self.regions.tli,
                        BlockType::Index,
                        self.metadata.data_block_compression, // TODO: maybe index compression
                    )
                    .expect("should load block"),
                )
            }
        };

        let index_iter = create_index_block_reader(index_block.clone());
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

        // Set normal iterator bounds based on range
        if let Bound::Excluded(key) | Bound::Included(key) = range.start_bound() {
            iter.set_lower_bound(key.clone());
        }

        if let Bound::Excluded(key) | Bound::Included(key) = range.end_bound() {
            iter.set_upper_bound(key.clone());
        }

        Some(FallibleClippingIter::new(iter, range))
    }

    /// Tries to recover a segment from a file.
    pub fn recover(
        file_path: PathBuf,
        tree_id: TreeId,
        cache: Arc<Cache>,
        descriptor_table: Arc<DescriptorTable>,
        prefix_extractor: Option<SharedPrefixExtractor>,
        pin_filter: bool,
        pin_index: bool,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> crate::Result<Self> {
        use block_index::FullBlockIndex;
        use meta::ParsedMeta;
        use regions::ParsedRegions;
        use std::sync::atomic::AtomicBool;
        use trailer::Trailer;

        log::debug!("Recovering segment from file {}", file_path.display());
        let mut file = std::fs::File::open(&file_path)?;

        let trailer = Trailer::from_file(&mut file)?;
        log::trace!("Got trailer: {trailer:#?}");

        log::debug!(
            "Reading regions block, with region_ptr={:?}",
            trailer.regions_block_handle(),
        );
        let regions = ParsedRegions::load_with_handle(&file, trailer.regions_block_handle())?;

        log::debug!("Reading meta block, with meta_ptr={:?}", regions.metadata);
        let metadata = ParsedMeta::load_with_handle(&file, &regions.metadata)?;

        // Check prefix extractor compatibility
        let prefix_extractor_compatible = match (&metadata.prefix_extractor_name, &prefix_extractor)
        {
            // No extractor configured on either side - compatible
            (None, None) => true,

            (None, Some(_)) => {
                log::warn!(
                    "Segment {} was created without prefix extractor, but one is now configured. Prefix filter will be disabled for this segment.",
                    metadata.id
                );
                false
            }

            (Some(_), None) => {
                log::warn!(
                    "Segment {} was created with prefix extractor, but none is configured now. Prefix filter will be disabled for this segment.", 
                    metadata.id
                );
                false
            }

            (Some(stored_name), Some(current_extractor)) => {
                let current_name = current_extractor.name();
                if stored_name == current_name {
                    true
                } else {
                    log::warn!(
                        "Segment {} was created with prefix extractor '{}', but current extractor is '{}'. Prefix filter will be disabled for this segment.",
                        metadata.id,
                        stored_name,
                        current_name
                    );
                    false
                }
            }
        };

        let block_index = if let Some(index_block_handle) = regions.index {
            log::debug!(
                "Creating partitioned block index, with tli_ptr={:?}, index_block_ptr={index_block_handle:?}",
                regions.tli,
            );

            unimplemented!("partitioned index is not supported yet");

            // BlockIndexImpl::TwoLevel(tli_block, todo!())
        } else if pin_index {
            let tli_block = {
                log::debug!("Reading TLI block, with tli_ptr={:?}", regions.tli);

                let block = Block::from_file(
                    &file,
                    regions.tli,
                    crate::segment::block::BlockType::Index,
                    metadata.data_block_compression, // TODO: index blocks may get their own compression level
                )?;

                IndexBlock::new(block)
            };

            log::debug!(
                "Creating pinned block index, with tli_ptr={:?}",
                regions.tli,
            );
            BlockIndexImpl::Full(FullBlockIndex::new(tli_block))
        } else {
            log::debug!("Creating volatile block index");
            BlockIndexImpl::VolatileFull
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
                        filter_handle,
                        crate::segment::block::BlockType::Filter,
                        crate::CompressionType::None, // NOTE: We never write a filter block with compression
                    )
                })
                .transpose()?
        } else {
            None
        };

        // TODO: Maybe only in L0/L1
        // For larger levels, this will
        // cache possibly many FDs
        // causing kick-out of other
        // FDs in the cache
        //
        // NOTE: We already have a file descriptor open, so let's just cache it immediately
        // descriptor_table.insert_for_table((tree_id, metadata.id).into(), Arc::new(file));

        let segment = Self(Arc::new(Inner {
            path: Arc::new(file_path),
            tree_id,

            metadata,
            regions,

            cache,

            descriptor_table,

            block_index: Arc::new(block_index),

            pinned_filter_block,

            prefix_extractor,
            prefix_extractor_compatible,

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

    /// Returns the seqno range of the `Segment`.
    #[must_use]
    pub fn seqno_range(&self) -> (SeqNo, SeqNo) {
        self.0.metadata.seqnos
    }

    /// Returns the highest sequence number in the segment.
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
        self.0.metadata.seqnos.1
    }

    /// Returns true if this segment has a prefix extractor configured.
    #[must_use]
    pub fn has_prefix_extractor(&self) -> bool {
        self.prefix_extractor.is_some()
    }

    /// Checks if this segment might contain data for the given range.
    /// Returns false only if we can definitively rule out the segment using filters.
    /// Returns true if the segment might contain data (or if we can't determine).
    #[must_use]
    pub fn might_contain_range<R: RangeBounds<UserKey>>(&self, range: &R) -> bool {
        // If no prefix extractor or extractor is incompatible, we can't use filter optimization
        if self.prefix_extractor.is_none() || !self.prefix_extractor_compatible {
            return true;
        }

        // Check if we can skip this segment based on filter
        !self.should_skip_range_by_prefix_filter(range)
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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                None,
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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                None,
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
    fn v3_segment_iter_simple() -> crate::Result<()> {
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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                None,
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

            let iter = segment.iter().unwrap();
            assert_eq!(items, &*iter.flatten().collect::<Vec<_>>());
            let iter = segment.iter().unwrap();
            assert_eq!(
                items.iter().rev().cloned().collect::<Vec<_>>(),
                &*iter.rev().flatten().collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_segment_range_simple() -> crate::Result<()> {
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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                None,
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

            assert_eq!(
                items.iter().skip(1).cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..)
                    .unwrap()
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..)
                    .unwrap()
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_segment_range_ping_pong() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("segment");

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
                None,
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

            let mut iter = segment
                .range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()))
                .unwrap();

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
    fn v3_segment_range_multiple_data_blocks() -> crate::Result<()> {
        let dir = tempdir()?;
        let file = dir.path().join("segment");

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
                None,
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

            assert_eq!(
                items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
                &*segment
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .unwrap()
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
                    .unwrap()
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
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                None,
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
