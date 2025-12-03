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

#[cfg(test)]
mod tests;

pub use block::{Block, BlockOffset};
pub use data_block::DataBlock;
pub use id::{GlobalTableId, TableId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
pub use scanner::Scanner;
pub use writer::Writer;

use crate::{
    cache::Cache,
    descriptor_table::DescriptorTable,
    table::{
        block::{BlockType, ParsedItem},
        block_index::{BlockIndex, FullBlockIndex, TwoLevelBlockIndex, VolatileBlockIndex},
        filter::block::FilterBlock,
        regions::ParsedRegions,
        writer::LinkedFile,
    },
    Checksum, CompressionType, InternalValue, SeqNo, TreeId, UserKey,
};
use block_index::BlockIndexImpl;
use inner::Inner;
use iter::Iter;
use std::{
    borrow::Cow,
    fs::File,
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::Arc,
};
use util::load_block;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

// TODO: table iter:
// TODO:    we only need to truncate items from blocks that are not the first and last block
// TODO:    because any block inbetween must (trivially) only contain relevant items

// TODO: in Leveled compaction, compact tables that live very long and have
// many versions (possibly unnecessary space usage of old, stale versions)

pub type TableInner = Inner;

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
pub struct Table(Arc<Inner>);

impl From<Inner> for Table {
    fn from(value: Inner) -> Self {
        Self(Arc::new(value))
    }
}

impl std::ops::Deref for Table {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table:{}({:?})", self.id(), self.metadata.key_range)
    }
}

impl Table {
    #[must_use]
    pub fn global_seqno(&self) -> SeqNo {
        self.0.global_seqno
    }

    pub fn referenced_blob_bytes(&self) -> crate::Result<u64> {
        if let Some(v) = self.0.cached_blob_bytes.get() {
            return Ok(*v);
        }

        let sum = self
            .list_blob_file_references()?
            .map(|bf| bf.iter().map(|f| f.on_disk_bytes).sum::<u64>())
            .unwrap_or_default();

        let _ = self.0.cached_blob_bytes.set(sum);
        Ok(sum)
    }

    pub fn list_blob_file_references(&self) -> crate::Result<Option<Vec<LinkedFile>>> {
        use byteorder::{ReadBytesExt, LE};

        Ok(if let Some(handle) = &self.regions.linked_blob_files {
            // Try to get FD from descriptor table first, similar to util::load_block
            let table_id = self.global_id();
            let cached_fd = self.descriptor_table.access_for_table(&table_id);
            let fd_cache_miss = cached_fd.is_none();

            let fd = if let Some(fd) = cached_fd {
                fd
            } else {
                Arc::new(File::open(&*self.path)?)
            };

            // Read the exact region using pread-style helper
            let buf = crate::file::read_exact(&fd, *handle.offset(), handle.size() as usize)?;

            // If we opened the file here, cache the FD for future accesses
            if fd_cache_miss {
                self.descriptor_table.insert_for_table(table_id, fd);
            }

            // Parse the buffer
            let mut reader = &buf[..];
            let len = reader.read_u32::<LE>()?;
            let mut blob_files = Vec::with_capacity(len as usize);

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

    /// Gets the global table ID.
    #[must_use]
    fn global_id(&self) -> GlobalTableId {
        (self.tree_id, self.id()).into()
    }

    #[must_use]
    pub fn filter_size(&self) -> u32 {
        self.regions.filter.map(|x| x.size()).unwrap_or_default()
    }

    #[must_use]
    pub fn pinned_filter_size(&self) -> usize {
        self.pinned_filter_block
            .as_ref()
            .map(FilterBlock::size)
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

    /// Gets the table ID.
    ///
    /// The table ID is unique for this tree, but not
    /// across multiple trees, use [`Table::global_id`] for that.
    #[must_use]
    pub fn id(&self) -> TableId {
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
        #[cfg(feature = "metrics")]
        use std::sync::atomic::Ordering::Relaxed;

        if (self.metadata.seqnos.0 + self.global_seqno()) >= seqno {
            return Ok(None);
        }

        let filter_block = if let Some(block) = &self.pinned_filter_block {
            Some(Cow::Borrowed(block))
        } else if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter();
            iter.seek(key);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None, // NOTE: We never write a filter block with compression
                )?;
                let block = FilterBlock::new(block);

                Some(Cow::Owned(block))
            } else {
                None
            }
        } else if let Some(_filter_tli_handle) = &self.regions.filter_tli {
            unimplemented!("unpinned filter TLI not supported");
        } else if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None, // NOTE: We never write a filter block with compression
            )?;
            let block = FilterBlock::new(block);

            Some(Cow::Owned(block))
        } else {
            None
        };

        if let Some(filter_block) = filter_block {
            #[cfg(feature = "metrics")]
            self.metrics.filter_queries.fetch_add(1, Relaxed);

            if !filter_block.maybe_contains_hash(key_hash)? {
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

        let seqno = seqno.saturating_sub(self.global_seqno());

        for block_handle in iter {
            let block_handle = block_handle?;

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

    /// Creates a scanner over the `Table`.
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
            self.global_seqno(),
        )
    }

    /// Creates an iterator over the `Table`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[doc(hidden)]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> {
        self.range(..)
    }

    /// Creates a ranged iterator over the `Table`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[doc(hidden)]
    pub fn range<R: RangeBounds<UserKey> + Send>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + Send {
        let index_iter = self.block_index.iter();

        let mut iter = Iter::new(
            self.global_id(),
            self.global_seqno(),
            self.path.clone(),
            index_iter,
            self.descriptor_table.clone(),
            self.cache.clone(),
            self.metadata.data_block_compression,
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        );

        match range.start_bound() {
            Bound::Included(key) => iter.set_lower_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_lower_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        match range.end_bound() {
            Bound::Included(key) => iter.set_upper_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_upper_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        iter
    }

    fn read_tli(
        regions: &ParsedRegions,
        file: &File,
        compression: CompressionType,
    ) -> crate::Result<IndexBlock> {
        log::trace!("Reading TLI block, with tli_ptr={:?}", regions.tli);

        let block = Block::from_file(file, regions.tli, compression)?;

        if block.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        Ok(IndexBlock::new(block))
    }

    /// Tries to recover a table from a file.
    #[warn(clippy::too_many_arguments)]
    pub fn recover(
        file_path: PathBuf,
        checksum: Checksum,
        global_seqno: SeqNo,
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

        #[cfg(feature = "metrics")]
        metrics
            .table_file_opened_uncached
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::debug!("Recovering table from file {}", file_path.display());
        let mut file = std::fs::File::open(&file_path)?;
        let file_path = Arc::new(file_path);

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
                path: Arc::clone(&file_path),
                table_id: (tree_id, metadata.id).into(),

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
                path: Arc::clone(&file_path),
                table_id: (tree_id, metadata.id).into(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        };

        let pinned_filter_index = if let Some(filter_tli_handle) = regions.filter_tli {
            let block =
                Block::from_file(&file, filter_tli_handle, metadata.index_block_compression)?;
            Some(IndexBlock::new(block))
        } else {
            None
        };

        // TODO: FilterBlock newtype
        let pinned_filter_block = if pinned_filter_index.is_none() && pin_filter {
            regions
                .filter
                .map(|filter_handle| {
                    log::debug!(
                        "Loading and pinning filter block, with filter_ptr={filter_handle:?}"
                    );

                    let block = Block::from_file(
                        &file,
                        filter_handle,
                        crate::CompressionType::None, // NOTE: We never write a filter block with compression
                    )
                    .and_then(|block| {
                        if block.header.block_type == BlockType::Filter {
                            Ok(block)
                        } else {
                            Err(crate::Error::InvalidTag((
                                "BlockType",
                                block.header.block_type.into(),
                            )))
                        }
                    })?;

                    Ok::<_, crate::Error>(FilterBlock::new(block))
                })
                .transpose()?
        } else {
            None
        };

        descriptor_table.insert_for_table((tree_id, metadata.id).into(), Arc::new(file));

        log::trace!("Table #{} recovered", metadata.id);

        Ok(Self(Arc::new(Inner {
            path: file_path,
            tree_id,

            metadata,
            regions,

            cache,

            descriptor_table,

            block_index: Arc::new(block_index),

            pinned_filter_index,

            pinned_filter_block,

            is_deleted: AtomicBool::default(),

            checksum,
            global_seqno,

            #[cfg(feature = "metrics")]
            metrics,

            cached_blob_bytes: std::sync::OnceLock::new(),
        })))
    }

    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.0.checksum
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

    /// Checks if a key range is (partially or fully) contained in this table.
    pub(crate) fn check_key_range_overlap(&self, bounds: &(Bound<&[u8]>, Bound<&[u8]>)) -> bool {
        self.metadata.key_range.overlaps_with_bounds(bounds)
    }

    /// Returns the highest sequence number in the table.
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
        self.metadata.seqnos.1
    }

    /// Returns the number of tombstone markers in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns the number of weak (single delete) tombstones in the `Table`.
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

    /// Returns the ratio of tombstone markers in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_ratio(&self) -> f32 {
        todo!()

        //  self.metadata.tombstone_count as f32 / self.metadata.key_count as f32
    }
}
