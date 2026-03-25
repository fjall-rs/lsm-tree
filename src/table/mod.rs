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
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::needless_borrows_for_generic_args,
    reason = "test code"
)]
mod tests;

pub use block::{Block, BlockOffset};
pub use data_block::DataBlock;
pub use id::{GlobalTableId, TableId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
pub use scanner::Scanner;
pub use writer::Writer;

use crate::{
    cache::Cache,
    comparator::SharedComparator,
    descriptor_table::DescriptorTable,
    file_accessor::FileAccessor,
    fs::FsFile,
    range_tombstone::RangeTombstone,
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
    ops::{Bound, RangeBounds},
    path::PathBuf,
    sync::Arc,
};
use util::load_block;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

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

impl std::ops::Deref for Table {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
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
            let table_id = self.global_id();

            let (fd, fd_cache_miss) =
                if let Some(fd) = self.file_accessor.access_for_table(&table_id) {
                    (fd, false)
                } else {
                    let fd: Arc<dyn FsFile> = Arc::new(std::fs::File::open(&*self.path)?);
                    (fd, true)
                };

            // Read the exact region using pread-style helper
            let buf =
                crate::file::read_exact(fd.as_ref(), *handle.offset(), handle.size() as usize)?;

            // If we opened the file here, cache the FD for future accesses
            if fd_cache_miss {
                self.file_accessor.insert_for_table(table_id, fd);
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

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "truncation is not expected to happen"
                )]
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
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<Block> {
        load_block(
            self.global_id(),
            &self.path,
            &self.file_accessor,
            &self.cache,
            handle,
            block_type,
            compression,
            self.encryption.as_deref(),
            #[cfg(zstd_any)]
            zstd_dict,
            #[cfg(feature = "metrics")]
            &self.metrics,
        )
    }

    fn load_data_block(&self, handle: &BlockHandle) -> crate::Result<DataBlock> {
        self.load_block(
            handle,
            BlockType::Data,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
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

        // Translate seqno to "our" seqno
        let seqno = seqno.saturating_sub(self.global_seqno());

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let filter_block = if let Some(block) = &self.pinned_filter_block {
            Some(Cow::Borrowed(block))
        } else if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter(self.comparator.clone());
            iter.seek(key, seqno);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None, // NOTE: We never write a filter block with compression
                    #[cfg(zstd_any)]
                    None,
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
                #[cfg(zstd_any)]
                None,
            )?;
            let block = FilterBlock::new(block);

            Some(Cow::Owned(block))
        } else {
            None
        };

        if let Some(filter_block) = &filter_block {
            if !filter_block.maybe_contains_hash(key_hash)? {
                #[cfg(feature = "metrics")]
                {
                    self.metrics.filter_queries.fetch_add(1, Relaxed);
                    self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
                }

                return Ok(None);
            }
        }

        let item = self.point_read(key, seqno);

        #[cfg(not(feature = "metrics"))]
        {
            item
        }

        #[cfg(feature = "metrics")]
        {
            // NOTE: Only increment the filter queries when the filter reported a miss
            // and we actually waste an I/O for a non-existing item.
            // Otherwise, the filter efficiency decreases whenever an item is hit.
            // https://github.com/fjall-rs/lsm-tree/issues/246
            item.inspect(|maybe_kv| {
                if maybe_kv.is_none() && filter_block.is_some() {
                    self.metrics.filter_queries.fetch_add(1, Relaxed);
                }
            })
        }
    }

    // TODO: maybe we can skip Fuse costs of the user key
    // TODO: because we just want to return the value
    // TODO: we would need to return something like ValueType + Value
    // TODO: so the caller can decide whether to return the value or not
    fn point_read(&self, key: &[u8], seqno: SeqNo) -> crate::Result<Option<InternalValue>> {
        let Some(iter) = self.block_index.forward_reader(key, seqno) else {
            return Ok(None);
        };

        for block_handle in iter {
            let block_handle = block_handle?;

            let block = self.load_data_block(block_handle.as_ref())?;

            if let Some(item) = block.point_read(key, seqno, &self.comparator) {
                return Ok(Some(item));
            }

            // NOTE: If the last block key is higher than ours,
            // our key cannot be in the next block
            if self.comparator.compare(block_handle.end_key(), key) == std::cmp::Ordering::Greater {
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
        #[expect(
            clippy::expect_used,
            reason = "there shouldn't be 4 billion data blocks in a single table"
        )]
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
            self.encryption.clone(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
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
            self.file_accessor.clone(),
            self.cache.clone(),
            self.metadata.data_block_compression,
            self.encryption.clone(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
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
        file: &dyn FsFile,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    ) -> crate::Result<IndexBlock> {
        log::trace!("Reading TLI block, with tli_ptr={:?}", regions.tli);

        let block = Block::from_file(
            file,
            regions.tli,
            compression,
            encryption,
            #[cfg(zstd_any)]
            None,
        )?;

        if block.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        Ok(IndexBlock::new(block))
    }

    /// Tries to recover a table from a file.
    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "recovery requires many context parameters and is inherently complex"
    )]
    pub fn recover(
        file_path: PathBuf,
        checksum: Checksum,
        global_seqno: SeqNo,
        tree_id: TreeId,
        cache: Arc<Cache>,
        descriptor_table: Option<Arc<DescriptorTable>>,
        pin_filter: bool,
        pin_index: bool,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> crate::Result<Self> {
        use meta::ParsedMeta;
        use regions::ParsedRegions;
        use std::sync::atomic::AtomicBool;

        log::debug!("Recovering table from file {}", file_path.display());
        let mut file = std::fs::File::open(&file_path)?;
        let file_path = Arc::new(file_path);

        #[cfg(feature = "metrics")]
        metrics
            .table_file_opened_uncached
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let trailer = sfa::Reader::from_reader(&mut file)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

        log::trace!("Reading meta block, with meta_ptr={:?}", regions.metadata);
        let metadata =
            ParsedMeta::load_with_handle(&file, &regions.metadata, encryption.as_deref())?;

        // Fail-fast: if this table was written with dictionary compression,
        // verify the caller provided the matching dictionary. Without this
        // check, reopening with the wrong dictionary (or None) would only
        // surface as a decompression error on the first data-block read.
        #[cfg(zstd_any)]
        if let CompressionType::ZstdDict { dict_id, .. } = metadata.data_block_compression {
            let got = zstd_dictionary.as_ref().map(|d| d.id());
            if got != Some(dict_id) {
                return Err(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got,
                });
            }
        }

        let file_handle: Arc<dyn FsFile> = Arc::new(file);

        let file_accessor = if let Some(dt) = descriptor_table {
            FileAccessor::DescriptorTable(dt)
        } else {
            FileAccessor::File(file_handle.clone())
        };

        let block_index = if regions.index.is_some() {
            log::trace!(
                "Creating partitioned block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(
                &regions,
                file_handle.as_ref(),
                metadata.index_block_compression,
                encryption.as_deref(),
            )?;

            BlockIndexImpl::TwoLevel(TwoLevelBlockIndex {
                top_level_index: block,
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                path: Arc::clone(&file_path),
                file_accessor: file_accessor.clone(),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                comparator: comparator.clone(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        } else if pin_index {
            log::trace!(
                "Creating pinned, full block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(
                &regions,
                file_handle.as_ref(),
                metadata.index_block_compression,
                encryption.as_deref(),
            )?;
            BlockIndexImpl::Full(FullBlockIndex::new(block, comparator.clone()))
        } else {
            log::trace!("Creating volatile, full block index");

            BlockIndexImpl::VolatileFull(VolatileBlockIndex {
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                file_accessor: file_accessor.clone(),
                handle: regions.tli,
                path: Arc::clone(&file_path),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                comparator: comparator.clone(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        };

        let pinned_filter_index = if let Some(filter_tli_handle) = regions.filter_tli {
            let block = Block::from_file(
                file_handle.as_ref(),
                filter_tli_handle,
                metadata.index_block_compression,
                encryption.as_deref(),
                #[cfg(zstd_any)]
                None,
            )?;
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
                        file_handle.as_ref(),
                        filter_handle,
                        crate::CompressionType::None, // NOTE: We never write a filter block with compression
                        encryption.as_deref(),
                        #[cfg(zstd_any)]
                        None,
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

        // Load range tombstones (if present)
        let range_tombstones = if let Some(rt_handle) = regions.range_tombstones {
            log::trace!("Loading range tombstone block, with rt_ptr={rt_handle:?}");
            let block = Block::from_file(
                file_handle.as_ref(),
                rt_handle,
                crate::CompressionType::None,
                encryption.as_deref(),
                #[cfg(zstd_any)]
                None,
            )?;

            if block.header.block_type != BlockType::RangeTombstone {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }

            let mut rts = Self::decode_range_tombstones(&block, comparator.as_ref())?;
            // Sort range tombstones by (start asc, seqno desc) using the
            // user comparator so the order matches the tree's key ordering.
            // The seqno-desc tiebreaker ensures higher-seqno RTs are checked
            // first when multiple share the same start key.
            let cmp = &comparator;
            rts.sort_unstable_by(|a, b| {
                cmp.compare(&a.start, &b.start)
                    .then_with(|| b.seqno.cmp(&a.seqno))
            });
            rts
        } else {
            Vec::new()
        };

        log::debug!(
            "Recovered table #{} from {}",
            metadata.id,
            file_path.display(),
        );

        Ok(Self(Arc::new(Inner {
            path: file_path,
            tree_id,

            metadata,
            regions,

            cache,

            file_accessor,

            block_index: Arc::new(block_index),

            pinned_filter_index,

            pinned_filter_block,

            is_deleted: AtomicBool::default(),

            checksum,
            global_seqno,

            comparator,

            #[cfg(feature = "metrics")]
            metrics,

            cached_blob_bytes: std::sync::OnceLock::new(),
            range_tombstones,
            encryption,

            #[cfg(zstd_any)]
            zstd_dictionary,
        })))
    }

    #[must_use]
    pub fn checksum(&self) -> Checksum {
        self.0.checksum
    }

    /// Read `len` bytes from the cursor position with checked arithmetic.
    /// Uses `.get()` instead of direct indexing to satisfy `clippy::indexing_slicing`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block sizes are bounded well within usize on all supported platforms"
    )]
    fn read_checked_slice(
        cursor: &mut std::io::Cursor<&[u8]>,
        field: &'static str,
        len: usize,
    ) -> crate::Result<Vec<u8>> {
        let offset = cursor.position();
        let data = cursor.get_ref();
        let pos = offset as usize;
        let end_pos = pos
            .checked_add(len)
            .ok_or(crate::Error::RangeTombstoneDecode { field, offset })?;
        let buf = data
            .get(pos..end_pos)
            .ok_or(crate::Error::RangeTombstoneDecode { field, offset })?
            .to_vec();
        cursor.set_position(end_pos as u64);
        Ok(buf)
    }

    /// Decodes range tombstones from a raw block.
    ///
    /// Wire format (repeated): `[start_len:u16_le][start][end_len:u16_le][end][seqno:u64_le]`
    ///
    /// # Errors
    ///
    /// Will return `Err` if the block data is malformed.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block sizes are bounded well within usize on all supported platforms"
    )]
    fn decode_range_tombstones(
        block: &Block,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Vec<RangeTombstone>> {
        use byteorder::{ReadBytesExt, LE};
        use std::io::Cursor;

        let mut tombstones = Vec::new();
        let data = block.data.as_ref();

        // A dedicated RT block with empty payload is corruption — the writer
        // only creates an RT block handle when at least one tombstone exists.
        if data.is_empty() {
            log::error!("Range tombstone block: missing start_len");
            return Err(crate::Error::RangeTombstoneDecode {
                field: "start_len",
                offset: 0,
            });
        }

        let mut cursor = Cursor::new(data);

        while (cursor.position() as usize) < data.len() {
            let entry_offset = cursor.position();
            let start_len_offset = entry_offset;
            let start_len =
                cursor
                    .read_u16::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "start_len",
                        offset: start_len_offset,
                    })? as usize;

            // Validate length against remaining data before allocating
            let remaining = data.len() - cursor.position() as usize;
            if start_len > remaining {
                log::error!(
                    "Range tombstone block: start_len {start_len} exceeds remaining {remaining}"
                );
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "start_len",
                    offset: start_len_offset,
                });
            }

            // Extract validated slice from cursor position.
            // Using .get() instead of direct indexing to satisfy clippy::indexing_slicing.
            let start_buf = Self::read_checked_slice(&mut cursor, "start", start_len)?;

            let end_len_offset = cursor.position();
            let end_len =
                cursor
                    .read_u16::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "end_len",
                        offset: end_len_offset,
                    })? as usize;

            let remaining = data.len() - cursor.position() as usize;
            if end_len > remaining {
                log::error!(
                    "Range tombstone block: end_len {end_len} exceeds remaining {remaining}"
                );
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "end_len",
                    offset: end_len_offset,
                });
            }

            let end_buf = Self::read_checked_slice(&mut cursor, "end", end_len)?;

            let seqno_offset = cursor.position();
            let seqno =
                cursor
                    .read_u64::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "seqno",
                        offset: seqno_offset,
                    })?;

            let start = UserKey::from(start_buf);
            let end = UserKey::from(end_buf);

            // Validate invariant: start < end using the tree's comparator
            // (reject corrupted or misordered intervals)
            if comparator.compare(&start, &end) != std::cmp::Ordering::Less {
                log::error!("Range tombstone block: invalid interval (start >= end)");
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "interval",
                    offset: entry_offset,
                });
            }

            tombstones.push(RangeTombstone::new(start, end, seqno));
        }

        Ok(tombstones)
    }

    /// Returns the range tombstones stored in this table.
    #[must_use]
    pub(crate) fn range_tombstones(&self) -> &[RangeTombstone] {
        &self.0.range_tombstones
    }

    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Checks if a key range overlaps (partially or fully) with this table's key range.
    pub(crate) fn check_key_range_overlap_cmp(
        &self,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        self.metadata
            .key_range
            .overlaps_with_bounds_cmp(bounds, cmp)
    }

    /// Checks the full-table bloom filter for a hash value.
    ///
    /// Returns `Ok(true)` if the hash may exist in the filter (or if no full
    /// filter is available), `Ok(false)` if the hash is definitely absent.
    ///
    /// Handles full (non-partitioned) filters directly. Partitioned / TLI
    /// filters are keyed by user key, not raw hash, so this method returns
    /// `Ok(true)` conservatively for those types.
    fn bloom_may_contain_hash(&self, hash: u64) -> crate::Result<bool> {
        // Full (non-partitioned) filter — single bloom covers the entire table
        if let Some(block) = &self.pinned_filter_block {
            return block.maybe_contains_hash(hash);
        }

        // Partitioned / TLI filters: partition index is keyed by user key, not
        // raw hash — we would need to scan ALL partitions to check,
        // which is O(partitions) I/O and defeats the purpose of bloom skip.
        // Returning Ok(true) is correct (conservative: segment is NOT skipped).
        if self.pinned_filter_index.is_some() || self.regions.filter_tli.is_some() {
            return Ok(true);
        }

        // Unpinned full filter — load from disk.
        // Safe: if we reach here, filter_tli is None (no partitioned filter),
        // so regions.filter is a single full-table bloom, not a concatenation.
        if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None, // NOTE: Filter blocks are never compressed (crate invariant)
                #[cfg(zstd_any)]
                None,
            )?;
            let block = FilterBlock::new(block);
            return block.maybe_contains_hash(hash);
        }

        // No filter available — cannot rule out the hash
        Ok(true)
    }

    /// Checks the bloom filter for a prefix hash.
    ///
    /// Returns `Ok(true)` if the prefix may exist in this table (or if no
    /// filter is available), `Ok(false)` if the prefix is definitely absent.
    ///
    /// This is used by prefix scans to skip segments that contain no keys
    /// with a matching prefix. The prefix must have been indexed at write
    /// time via a [`PrefixExtractor`](crate::PrefixExtractor).
    pub(crate) fn maybe_contains_prefix(&self, prefix_hash: u64) -> crate::Result<bool> {
        self.bloom_may_contain_hash(prefix_hash)
    }

    /// Checks the bloom filter for a precomputed key hash.
    ///
    /// Returns `Ok(true)` if the key may exist in this table (or if no
    /// filter is available), `Ok(false)` if the key is definitely absent.
    ///
    /// Used by the point-read merge pipeline to pre-filter disk tables
    /// before building range iterators. For partitioned or TLI filter
    /// configurations, the underlying check returns `Ok(true)` conservatively,
    /// so pre-filtering is best-effort and configuration-dependent.
    pub(crate) fn bloom_may_contain_key_hash(&self, key_hash: u64) -> crate::Result<bool> {
        self.bloom_may_contain_hash(key_hash)
    }

    /// Checks the bloom filter for a key, with partition-aware seeking.
    ///
    /// Unlike [`bloom_may_contain_key_hash`](Self::bloom_may_contain_key_hash)
    /// which falls back to `Ok(true)` for partitioned filters, this method
    /// uses the user key to seek the partition index and check only the
    /// matching partition's bloom filter.
    ///
    /// `key_hash` must be the xxh3 hash of `key` (pre-computed by the caller
    /// to avoid redundant hashing — same pattern as [`Table::get`]).
    pub(crate) fn bloom_may_contain_key(&self, key: &[u8], key_hash: u64) -> crate::Result<bool> {
        debug_assert_eq!(
            crate::table::filter::standard_bloom::Builder::get_hash(key),
            key_hash,
            "bloom_may_contain_key: key_hash must be BloomBuilder::get_hash(key)"
        );

        // Full (non-partitioned) filter — delegate to hash-only path.
        // A table has either pinned_filter_block (full) or pinned_filter_index
        // (partitioned), never both — checked at construction time.
        if self.pinned_filter_block.is_some() {
            return self.bloom_may_contain_hash(key_hash);
        }

        // Partitioned filter with pinned TLI — seek to the matching partition
        if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter(self.comparator.clone());
            iter.seek(key, crate::seqno::MAX_SEQNO);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None,
                    #[cfg(zstd_any)]
                    None,
                )?;
                let block = FilterBlock::new(block);
                return block.maybe_contains_hash(key_hash);
            }

            // iter.next() == None means the key is beyond all partition
            // boundaries (seek found no ceiling entry in the TLI, which is
            // ordered by each partition's last user key). The key cannot
            // exist in this table. Same logic as Table::get (line ~265).
            return Ok(false);
        }

        // Unpinned filter — fall through to hash-only path (handles both
        // unpinned full filters and the no-filter case)
        self.bloom_may_contain_hash(key_hash)
    }

    /// Returns the highest effective sequence number in the table.
    ///
    /// For tables produced by flush/compaction (`global_seqno == 0`), this
    /// returns the highest item seqno directly.
    ///
    /// For tables produced by bulk ingestion (`global_seqno > 0`), items
    /// are written with local seqno 0 and the table carries a global offset.
    /// The effective seqno of each item is `global_seqno + local_seqno`,
    /// which mirrors the translation in [`Table::get`].
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
        self.metadata.seqnos.1 + self.global_seqno()
    }

    /// Returns the highest sequence number from KV entries only,
    /// excluding range tombstone seqnos.
    ///
    /// This enables more aggressive table-skip: a covering RT stored
    /// in the same table can trigger skip because its seqno may exceed
    /// the KV-only max even though it doesn't exceed the overall max.
    ///
    /// For tables written before this field was introduced, falls back
    /// to `get_highest_seqno()` (conservative but correct).
    #[must_use]
    pub fn get_highest_kv_seqno(&self) -> SeqNo {
        self.metadata.highest_kv_seqno + self.global_seqno()
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
