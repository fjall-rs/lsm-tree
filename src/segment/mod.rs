pub mod block;
pub mod block_index;
pub mod meta;
pub mod multi_reader;
pub mod multi_writer;
pub mod prefix;
pub mod range;
pub mod reader;
pub mod writer;

use self::{
    block::load_and_cache_by_block_handle, block_index::BlockIndex, meta::Metadata,
    prefix::PrefixedReader, range::Range, reader::Reader,
};
use crate::{
    block_cache::BlockCache,
    descriptor_table::FileDescriptorTable,
    file::SEGMENT_METADATA_FILE,
    value::{SeqNo, UserKey},
    Value,
};
use std::{ops::Bound, path::Path, sync::Arc};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

#[cfg(feature = "bloom")]
use crate::file::BLOOM_FILTER_FILE;

/// Disk segment (a.k.a. `SSTable`, `sorted string table`) that is located on disk
///
/// A segment is an immutable list of key-value pairs, split into compressed blocks (see [`block::ValueBlock`]).
/// The block offset and size in the file is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Segments can be merged together to remove duplicates, reducing disk space and improving read performance.
pub struct Segment {
    pub(crate) descriptor_table: Arc<FileDescriptorTable>,

    /// Segment metadata object (will be stored in a JSON file)
    pub metadata: meta::Metadata,

    /// Translates key (first item of a block) to block offset (address inside file) and (compressed) size
    pub(crate) block_index: Arc<BlockIndex>,

    /// Block cache
    ///
    /// Stores index and data blocks
    pub(crate) block_cache: Arc<BlockCache>,

    /// Bloom filter
    #[cfg(feature = "bloom")]
    pub(crate) bloom_filter: BloomFilter,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Segment:{}", self.metadata.id)
    }
}

impl Segment {
    /// Tries to recover a segment from a folder.
    pub fn recover<P: AsRef<Path>>(
        folder: P,
        block_cache: Arc<BlockCache>,
        descriptor_table: Arc<FileDescriptorTable>,
    ) -> crate::Result<Self> {
        let folder = folder.as_ref();

        let metadata = Metadata::from_disk(folder.join(SEGMENT_METADATA_FILE))?;
        let block_index = BlockIndex::from_file(
            metadata.id.clone(),
            descriptor_table.clone(),
            folder,
            Arc::clone(&block_cache),
        )?;

        Ok(Self {
            descriptor_table,
            metadata,
            block_index: Arc::new(block_index),
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::from_file(folder.join(BLOOM_FILTER_FILE))?,
        })
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

        // Get the block handle, if it doesn't exist, the key is definitely not found
        let Some(block_handle) = self.block_index.get_latest(key.as_ref())? else {
            return Ok(None);
        };

        // The block should definitely exist, we just got the block handle before
        let Some(block) = load_and_cache_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            &self.metadata.id,
            &block_handle,
        )?
        else {
            return Ok(None);
        };

        let mut maybe_our_items_iter = block
            .items
            .iter()
            // TODO: maybe binary search can be used, but it needs to find the max seqno
            .filter(|item| item.key == key.as_ref().into());

        match seqno {
            None => {
                // NOTE: Fastpath for non-seqno reads (which are most common)
                // This avoids setting up a rather expensive block iterator
                // (see explanation for that below)
                // This only really works because sequence numbers are sorted
                // in descending order
                //
                // If it doesn't exist, we avoid loading the next block
                // because the block handle was retrieved using the item key, so if
                // the item exists, it HAS to be in the first block

                Ok(maybe_our_items_iter.next().cloned())
            }
            Some(seqno) => {
                for item in maybe_our_items_iter {
                    if item.seqno < seqno {
                        return Ok(Some(item.clone()));
                    }
                }

                // NOTE: If we got here, the item was not in the block :(

                // NOTE: For finding a specific seqno,
                // we need to use a prefixed reader
                // because nothing really prevents the version
                // we are searching for to be in the next block
                // after the one our key starts in
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

                // Load next block and setup block iterator
                let Some(next_block_handle) = self
                    .block_index
                    .get_next_block_key(&block_handle.start_key)?
                else {
                    return Ok(None);
                };

                let iter = Reader::new(
                    Arc::clone(&self.descriptor_table),
                    self.metadata.id.clone(),
                    Some(Arc::clone(&self.block_cache)),
                    Arc::clone(&self.block_index),
                    Some(&next_block_handle.start_key),
                    None,
                );

                for item in iter {
                    let item = item?;

                    // Just stop iterating once we go past our desired key
                    if &*item.key != key {
                        return Ok(None);
                    }

                    if item.seqno < seqno {
                        return Ok(Some(item));
                    }
                }

                Ok(None)
            }
        }
    }

    /// Creates an iterator over the `Segment`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self, use_cache: bool) -> Reader {
        let cache = if use_cache {
            Some(Arc::clone(&self.block_cache))
        } else {
            None
        };

        Reader::new(
            Arc::clone(&self.descriptor_table),
            self.metadata.id.clone(),
            cache,
            Arc::clone(&self.block_index),
            None,
            None,
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
            Arc::clone(&self.descriptor_table),
            self.metadata.id.clone(),
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
            Arc::clone(&self.descriptor_table),
            self.metadata.id.clone(),
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

    /*  /// Returns `true` if the key is contained in the segment's key range.
    pub(crate) fn key_range_contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.metadata.key_range_contains(key)
    }

    /// Returns `true` if the prefix matches any key in the segment's key range.
    pub(crate) fn check_prefix_overlap(&self, prefix: &[u8]) -> bool {
        self.metadata.key_range.contains_prefix(prefix)
    } */

    /// Checks if a key range is (partially or fully) contained in this segment.
    pub(crate) fn check_key_range_overlap(
        &self,
        bounds: &(Bound<UserKey>, Bound<UserKey>),
    ) -> bool {
        self.metadata.key_range.overlaps_with_bounds(bounds)
    }
}
