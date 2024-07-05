use crate::either::{
    Either,
    Either::{Left, Right},
};
use crate::segment::block_index::block_handle::KeyedBlockHandle;
use crate::segment::id::GlobalSegmentId;
use crate::segment::{block_index::IndexBlock, value_block::ValueBlock};
use quick_cache::Weighter;
use quick_cache::{sync::Cache, Equivalent};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum BlockTag {
    Data = 0,
    Index = 1,
}

type Item = Either<Arc<ValueBlock>, Arc<IndexBlock>>;

// (Type (disk or index), Segment ID, Block offset)
#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey(BlockTag, GlobalSegmentId, u64);

impl Equivalent<CacheKey> for (BlockTag, GlobalSegmentId, &u64) {
    fn equivalent(&self, key: &CacheKey) -> bool {
        self.0 == key.0 && self.1 == key.1 && self.2 == &key.2
    }
}

impl From<(BlockTag, GlobalSegmentId, u64)> for CacheKey {
    fn from((tag, gid, bid): (BlockTag, GlobalSegmentId, u64)) -> Self {
        Self(tag, gid, bid)
    }
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
    // TODO: replace .size() calls with block.header.data_length... remove Block::size(), not needed in code base and benches
    fn weight(&self, _: &CacheKey, block: &Item) -> u64 {
        #[allow(clippy::cast_possible_truncation)]
        match block {
            Either::Left(block) => block.size() as u64,
            Either::Right(block) => block
                .items
                .iter()
                .map(|x| x.end_key.len() + std::mem::size_of::<KeyedBlockHandle>())
                .sum::<usize>() as u64,
        }
    }
}

/// Block cache, in which blocks are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive queries to nearby data, improving
/// read performance for hot data.
///
/// # Examples
///
/// Sharing block cache between multiple trees
///
/// ```
/// # use lsm_tree::{Tree, Config, BlockCache};
/// # use std::sync::Arc;
/// #
/// // Provide 40 MB of cache capacity
/// let block_cache = Arc::new(BlockCache::with_capacity_bytes(40 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```
pub struct BlockCache {
    data: Cache<CacheKey, Item, BlockWeighter>,
    capacity: u64,
}

impl BlockCache {
    /// Creates a new block cache with roughly `n` bytes of capacity.
    #[must_use]
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self {
            data: Cache::with_weighter(10_000, bytes, BlockWeighter),
            capacity: bytes,
        }
    }

    /// Returns the amount of cached bytes.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.data.weight()
    }

    /// Returns the cache capacity in bytes.
    #[must_use]
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Returns the number of cached blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no cached blocks.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[doc(hidden)]
    pub fn insert_disk_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: u64,
        value: Arc<ValueBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((BlockTag::Data, segment_id, offset).into(), Left(value));
        }
    }

    #[doc(hidden)]
    pub fn insert_index_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: u64,
        value: Arc<IndexBlock>,
    ) {
        if self.capacity > 0 {
            self.data
                .insert((BlockTag::Index, segment_id, offset).into(), Right(value));
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_disk_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: u64,
    ) -> Option<Arc<ValueBlock>> {
        let key = (BlockTag::Data, segment_id, &offset);
        let item = self.data.get(&key)?;
        Some(item.left().clone())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_index_block(
        &self,
        segment_id: GlobalSegmentId,
        offset: u64,
    ) -> Option<Arc<IndexBlock>> {
        let key = (BlockTag::Index, segment_id, &offset);
        let item = self.data.get(&key)?;
        Some(item.right().clone())
    }
}
