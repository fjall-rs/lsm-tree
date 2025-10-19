use super::KeyedBlockHandle;
use crate::{
    segment::{
        block::BlockType,
        block_index::{iter::OwnedIndexBlockIter, BlockIndexIter},
        util::load_block,
        BlockHandle, IndexBlock,
    },
    Cache, CompressionType, DescriptorTable, GlobalSegmentId, UserKey,
};
use std::{path::PathBuf, sync::Arc};

/// Index that translates item keys to data block handles
///
/// The index is loaded on demand.
pub struct VolatileBlockIndex {
    pub(crate) segment_id: GlobalSegmentId,
    pub(crate) path: PathBuf,
    pub(crate) descriptor_table: Arc<DescriptorTable>,
    pub(crate) cache: Arc<Cache>,
    pub(crate) handle: BlockHandle,
    pub(crate) compression: CompressionType,
}

impl VolatileBlockIndex {
    pub fn forward_reader(
        &self,
        needle: &[u8],
    ) -> impl Iterator<Item = crate::Result<KeyedBlockHandle>> + '_ {
        let mut iter = Iter::new(self);
        iter.seek_lower(needle);
        iter
    }

    pub fn iter(&self) -> impl BlockIndexIter {
        Iter::new(self)
    }
}

struct Iter {
    inner: Option<OwnedIndexBlockIter>,
    segment_id: GlobalSegmentId,
    path: PathBuf,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    handle: BlockHandle,
    compression: CompressionType,

    lo: Option<UserKey>,
    hi: Option<UserKey>,
}

impl Iter {
    fn new(index: &VolatileBlockIndex) -> Self {
        Self {
            inner: None,
            segment_id: index.segment_id,
            path: index.path.clone(),
            descriptor_table: index.descriptor_table.clone(),
            cache: index.cache.clone(),
            handle: index.handle,
            compression: index.compression,

            lo: None,
            hi: None,
        }
    }
}

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8]) -> bool {
        self.lo = Some(key.into());
        true
    }

    fn seek_upper(&mut self, key: &[u8]) -> bool {
        self.hi = Some(key.into());
        true
    }
}

impl Iterator for Iter {
    type Item = crate::Result<KeyedBlockHandle>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner {
            inner.next().map(Ok)
        } else {
            let block = fail_iter!(load_block(
                self.segment_id,
                &self.path,
                &self.descriptor_table,
                &self.cache,
                &self.handle,
                BlockType::Index,
                self.compression,
            ));
            let index_block = IndexBlock::new(block);

            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

            if let Some(lo) = &self.lo {
                if !iter.seek_lower(lo) {
                    return None;
                }
            }
            if let Some(hi) = &self.hi {
                if !iter.seek_upper(hi) {
                    return None;
                }
            }

            let next_item = iter.next().map(Ok);

            self.inner = Some(iter);

            next_item
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(inner) = &mut self.inner {
            inner.next_back().map(Ok)
        } else {
            let block = fail_iter!(load_block(
                self.segment_id,
                &self.path,
                &self.descriptor_table,
                &self.cache,
                &self.handle,
                BlockType::Index,
                self.compression,
            ));
            let index_block = IndexBlock::new(block);

            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

            if let Some(lo) = &self.lo {
                if !iter.seek_lower(lo) {
                    return None;
                }
            }
            if let Some(hi) = &self.hi {
                if !iter.seek_upper(hi) {
                    return None;
                }
            }

            let next_item = iter.next_back().map(Ok);

            self.inner = Some(iter);

            next_item
        }
    }
}
