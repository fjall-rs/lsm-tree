use crate::segment::{IndexBlock, KeyedBlockHandle};
use crate::{
    segment::{
        block::BlockType,
        block_index::{iter::OwnedIndexBlockIter, BlockIndexIter},
        util::load_block,
    },
    Cache, CompressionType, DescriptorTable, GlobalSegmentId, UserKey,
};
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::Metrics;

/// Index that translates item keys to data block handles
///
/// Only the top-level index is loaded into memory.
pub struct TwoLevelBlockIndex {
    pub(crate) top_level_index: IndexBlock,
    pub(crate) segment_id: GlobalSegmentId,
    pub(crate) path: PathBuf,
    pub(crate) descriptor_table: Arc<DescriptorTable>,
    pub(crate) cache: Arc<Cache>,
    pub(crate) compression: CompressionType,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl TwoLevelBlockIndex {
    pub fn forward_reader(&self, needle: &[u8]) -> Iter {
        let mut iter = self.iter();
        iter.seek_lower(needle);
        iter
    }

    pub fn iter(&self) -> Iter {
        Iter {
            tli_block: self.top_level_index.clone(),
            tli: None,
            lo_consumer: None,
            hi_consumer: None,
            lo: None,
            hi: None,
            segment_id: self.segment_id,
            path: self.path.clone(),
            descriptor_table: self.descriptor_table.clone(),
            cache: self.cache.clone(),
            compression: self.compression,

            #[cfg(feature = "metrics")]
            metrics: self.metrics.clone(),
        }
    }
}

pub(super) struct Iter {
    tli_block: IndexBlock,
    tli: Option<OwnedIndexBlockIter>,

    lo_consumer: Option<OwnedIndexBlockIter>,
    hi_consumer: Option<OwnedIndexBlockIter>,

    lo: Option<UserKey>,
    hi: Option<UserKey>,

    segment_id: GlobalSegmentId,
    path: PathBuf,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    compression: CompressionType,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    fn init_tli(&mut self) -> bool {
        let mut iter = OwnedIndexBlockIter::new(self.tli_block.clone(), IndexBlock::iter);

        if let Some(lo) = &self.lo {
            if !iter.seek_lower(lo) {
                return false;
            }
        }
        if let Some(hi) = &self.hi {
            if !iter.seek_upper(hi) {
                return false;
            }
        }

        self.tli = Some(iter);

        true
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
        if let Some(lo_block) = &mut self.lo_consumer {
            if let Some(item) = lo_block.next() {
                return Some(Ok(item));
            }
        }

        if self.tli.is_none() && !self.init_tli() {
            return None;
        }

        if let Some(tli) = &mut self.tli {
            let next_lowest_block = tli.next();

            if let Some(handle) = next_lowest_block {
                let block = fail_iter!(load_block(
                    self.segment_id,
                    &self.path,
                    &self.descriptor_table,
                    &self.cache,
                    &handle.into_inner(),
                    BlockType::Index,
                    self.compression,
                    #[cfg(feature = "metrics")]
                    &self.metrics,
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

                self.lo_consumer = Some(iter);

                if let Some(item) = next_item {
                    return Some(item);
                }
            }
        }

        // Nothing more found, consume from hi consumer
        if let Some(hi_block) = &mut self.hi_consumer {
            if let Some(item) = hi_block.next() {
                return Some(Ok(item));
            }
        }

        None
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(hi_block) = &mut self.hi_consumer {
            if let Some(item) = hi_block.next_back() {
                return Some(Ok(item));
            }
        }

        if self.tli.is_none() && !self.init_tli() {
            return None;
        }

        if let Some(tli) = &mut self.tli {
            let next_highest_block = tli.next_back();

            if let Some(handle) = next_highest_block {
                let block = fail_iter!(load_block(
                    self.segment_id,
                    &self.path,
                    &self.descriptor_table,
                    &self.cache,
                    &handle.into_inner(),
                    BlockType::Index,
                    self.compression,
                    #[cfg(feature = "metrics")]
                    &self.metrics,
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

                self.hi_consumer = Some(iter);

                if let Some(item) = next_item {
                    return Some(item);
                }
            }
        }

        // Nothing more found, consume from lo consumer
        if let Some(lo_block) = &mut self.lo_consumer {
            if let Some(item) = lo_block.next_back() {
                return Some(Ok(item));
            }
        }

        None
    }
}
