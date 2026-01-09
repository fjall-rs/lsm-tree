// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::KeyedBlockHandle;
use crate::{
    table::{
        block::BlockType,
        block_index::{iter::OwnedIndexBlockIter, BlockIndexIter},
        util::load_block,
        BlockHandle, IndexBlock,
    },
    Cache, CompressionType, DescriptorTable, GlobalTableId, SeqNo, UserKey,
};
use std::{fs::File, path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::Metrics;

/// Index that translates item keys to data block handles
///
/// The index is loaded on demand.
pub struct VolatileBlockIndex {
    pub(crate) table_id: GlobalTableId,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) pinned_file_descriptor: Option<Arc<File>>,
    pub(crate) descriptor_table: Arc<DescriptorTable>,
    pub(crate) cache: Arc<Cache>,
    pub(crate) handle: BlockHandle,
    pub(crate) compression: CompressionType,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl VolatileBlockIndex {
    pub fn forward_reader(&self, needle: &[u8], seqno: SeqNo) -> Iter {
        let mut iter = Iter::new(self);
        iter.seek_lower(needle, seqno);
        iter
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self)
    }
}

pub struct Iter {
    inner: Option<OwnedIndexBlockIter>,
    table_id: GlobalTableId,
    path: Arc<PathBuf>,
    pinned_file_descriptor: Option<Arc<File>>,
    descriptor_table: Arc<DescriptorTable>,
    cache: Arc<Cache>,
    handle: BlockHandle,
    compression: CompressionType,

    lo: Option<(UserKey, SeqNo)>,
    hi: Option<(UserKey, SeqNo)>,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl Iter {
    fn new(index: &VolatileBlockIndex) -> Self {
        Self {
            inner: None,
            table_id: index.table_id,
            path: index.path.clone(),
            pinned_file_descriptor: index.pinned_file_descriptor.clone(),
            descriptor_table: index.descriptor_table.clone(),
            cache: index.cache.clone(),
            handle: index.handle,
            compression: index.compression,

            lo: None,
            hi: None,

            #[cfg(feature = "metrics")]
            metrics: index.metrics.clone(),
        }
    }
}

impl BlockIndexIter for Iter {
    fn seek_lower(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.lo = Some((key.into(), seqno));
        true
    }

    fn seek_upper(&mut self, key: &[u8], seqno: SeqNo) -> bool {
        self.hi = Some((key.into(), seqno));
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
                self.table_id,
                &self.path,
                self.pinned_file_descriptor.as_ref(),
                &self.descriptor_table,
                &self.cache,
                &self.handle,
                BlockType::Index,
                self.compression,
                #[cfg(feature = "metrics")]
                &self.metrics,
            ));
            let index_block = IndexBlock::new(block);

            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

            if let Some((lo_key, lo_seqno)) = &self.lo {
                if !iter.seek_lower(lo_key, *lo_seqno) {
                    return None;
                }
            }
            if let Some((hi_key, hi_seqno)) = &self.hi {
                if !iter.seek_upper(hi_key, *hi_seqno) {
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
                self.table_id,
                &self.path,
                self.pinned_file_descriptor.as_ref(),
                &self.descriptor_table,
                &self.cache,
                &self.handle,
                BlockType::Index,
                self.compression,
                #[cfg(feature = "metrics")]
                &self.metrics,
            ));
            let index_block = IndexBlock::new(block);

            let mut iter = OwnedIndexBlockIter::new(index_block, IndexBlock::iter);

            if let Some((lo_key, lo_seqno)) = &self.lo {
                if !iter.seek_lower(lo_key, *lo_seqno) {
                    return None;
                }
            }
            if let Some((hi_key, hi_seqno)) = &self.hi {
                if !iter.seek_upper(hi_key, *hi_seqno) {
                    return None;
                }
            }

            let next_item = iter.next_back().map(Ok);

            self.inner = Some(iter);

            next_item
        }
    }
}
