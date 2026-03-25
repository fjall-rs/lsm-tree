// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::comparator::SharedComparator;
use crate::encryption::EncryptionProvider;
use crate::file_accessor::FileAccessor;
use crate::table::{IndexBlock, KeyedBlockHandle};
use crate::SeqNo;
use crate::{
    table::{
        block::BlockType,
        block_index::{iter::OwnedIndexBlockIter, BlockIndexIter},
        util::load_block,
    },
    Cache, CompressionType, GlobalTableId, UserKey,
};
use std::{path::PathBuf, sync::Arc};

#[cfg(feature = "metrics")]
use crate::Metrics;

/// Index that translates item keys to data block handles
///
/// Only the top-level index is loaded into memory.
pub struct TwoLevelBlockIndex {
    pub(crate) top_level_index: IndexBlock,
    pub(crate) table_id: GlobalTableId,
    pub(crate) path: Arc<PathBuf>,
    pub(crate) file_accessor: FileAccessor,
    pub(crate) cache: Arc<Cache>,
    pub(crate) compression: CompressionType,
    pub(crate) encryption: Option<Arc<dyn EncryptionProvider>>,
    pub(crate) comparator: SharedComparator,

    #[cfg(feature = "metrics")]
    pub(crate) metrics: Arc<Metrics>,
}

impl TwoLevelBlockIndex {
    pub fn iter(&self) -> Iter {
        Iter {
            tli_block: self.top_level_index.clone(),
            tli: None,
            lo_consumer: None,
            hi_consumer: None,
            lo: None,
            hi: None,
            table_id: self.table_id,
            path: self.path.clone(),
            file_accessor: self.file_accessor.clone(),
            cache: self.cache.clone(),
            compression: self.compression,
            encryption: self.encryption.clone(),
            comparator: self.comparator.clone(),

            #[cfg(feature = "metrics")]
            metrics: self.metrics.clone(),
        }
    }
}

pub struct Iter {
    tli_block: IndexBlock,
    tli: Option<OwnedIndexBlockIter>,

    lo_consumer: Option<OwnedIndexBlockIter>,
    hi_consumer: Option<OwnedIndexBlockIter>,

    lo: Option<(UserKey, SeqNo)>,
    hi: Option<(UserKey, SeqNo)>,

    table_id: GlobalTableId,
    path: Arc<PathBuf>,
    file_accessor: FileAccessor,
    cache: Arc<Cache>,
    compression: CompressionType,
    encryption: Option<Arc<dyn EncryptionProvider>>,
    comparator: SharedComparator,

    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
}

impl Iter {
    fn init_tli(&mut self) -> bool {
        let lo = self.lo.as_ref().map(|(k, s)| (k.as_ref(), *s));
        let hi = self.hi.as_ref().map(|(k, s)| (k.as_ref(), *s));

        if let Some(it) = OwnedIndexBlockIter::from_block_with_bounds(
            self.tli_block.clone(),
            self.comparator.clone(),
            lo,
            hi,
        ) {
            self.tli = Some(it);
            true
        } else {
            false
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
                    self.table_id,
                    &self.path,
                    &self.file_accessor,
                    &self.cache,
                    &handle.into_inner(),
                    BlockType::Index,
                    self.compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                    #[cfg(feature = "metrics")]
                    &self.metrics,
                ));
                let index_block = IndexBlock::new(block);
                let lo = self.lo.as_ref().map(|(k, s)| (k.as_ref(), *s));
                let hi = self.hi.as_ref().map(|(k, s)| (k.as_ref(), *s));

                let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
                    index_block,
                    self.comparator.clone(),
                    lo,
                    hi,
                )?;

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
                    self.table_id,
                    &self.path,
                    &self.file_accessor,
                    &self.cache,
                    &handle.into_inner(),
                    BlockType::Index,
                    self.compression,
                    self.encryption.as_deref(),
                    #[cfg(zstd_any)]
                    None,
                    #[cfg(feature = "metrics")]
                    &self.metrics,
                ));
                let index_block = IndexBlock::new(block);
                let lo = self.lo.as_ref().map(|(k, s)| (k.as_ref(), *s));
                let hi = self.hi.as_ref().map(|(k, s)| (k.as_ref(), *s));

                let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
                    index_block,
                    self.comparator.clone(),
                    lo,
                    hi,
                )?;

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
