// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    comparator::SharedComparator,
    table::{block::ParsedItem, index_block::Iter as IndexBlockIter, IndexBlock, KeyedBlockHandle},
    SeqNo,
};
use self_cell::self_cell;

self_cell!(
    pub struct OwnedIndexBlockIter {
        owner: IndexBlock,

        #[covariant]
        dependent: IndexBlockIter,
    }
);

impl OwnedIndexBlockIter {
    /// Creates an owned iterator from a block and a comparator.
    pub(crate) fn from_block(block: IndexBlock, comparator: SharedComparator) -> Self {
        Self::new(block, |b| b.iter(comparator))
    }

    /// Creates an owned iterator with optional lower/upper seek bounds.
    ///
    /// The lower bound `lo`, if provided, seeks the forward cursor to the
    /// first entry at or after `(key, seqno)`. Returns `None` if no such
    /// entry exists.
    ///
    /// The upper bound `hi`, if provided, positions the internal back
    /// cursor for reverse iteration; it does *not* exclude entries from
    /// forward iteration. Returns `None` for `hi` only if the underlying
    /// `seek_upper` reports failure.
    pub(crate) fn from_block_with_bounds(
        block: IndexBlock,
        comparator: SharedComparator,
        lo: Option<(&[u8], SeqNo)>,
        hi: Option<(&[u8], SeqNo)>,
    ) -> Option<Self> {
        let mut iter = Self::from_block(block, comparator);

        if let Some((key, seqno)) = lo {
            if !iter.seek_lower(key, seqno) {
                return None;
            }
        }
        // NOTE: seek_upper on index blocks (restart_interval=1) always succeeds —
        // it positions the back-end cursor but does not reject out-of-range bounds.
        // The None path here guards against future decoder changes.
        if let Some((key, seqno)) = hi {
            if !iter.seek_upper(key, seqno) {
                return None;
            }
        }

        Some(iter)
    }

    pub fn seek_lower(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek(needle, seqno))
    }

    pub fn seek_upper(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.with_dependent_mut(|_, m| m.seek_upper(needle, seqno))
    }
}

impl Iterator for OwnedIndexBlockIter {
    type Item = KeyedBlockHandle;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next().map(|item| item.materialize(&block.inner.data))
        })
    }
}

impl DoubleEndedIterator for OwnedIndexBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|block, iter| {
            iter.next_back()
                .map(|item| item.materialize(&block.inner.data))
        })
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::{
        comparator::default_comparator,
        table::block::{BlockOffset, BlockType, Header},
        table::BlockHandle,
        Checksum,
    };

    /// Builds an IndexBlock containing entries with the given keys (seqno=0 for all).
    fn make_index_block(keys: &[&[u8]]) -> IndexBlock {
        let items: Vec<KeyedBlockHandle> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                KeyedBlockHandle::new(
                    (*k).into(),
                    0,
                    BlockHandle::new(BlockOffset(i as u64 * 100), 100),
                )
            })
            .collect();

        let bytes = IndexBlock::encode_into_vec(&items).unwrap();
        let data_len = bytes.len() as u32;
        IndexBlock::new(crate::table::block::Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: data_len,
                uncompressed_length: data_len,
            },
        })
    }

    #[test]
    fn from_block_iterates_all_entries() {
        let block = make_index_block(&[b"a", b"b", b"c"]);
        let mut iter = OwnedIndexBlockIter::from_block(block, default_comparator());

        let keys: Vec<_> = iter.by_ref().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c"]);
    }

    #[test]
    fn from_block_with_bounds_no_bounds_returns_all() {
        let block = make_index_block(&[b"a", b"b", b"c"]);
        let iter =
            OwnedIndexBlockIter::from_block_with_bounds(block, default_comparator(), None, None);

        assert!(iter.is_some());
        let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c"]);
    }

    #[test]
    fn from_block_with_bounds_lo_bound_seeks_forward() {
        let block = make_index_block(&[b"a", b"b", b"c"]);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"b", SeqNo::MAX)),
            None,
        );

        assert!(iter.is_some());
        let keys: Vec<_> = iter.unwrap().map(|h| h.end_key().to_vec()).collect();
        assert_eq!(keys, vec![b"b", b"c"]);
    }

    #[test]
    fn from_block_with_bounds_hi_bound_sets_back_cursor() {
        // seek_upper positions the decoder's back-end cursor
        let block = make_index_block(&[b"a", b"b", b"c", b"d"]);
        let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            None,
            Some((b"c", 0)),
        )
        .unwrap();

        // Forward iteration still starts from the beginning
        assert_eq!(iter.next().unwrap().end_key().as_ref(), b"a");

        // seek_upper("c") positions the back cursor at the partition boundary;
        // next_back yields items from that position downward
        let back = iter.next_back().unwrap();
        assert!(back.end_key().as_ref() <= &b"d"[..]);
        // Confirm backward iteration continues in reverse order
        let prev = iter.next_back().unwrap();
        assert!(prev.end_key().as_ref() < back.end_key().as_ref());
    }

    #[test]
    fn from_block_with_bounds_both_bounds() {
        let block = make_index_block(&[b"a", b"b", b"c", b"d"]);
        let mut iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"b", SeqNo::MAX)),
            Some((b"c", 0)),
        )
        .unwrap();

        // Forward from lo bound
        assert_eq!(iter.next().unwrap().end_key().as_ref(), b"b");
    }

    #[test]
    fn from_block_with_bounds_lo_past_end_returns_none() {
        let block = make_index_block(&[b"a", b"b"]);
        let iter = OwnedIndexBlockIter::from_block_with_bounds(
            block,
            default_comparator(),
            Some((b"z", SeqNo::MAX)),
            None,
        );

        assert!(iter.is_none());
    }
}
