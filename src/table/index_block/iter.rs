// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    comparator::SharedComparator,
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt},
    table::{block::Decoder, index_block::IndexBlockParsedItem, KeyedBlockHandle},
    SeqNo,
};

pub struct Iter<'a> {
    decoder: DoubleEndedPeekable<
        IndexBlockParsedItem,
        Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>,
    >,
    comparator: SharedComparator,
}

impl<'a> Iter<'a> {
    #[must_use]
    pub fn new(
        decoder: Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>,
        comparator: SharedComparator,
    ) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self {
            decoder,
            comparator,
        }
    }

    pub fn seek(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        self.decoder.inner_mut().seek(
            |end_key, s| match cmp.compare(end_key, needle) {
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => s >= seqno,
            },
            true,
        )
    }

    pub fn seek_upper(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        self.decoder.inner_mut().seek_upper(
            |end_key, _s| cmp.compare(end_key, needle) != std::cmp::Ordering::Greater,
            true,
        )
    }
}

impl Iterator for Iter<'_> {
    type Item = IndexBlockParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.decoder.next_back()
    }
}

// Unit tests for IndexBlock::Iter seek/seek_upper behavior are covered by
// integration tests in tests/custom_comparator.rs (which exercise the full
// block-index → data-block path with both default and custom comparators)
// and by the existing table-level tests in src/table/tests.rs.
