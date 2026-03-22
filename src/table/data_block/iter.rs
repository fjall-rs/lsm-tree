// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    comparator::SharedComparator,
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt},
    table::{
        block::{Decoder, ParsedItem},
        data_block::DataBlockParsedItem,
    },
    InternalValue, SeqNo,
};

/// The data block iterator handles double-ended scans over a data block
pub struct Iter<'a> {
    bytes: &'a [u8],
    decoder:
        DoubleEndedPeekable<DataBlockParsedItem, Decoder<'a, InternalValue, DataBlockParsedItem>>,
    comparator: SharedComparator,
}

impl<'a> Iter<'a> {
    /// Creates a new iterator over a data block.
    #[must_use]
    pub fn new(
        bytes: &'a [u8],
        decoder: Decoder<'a, InternalValue, DataBlockParsedItem>,
        comparator: SharedComparator,
    ) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self {
            bytes,
            decoder,
            comparator,
        }
    }

    /// Seek the iterator to an byte offset.
    ///
    /// This is used when the hash index returns a hit.
    pub fn seek_to_offset(&mut self, offset: usize) -> bool {
        self.decoder.inner_mut().set_lo_offset(offset);
        true
    }

    /// Seeks to the last restart interval whose head key is strictly below the
    /// target `needle`, or equal to it with a seqno that is at least the given
    /// snapshot boundary.
    ///
    /// Here `seqno` is a snapshot boundary: point reads return the first item
    /// with `item.seqno < seqno`. Using the internal key ordering
    /// (`user_key` ASC, `seqno` DESC), this skips restart intervals that can only
    /// contain versions newer than the snapshot, so any visible version for
    /// `needle` will be found within roughly one restart interval of the
    /// resulting position.
    pub fn seek_to_key_seqno(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        self.decoder.inner_mut().seek(
            |head_key, head_seqno| match cmp.compare(head_key, needle) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => head_seqno >= seqno,
                std::cmp::Ordering::Greater => false,
            },
            false,
        )
    }

    pub fn seek(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        // Reuse the seqno-aware binary search from `seek_to_key_seqno`, then
        // follow up with a linear scan to position at the exact key.
        if !self.seek_to_key_seqno(needle, seqno) {
            return false;
        }

        // TODO: make sure we only linear scan over the current restart interval
        // TODO: if we do more steps, something has gone wrong with the seek probably, maybe...?

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                std::cmp::Ordering::Equal => {
                    return true;
                }
                std::cmp::Ordering::Greater => {
                    return false;
                }
                std::cmp::Ordering::Less => {
                    // Continue

                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next().expect("should exist");
                }
            }
        }
    }

    /// Reverse inclusive seek: position at the last key `<= needle`.
    ///
    /// `seqno` is accepted for API uniformity with the forward seek methods
    /// ([`seek`], [`seek_exclusive`]) but is **intentionally unused** here.
    /// Backward binary search cannot leverage seqno because intervals are
    /// visited from the selected one toward index 0 — a tighter predicate
    /// would skip intervals that may contain the visible version.
    pub fn seek_upper(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        if !self.decoder.inner_mut().seek_upper(
            |head_key, _| cmp.compare(head_key, needle) != std::cmp::Ordering::Greater,
            false,
        ) {
            return false;
        }

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                std::cmp::Ordering::Equal => {
                    return true;
                }
                std::cmp::Ordering::Less => {
                    return false;
                }
                std::cmp::Ordering::Greater => {
                    // Continue

                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next_back().expect("should exist");
                }
            }
        }
    }

    pub fn seek_exclusive(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        // Exclusive lower bound: same seqno-aware binary search, but the linear
        // scan below skips entries equal to `needle`.
        if !self.seek_to_key_seqno(needle, seqno) {
            return false;
        }

        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                std::cmp::Ordering::Greater => {
                    return true;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next().expect("should exist");
                }
            }
        }
    }

    /// Reverse exclusive seek: position at the last key `< needle`.
    ///
    /// See [`seek_upper`] for why `seqno` is accepted but unused in reverse
    /// seeks.
    pub fn seek_upper_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        let cmp = &self.comparator;
        if !self.decoder.inner_mut().seek_upper(
            |head_key, _| cmp.compare(head_key, needle) != std::cmp::Ordering::Greater,
            false,
        ) {
            return false;
        }

        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes, self.comparator.as_ref()) {
                std::cmp::Ordering::Less => {
                    return true;
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {
                    #[expect(
                        clippy::expect_used,
                        reason = "we peeked a value successfully, so there must be a next item in the stream"
                    )]
                    self.decoder.next_back().expect("should exist");
                }
            }
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = DataBlockParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.decoder.next_back()
    }
}
