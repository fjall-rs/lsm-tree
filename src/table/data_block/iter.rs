// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
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
}

impl<'a> Iter<'a> {
    /// Creates a new iterator over a data block.
    #[must_use]
    pub fn new(bytes: &'a [u8], decoder: Decoder<'a, InternalValue, DataBlockParsedItem>) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self { bytes, decoder }
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
    /// (user_key ASC, seqno DESC), this skips restart intervals that can only
    /// contain versions newer than the snapshot, so any visible version for
    /// `needle` will be found within roughly one restart interval of the
    /// resulting position.
    pub fn seek_to_key_seqno(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        self.decoder.inner_mut().seek(
            |head_key, head_seqno| match head_key.cmp(needle) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => head_seqno >= seqno,
                std::cmp::Ordering::Greater => false,
            },
            false,
        )
    }

    pub fn seek(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        // Find the last restart interval whose head precedes (needle, seqno) in
        // internal key order (user_key ASC, seqno DESC).  This lets us skip
        // restart intervals that contain only versions newer than the snapshot,
        // reducing the subsequent linear scan.
        if !self.decoder.inner_mut().seek(
            |head_key, head_seqno| match head_key.cmp(needle) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => head_seqno >= seqno,
                std::cmp::Ordering::Greater => false,
            },
            false,
        ) {
            return false;
        }

        // TODO: make sure we only linear scan over the current restart interval
        // TODO: if we do more steps, something has gone wrong with the seek probably, maybe...?

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes) {
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

    pub fn seek_upper(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        // Reverse-bound seek: position the high scanner at the last restart whose
        // head key is ≤ needle, then walk backwards inside the interval until we
        // find a key ≤ needle.
        //
        // Note: seqno cannot narrow the backward binary search.  Backward
        // iteration visits intervals from the selected one toward index 0, so a
        // tighter predicate would cause later intervals (higher index, older
        // versions of the same key) to be skipped entirely — potentially missing
        // the visible version.
        if !self
            .decoder
            .inner_mut()
            .seek_upper(|head_key, _| head_key <= needle, false)
        {
            return false;
        }

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes) {
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
        // Exclusive lower bound: identical to `seek`, except we must not yield
        // entries equal to `needle`.  The seqno-aware binary search still helps
        // by landing closer to the target position in the restart index.
        if !self.decoder.inner_mut().seek(
            |head_key, head_seqno| match head_key.cmp(needle) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => head_seqno >= seqno,
                std::cmp::Ordering::Greater => false,
            },
            false,
        ) {
            return false;
        }

        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes) {
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

    pub fn seek_upper_exclusive(&mut self, needle: &[u8], _seqno: SeqNo) -> bool {
        // Exclusive upper bound: mirror of `seek_upper`.  Same backward-search
        // limitation applies — seqno cannot narrow the binary search here.
        if !self
            .decoder
            .inner_mut()
            .seek_upper(|head_key, _| head_key <= needle, false)
        {
            return false;
        }

        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes) {
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
