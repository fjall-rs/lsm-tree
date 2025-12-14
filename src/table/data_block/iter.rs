// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt},
    table::{
        block::{Decoder, ParsedItem},
        data_block::DataBlockParsedItem,
    },
    InternalValue,
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

    pub fn seek(&mut self, needle: &[u8]) -> bool {
        // Find the restart interval whose head key is the last one strictly below `needle`.
        // The decoder then performs a linear scan within that interval; we stop as soon as we
        // reach a key ≥ needle. This minimizes parsing work while preserving correctness.
        if !self
            .decoder
            .inner_mut()
            .seek(|head_key, _| head_key < needle, false)
        {
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

    pub fn seek_upper(&mut self, needle: &[u8]) -> bool {
        // Reverse-bound seek: position the high scanner at the first restart whose head key is
        // ≤ needle, then walk backwards inside the interval until we find a key ≤ needle.
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

    pub fn seek_exclusive(&mut self, needle: &[u8]) -> bool {
        // Exclusive lower bound: identical to `seek`, except we must not yield entries equal to
        // `needle`. We therefore keep consuming while keys compare equal and only stop once we
        // observe a strictly greater key.
        if !self
            .decoder
            .inner_mut()
            .seek(|head_key, _| head_key < needle, false)
        {
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

    pub fn seek_upper_exclusive(&mut self, needle: &[u8]) -> bool {
        // Exclusive upper bound: mirror of `seek_upper`. We must not include entries equal to
        // `needle`, so we consume equals from the high end until we see a strictly smaller key.
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
