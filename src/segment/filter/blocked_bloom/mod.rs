// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod builder;
use super::{bit_array::BitArrayReader, CACHE_LINE_BYTES};
pub use builder::Builder;

/// Two hashes that are used for double hashing
pub type CompositeHash = (u64, u64);

pub struct BlockedBloomFilter {
    /// Raw bytes exposed as bit array
    inner: BitArrayReader,

    /// Number of hash functions
    k: usize,

    /// Number of blocks in the blocked bloom filter
    num_blocks: usize,
}

// TODO: Implement Encode and Decode for BlockedBloomFilter

impl BlockedBloomFilter {
    /// Size of bloom filter in bytes
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.bytes().len()
    }

    fn from_raw(m: usize, k: usize, slice: crate::Slice) -> Self {
        let num_blocks = m.div_ceil(CACHE_LINE_BYTES);
        Self {
            inner: BitArrayReader::new(slice),
            k,
            num_blocks,
        }
    }

    /// Returns `true` if the hash may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    pub fn contains_hash(&self, (mut h1, mut h2): CompositeHash) -> bool {
        let block_idx = h1 % (self.num_blocks as u64);

        for i in 1..(self.k as u64) {
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);

            let idx = h1 % (CACHE_LINE_BYTES as u64);

            // NOTE: should be in bounds because of modulo
            #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
            if !self.has_bit(block_idx as usize, idx as usize) {
                return false;
            }
        }

        true
    }

    /// Returns `true` if the item may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(Self::get_hash(key))
    }

    /// Returns `true` if the bit at `idx` is `1`.
    fn has_bit(&self, block_idx: usize, idx_in_block: usize) -> bool {
        self.inner
            .get(Builder::get_bit_idx(block_idx, idx_in_block))
    }

    /// Gets the hash of a key.
    pub fn get_hash(key: &[u8]) -> CompositeHash {
        Builder::get_hash(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_bloom_basic() {
        let mut filter = Builder::with_fp_rate(10, 0.0001);
        let keys = [
            b"item0" as &[u8],
            b"item1",
            b"item2",
            b"item3",
            b"item4",
            b"item5",
            b"item6",
            b"item7",
            b"item8",
            b"item9",
        ];

        for key in &keys {
            filter.set_with_hash(Builder::get_hash(key));
        }

        let filter = filter.build();

        for key in &keys {
            assert!(filter.contains(key));
        }

        assert!(!filter.contains(b"asdasdasdasdasdasdasd"));
    }
}
