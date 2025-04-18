// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::CompositeHash;
use crate::bloom::bit_array::BitArray;

const CACHE_LINE_BYTES: usize = 64;
pub struct BlockedBloomFilter {
    /// Raw bytes exposed as bit array
    ///
    inner: BitArray,

    /// Number of hash functions
    k: usize,

    /// Number of blocks in the blocked bloom filter
    num_blocks: usize,
}

impl BlockedBloomFilter {
    /// Returns the size of the bloom filter in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.bytes().len()
    }

    /// Returns the amount of hashes used per lookup.
    #[must_use]
    pub fn hash_fn_count(&self) -> usize {
        self.k
    }

    fn from_raw(m: usize, k: usize, bytes: Box<[u8]>) -> Self {
        let num_blocks = m.div_ceil(CACHE_LINE_BYTES);
        Self {
            inner: BitArray::from_bytes(bytes),
            k,
            num_blocks,
        }
    }

    /// Constructs a blocked bloom filter that can hold `n` items
    /// while maintaining a certain false positive rate `fpr`.
    #[must_use]
    pub fn with_fp_rate(n: usize, fpr: f32) -> Self {
        // TODO: m and k is still calculated by traditional standard bloom filter formula
        use std::f32::consts::LN_2;

        assert!(n > 0);

        // NOTE: Some sensible minimum
        let fpr = fpr.max(0.000_001);

        let m = Self::calculate_m(n, fpr);
        let bpk = m / n;
        let k = (((bpk as f32) * LN_2) as usize).max(1);

        let num_blocks = m.div_ceil(CACHE_LINE_BYTES);

        Self {
            inner: BitArray::with_capacity(num_blocks * CACHE_LINE_BYTES),
            k,
            num_blocks,
        }
    }

    /// Constructs a bloom filter that can hold `n` items
    /// with `bpk` bits per key.
    #[must_use]
    pub fn with_bpk(n: usize, bpk: u8) -> Self {
        use std::f32::consts::LN_2;

        assert!(bpk > 0);
        assert!(n > 0);

        let bpk = bpk as usize;

        let m = n * bpk;
        let k = (((bpk as f32) * LN_2) as usize).max(1);

        let num_blocks = m.div_ceil(CACHE_LINE_BYTES);

        Self {
            inner: BitArray::with_capacity(num_blocks * CACHE_LINE_BYTES),
            k,
            num_blocks,
        }
    }

    fn calculate_m(n: usize, fp_rate: f32) -> usize {
        use std::f32::consts::LN_2;

        let n = n as f32;
        let ln2_squared = LN_2.powi(2);

        let numerator = n * fp_rate.ln();
        let m = -(numerator / ln2_squared);

        // Round up to next byte
        ((m / 8.0).ceil() * 8.0) as usize
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

    /// Adds the key to the filter.
    pub fn set_with_hash(&mut self, (mut h1, mut h2): CompositeHash) {
        let block_idx = h1 % (self.num_blocks as u64);

        for i in 1..(self.k as u64) {
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);

            let idx = h1 % (CACHE_LINE_BYTES as u64);

            #[allow(clippy::cast_possible_truncation)]
            self.enable_bit(block_idx as usize, idx as usize);
        }
    }

    /// Returns `true` if the bit at `idx` is `1`.
    fn has_bit(&self, block_idx: usize, idx: usize) -> bool {
        self.inner.get(block_idx * CACHE_LINE_BYTES as usize + idx)
    }

    /// Sets the bit at the given index to `true`.
    fn enable_bit(&mut self, block_idx: usize, idx: usize) {
        self.inner
            .enable(block_idx * CACHE_LINE_BYTES as usize + idx)
    }

    /// Gets the hash of a key.
    #[must_use]
    pub fn get_hash(key: &[u8]) -> CompositeHash {
        let h0 = xxhash_rust::xxh3::xxh3_128(key);
        let h1 = (h0 >> 64) as u64;
        let h2 = h0 as u64;
        (h1, h2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_bloom_basic() {
        let mut filter = BlockedBloomFilter::with_fp_rate(10, 0.0001);

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            assert!(!filter.contains(key));
            filter.set_with_hash(BlockedBloomFilter::get_hash(key));
            assert!(filter.contains(key));
            assert!(!filter.contains(b"asdasdasdasdasdasdasd"));
        }
    }
}
