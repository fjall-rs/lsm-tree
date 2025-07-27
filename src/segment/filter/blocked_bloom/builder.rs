// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::super::bit_array::Builder as BitArrayBuilder;
use crate::segment::filter::{bit_array::BitArrayReader, CACHE_LINE_BYTES};

/// Two hashes that are used for double hashing
pub type CompositeHash = (u64, u64);

#[derive(Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub struct Builder {
    /// Raw bytes exposed as bit array
    inner: BitArrayBuilder,

    /// Number of hash functions
    k: usize,

    /// Number of blocks in the blocked bloom filter
    num_blocks: usize,
}

#[allow(clippy::len_without_is_empty)]
impl Builder {
    #[must_use]
    pub fn build(self) -> BlockedBloomFilter {
        BlockedBloomFilter {
            inner: BitArrayReader::new(self.inner.bytes().into()),
            k: self.k,
            num_blocks: self.num_blocks,
        }
    }

    /// Constructs a bloom filter that can hold `n` items
    /// while maintaining a certain false positive rate `fpr`.
    #[must_use]
    pub fn with_fp_rate(n: usize, fpr: f32) -> Self {
        use std::f32::consts::LN_2;

        assert!(n > 0);

        // NOTE: Some sensible minimum
        let fpr = fpr.max(0.000_001);

        // TODO: m and k is still calculated by traditional standard bloom filter formula
        let m = Self::calculate_m(n, fpr);
        let bpk = m / n;
        let k = (((bpk as f32) * LN_2) as usize).max(1);

        let num_blocks = m.div_ceil(CACHE_LINE_BYTES * 8);

        Self {
            inner: BitArrayBuilder::with_capacity(num_blocks * CACHE_LINE_BYTES),
            k,
            num_blocks,
        }
    }

    /// Constructs a bloom filter that can hold `n` items
    /// with `bpk` bits per key.
    ///
    /// 10 bits per key is a sensible default.
    #[must_use]
    pub fn with_bpk(n: usize, bpk: u8) -> Self {
        use std::f32::consts::LN_2;

        assert!(bpk > 0);
        assert!(n > 0);

        let bpk = bpk as usize;

        let m = n * bpk;
        let k = (((bpk as f32) * LN_2) as usize).max(1);

        let num_blocks = m.div_ceil(CACHE_LINE_BYTES * 8);

        Self {
            inner: BitArrayBuilder::with_capacity(num_blocks * CACHE_LINE_BYTES),
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

    /// Adds the key to the filter.
    pub fn set_with_hash(&mut self, (mut h1, mut h2): CompositeHash) {
        let block_idx = h1 % (self.num_blocks as u64);

        for i in 1..(self.k as u64) {
            let idx = h1 % (CACHE_LINE_BYTES as u64 * 8);

            self.inner
                .enable_bit(Self::get_bit_idx(block_idx as usize, idx as usize));

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(i);
        }
    }

    pub fn get_bit_idx(block_idx: usize, idx_in_block: usize) -> usize {
        block_idx * CACHE_LINE_BYTES * 8 + idx_in_block
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
    use test_log::test;

    #[test]
    fn bloom_calculate_m() {
        assert_eq!(9_592, Builder::calculate_m(1_000, 0.01));
        assert_eq!(4_800, Builder::calculate_m(1_000, 0.1));
        assert_eq!(4_792_536, Builder::calculate_m(1_000_000, 0.1));
    }
}
