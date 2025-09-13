// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::super::bit_array::Builder as BitArrayBuilder;
use super::super::standard_bloom::builder::secondary_hash;
use crate::{
    file::MAGIC_BYTES,
    segment::filter::{blocked_bloom::CACHE_LINE_BYTES, FilterType},
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct Builder {
    /// Raw bytes exposed as bit array
    inner: BitArrayBuilder,

    /// Number of hash functions
    pub(crate) k: usize,

    /// Number of blocks in the blocked bloom filter
    pub(crate) num_blocks: usize,
}

#[allow(clippy::len_without_is_empty)]
impl Builder {
    #[must_use]
    pub fn build(&self) -> Vec<u8> {
        let mut v = vec![];

        // Write header
        v.write_all(&MAGIC_BYTES).expect("should not fail");

        // NOTE: Filter type
        v.write_u8(FilterType::BlockedBloom.into())
            .expect("should not fail");

        // NOTE: Hash type (unused)
        v.write_u8(0).expect("should not fail");

        v.write_u64::<LittleEndian>(self.num_blocks as u64)
            .expect("should not fail");
        v.write_u64::<LittleEndian>(self.k as u64)
            .expect("should not fail");
        v.write_all(self.inner.bytes()).expect("should not fail");

        v
    }

    /// Constructs a bloom filter that can hold `n` items
    /// while maintaining a certain false positive rate `fpr`.
    #[must_use]
    pub fn with_fp_rate(n: usize, fpr: f32) -> Self {
        use std::f32::consts::LN_2;

        assert!(n > 0);

        // NOTE: Some sensible minimum
        let fpr = fpr.max(0.000_000_1);

        // NOTE: We add ~5-25% more bits to account for blocked bloom filters being a bit less accurate
        // See https://dl.acm.org/doi/10.1145/1498698.1594230
        let bonus = match fpr {
            _ if fpr <= 0.001 => 1.25,
            _ if fpr <= 0.01 => 1.2,
            _ if fpr <= 0.1 => 1.1,
            _ => 1.05,
        };

        let m = ((Self::calculate_m(n, fpr)) as f32 * bonus) as usize;
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
    pub fn set_with_hash(&mut self, mut h1: u64) {
        let mut h2 = secondary_hash(h1);

        let block_idx = h1 % (self.num_blocks as u64);

        for i in 1..(self.k as u64) {
            let idx = h1 % (CACHE_LINE_BYTES as u64 * 8);

            self.inner
                .enable_bit(Self::get_bit_idx(block_idx as usize, idx as usize));

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(i);
        }
    }

    #[must_use]
    pub fn get_bit_idx(block_idx: usize, idx_in_block: usize) -> usize {
        block_idx * CACHE_LINE_BYTES * 8 + idx_in_block
    }

    /// Gets the hash of a key.
    #[must_use]
    pub fn get_hash(key: &[u8]) -> u64 {
        super::super::standard_bloom::Builder::get_hash(key)
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
