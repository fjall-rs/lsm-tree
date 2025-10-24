// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::super::bit_array::Builder as BitArrayBuilder;
use crate::{file::MAGIC_BYTES, segment::filter::FilterType};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;

pub fn secondary_hash(h1: u64) -> u64 {
    // Taken from https://github.com/tomtomwombat/fastbloom
    h1.wrapping_shr(32).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95)
}

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct Builder {
    /// Raw bytes exposed as bit array
    inner: BitArrayBuilder,

    /// Bit count
    pub(super) m: usize,

    /// Number of hash functions
    pub(super) k: usize,
}

#[allow(clippy::len_without_is_empty)]
impl Builder {
    // NOTE: We write into a Vec<u8>, so no I/O error can happen
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn build(&self) -> Vec<u8> {
        let mut v = vec![];

        // Write header
        v.write_all(&MAGIC_BYTES).expect("should not fail");

        // NOTE: Filter type
        v.write_u8(FilterType::StandardBloom.into())
            .expect("should not fail");

        // NOTE: Hash type (unused)
        v.write_u8(0).expect("should not fail");

        v.write_u64::<LittleEndian>(self.m as u64)
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

        let m = Self::calculate_m(n, fpr);
        let bpk = m / n;
        let k = (((bpk as f32) * LN_2) as usize).max(1);

        Self {
            inner: BitArrayBuilder::with_capacity(m / 8),
            m,
            k,
        }
    }

    /// Constructs a bloom filter that can hold `n` items
    /// with `bpk` bits per key.
    ///
    /// 10 bits per key is a sensible default.
    #[must_use]
    pub fn with_bpk(n: usize, bpk: f32) -> Self {
        use std::f32::consts::LN_2;

        assert!(bpk > 0.0);
        assert!(n > 0);

        let m = n * (bpk as usize);
        let k = ((bpk * LN_2) as usize).max(1);

        // NOTE: Round up so we don't get too little bits
        let bytes = (m as f32 / 8.0).ceil() as usize;

        Self {
            inner: BitArrayBuilder::with_capacity(bytes),
            m: bytes * 8,
            k,
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

        for i in 1..=(self.k as u64) {
            let idx = h1 % (self.m as u64);

            // NOTE: Even for a large table, filters tend to be pretty small, definitely less than 4 GiB
            #[allow(clippy::cast_possible_truncation)]
            self.inner.enable_bit(idx as usize);

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(i);
        }
    }

    /// Gets the hash of a key.
    #[must_use]
    pub fn get_hash(key: &[u8]) -> u64 {
        crate::hash::hash64(key)
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
