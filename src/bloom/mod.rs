// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod bit_array;

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
};
use bit_array::BitArray;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Two hashes that are used for double hashing
pub type CompositeHash = (u64, u64);

/// A standard bloom filter
///
/// Allows buffering the key hashes before actual filter construction
/// which is needed to properly calculate the filter size, as the amount of items
/// are unknown during segment construction.
///
/// The filter uses double hashing instead of `k` hash functions, see:
/// <https://fjall-rs.github.io/post/bloom-filter-hash-sharing>
#[derive(Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub struct BloomFilter {
    /// Raw bytes exposed as bit array
    inner: BitArray,

    /// Bit count
    m: usize,

    /// Number of hash functions
    k: usize,
}

impl Encode for BloomFilter {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        // NOTE: Filter type
        writer.write_u8(0)?;

        // NOTE: Hash type (unused)
        writer.write_u8(0)?;

        writer.write_u64::<BigEndian>(self.m as u64)?;
        writer.write_u64::<BigEndian>(self.k as u64)?;
        writer.write_all(self.inner.bytes())?;

        Ok(())
    }
}

impl Decode for BloomFilter {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(DecodeError::InvalidHeader("BloomFilter"));
        }

        // NOTE: Filter type
        let filter_type = reader.read_u8()?;
        assert_eq!(0, filter_type, "Invalid filter type");

        // NOTE: Hash type (unused)
        let hash_type = reader.read_u8()?;
        assert_eq!(0, hash_type, "Invalid bloom hash type");

        let m = reader.read_u64::<BigEndian>()? as usize;
        let k = reader.read_u64::<BigEndian>()? as usize;

        let mut bytes = vec![0; m / 8];
        reader.read_exact(&mut bytes)?;

        Ok(Self::from_raw(m, k, bytes.into_boxed_slice()))
    }
}

#[allow(clippy::len_without_is_empty)]
impl BloomFilter {
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
        Self {
            inner: BitArray::from_bytes(bytes),
            m,
            k,
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

        let m = Self::calculate_m(n, fpr);
        let bpk = m / n;
        let k = (((bpk as f32) * LN_2) as usize).max(1);

        Self {
            inner: BitArray::with_capacity(m / 8),
            m,
            k,
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

        // NOTE: Round up so we don't get too little bits
        let bytes = (m as f32 / 8.0).ceil() as usize;

        Self {
            inner: BitArray::with_capacity(bytes),
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

    /// Returns `true` if the hash may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    pub fn contains_hash(&self, (mut h1, mut h2): CompositeHash) -> bool {
        for i in 0..(self.k as u64) {
            let idx = h1 % (self.m as u64);

            // NOTE: should be in bounds because of modulo
            #[allow(clippy::expect_used)]
            if !self.has_bit(idx as usize) {
                return false;
            }

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
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
        for i in 0..(self.k as u64) {
            let idx = h1 % (self.m as u64);

            self.enable_bit(idx as usize);

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
        }
    }

    /// Returns `true` if the bit at `idx` is `1`.
    fn has_bit(&self, idx: usize) -> bool {
        self.inner.get(idx)
    }

    /// Sets the bit at the given index to `true`.
    fn enable_bit(&mut self, idx: usize) {
        self.inner.enable(idx);
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
    use std::fs::File;
    use test_log::test;

    #[test]
    fn bloom_serde_round_trip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("bf");
        let mut file = File::create(&path)?;

        let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

        let keys = &[
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ];

        for key in keys {
            filter.set_with_hash(BloomFilter::get_hash(*key));
        }

        for key in keys {
            assert!(filter.contains(&**key));
        }
        assert!(!filter.contains(b"asdasads"));
        assert!(!filter.contains(b"item10"));
        assert!(!filter.contains(b"cxycxycxy"));

        filter.encode_into(&mut file)?;
        file.sync_all()?;
        drop(file);

        let mut file = File::open(&path)?;
        let filter_copy = BloomFilter::decode_from(&mut file)?;

        assert_eq!(filter, filter_copy);

        for key in keys {
            assert!(filter.contains(&**key));
        }
        assert!(!filter_copy.contains(b"asdasads"));
        assert!(!filter_copy.contains(b"item10"));
        assert!(!filter_copy.contains(b"cxycxycxy"));

        Ok(())
    }

    #[test]
    fn bloom_calculate_m() {
        assert_eq!(9_592, BloomFilter::calculate_m(1_000, 0.01));
        assert_eq!(4_800, BloomFilter::calculate_m(1_000, 0.1));
        assert_eq!(4_792_536, BloomFilter::calculate_m(1_000_000, 0.1));
    }

    #[test]
    fn bloom_basic() {
        let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            assert!(!filter.contains(key));
            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));

            assert!(!filter.contains(b"asdasdasdasdasdasdasd"));
        }
    }

    #[test]
    fn bloom_bpk() {
        let item_count = 1_000;
        let bpk = 5;

        let mut filter = BloomFilter::with_bpk(item_count, bpk);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));
        }

        let mut false_positives = 0;

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            if filter.contains(key) {
                false_positives += 1;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let fpr = false_positives as f32 / item_count as f32;
        assert!(fpr < 0.13);
    }

    #[test]
    fn bloom_fpr() {
        let item_count = 100_000;
        let wanted_fpr = 0.1;

        let mut filter = BloomFilter::with_fp_rate(item_count, wanted_fpr);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));
        }

        let mut false_positives = 0;

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            if filter.contains(key) {
                false_positives += 1;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let fpr = false_positives as f32 / item_count as f32;
        assert!(fpr > 0.05);
        assert!(fpr < 0.13);
    }

    #[test]
    fn bloom_fpr_2() {
        let item_count = 100_000;
        let wanted_fpr = 0.5;

        let mut filter = BloomFilter::with_fp_rate(item_count, wanted_fpr);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(BloomFilter::get_hash(key));
            assert!(filter.contains(key));
        }

        let mut false_positives = 0;

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            if filter.contains(key) {
                false_positives += 1;
            }
        }

        #[allow(clippy::cast_precision_loss)]
        let fpr = false_positives as f32 / item_count as f32;
        assert!(fpr > 0.45);
        assert!(fpr < 0.55);
    }
}
