mod bit_array;

use crate::serde::{Deserializable, Serializable};
use crate::{DeserializeError, SerializeError};
use bit_array::BitArray;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use seahash::SeaHasher;
use std::hash::Hasher;
use std::io::{Read, Write};

pub const BLOOM_HEADER_MAGIC: &[u8] = &[b'F', b'J', b'L', b'L', b'S', b'B', b'F', b'1'];

pub type CompositeHash = (u64, u64);

/// A standard bloom filter
///
/// Allows buffering the key hashes before actual filter construction
/// which is needed to properly calculate the filter size, as the amount of items
/// are unknown during segment construction.
#[derive(Debug, Eq, PartialEq)]
pub struct BloomFilter {
    /// Raw bytes exposed as bit array
    inner: BitArray,

    /// Bit count
    m: usize,

    /// Number of hash functions
    k: usize,
}

impl Serializable for BloomFilter {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(BLOOM_HEADER_MAGIC)?;

        // NOTE: Filter type (unused)
        writer.write_u8(0)?;

        writer.write_u64::<BigEndian>(self.m as u64)?;
        writer.write_u64::<BigEndian>(self.k as u64)?;
        writer.write_all(self.inner.bytes())?;
        Ok(())
    }
}

impl Deserializable for BloomFilter {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Check header
        let mut magic = [0u8; BLOOM_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != BLOOM_HEADER_MAGIC {
            return Err(DeserializeError::InvalidHeader("BloomFilter"));
        }

        // NOTE: Filter type (unused)
        let filter_type = reader.read_u8()?;
        assert_eq!(0, filter_type, "Invalid filter type");

        let m = reader.read_u64::<BigEndian>()? as usize;
        let k = reader.read_u64::<BigEndian>()? as usize;

        let mut bytes = vec![0; m / 8];
        reader.read_exact(&mut bytes)?;

        Ok(Self::from_raw(m, k, bytes.into_boxed_slice()))
    }
}

impl BloomFilter {
    /// Size of bloom filter in bytes
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn from_raw(m: usize, k: usize, bytes: Box<[u8]>) -> Self {
        Self {
            inner: BitArray::from_bytes(bytes),
            m,
            k,
        }
    }

    pub(crate) fn calculate_m(n: usize, fp_rate: f32) -> usize {
        use std::f32::consts::LN_2;

        let n = n as f32;
        let ln2_squared = LN_2.powi(2);

        let m = -(n * fp_rate.ln() / ln2_squared);
        ((m / 8.0).ceil() * 8.0) as usize
    }

    /// Heuristically get the somewhat-optimal k value for a given desired FPR
    fn get_k_heuristic(fp_rate: f32) -> usize {
        match fp_rate {
            _ if fp_rate > 0.4 => 1,
            _ if fp_rate > 0.2 => 2,
            _ if fp_rate > 0.1 => 3,
            _ if fp_rate > 0.05 => 4,
            _ if fp_rate > 0.03 => 5,
            _ if fp_rate > 0.02 => 5,
            _ if fp_rate > 0.01 => 7,
            _ if fp_rate > 0.001 => 10,
            _ if fp_rate > 0.000_1 => 13,
            _ if fp_rate > 0.000_01 => 17,
            _ => 20,
        }
    }

    /// Constructs a bloom filter that can hold `item_count` items
    /// while maintaining a certain false positive rate.
    #[must_use]
    pub fn with_fp_rate(item_count: usize, fp_rate: f32) -> Self {
        // NOTE: Some sensible minimum
        let fp_rate = fp_rate.max(0.000_001);

        let k = Self::get_k_heuristic(fp_rate);
        let m = Self::calculate_m(item_count, fp_rate);

        Self {
            inner: BitArray::with_capacity(m / 8),
            m,
            k,
        }
    }

    /// Returns `true` if the hash may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    pub fn contains_hash(&self, hash: CompositeHash) -> bool {
        let (mut h1, mut h2) = hash;

        for i in 0..(self.k as u64) {
            let idx = h1 % (self.m as u64);

            // NOTE: should be in bounds because of modulo
            #[allow(clippy::expect_used)]
            if !self.inner.get(idx as usize) {
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

    /// Adds the key to the filter
    pub fn set_with_hash(&mut self, (mut h1, mut h2): CompositeHash) {
        for i in 0..(self.k as u64) {
            let idx = h1 % (self.m as u64);

            self.enable_bit(idx as usize);

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_add(i);
        }
    }

    /// Sets the bit at the given index to `true`
    fn enable_bit(&mut self, idx: usize) {
        self.inner.set(idx, true);
    }

    /// Gets the hash of a key
    #[must_use]
    pub fn get_hash(key: &[u8]) -> CompositeHash {
        let mut hasher = SeaHasher::default();
        hasher.write(key);
        let h1 = hasher.finish();

        hasher.write(key);
        let h2 = hasher.finish();

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

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            filter.set_with_hash(BloomFilter::get_hash(key));
        }

        filter.serialize(&mut file)?;
        file.sync_all()?;
        drop(file);

        let mut file = File::open(&path)?;
        let filter_copy = BloomFilter::deserialize(&mut file)?;

        assert_eq!(filter, filter_copy);

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
    fn bloom_fpr() {
        let item_count = 1_000_000;
        let fpr = 0.01;

        let mut filter = BloomFilter::with_fp_rate(item_count, fpr);

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

        assert!((10_000 - false_positives) < 200);
    }
}
