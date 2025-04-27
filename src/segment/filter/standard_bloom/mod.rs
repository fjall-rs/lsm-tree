use super::{bit_array::BitArrayReader, AMQFilter, BloomFilter, BloomFilterType};
use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

mod builder;

pub use builder::{Builder, CompositeHash};

/// A standard bloom filter
///
/// Allows buffering the key hashes before actual filter construction
/// which is needed to properly calculate the filter size, as the amount of items
/// are unknown during segment construction.
///
/// The filter uses double hashing instead of `k` hash functions, see:
/// <https://fjall-rs.github.io/post/bloom-filter-hash-sharing>
#[derive(Debug, PartialEq)]
pub struct StandardBloomFilter {
    /// Raw bytes exposed as bit array
    inner: BitArrayReader,

    /// Bit count
    m: usize,

    /// Number of hash functions
    k: usize,
}

impl AMQFilter for StandardBloomFilter {
    /// Size of bloom filter in bytes.
    #[must_use]
    fn len(&self) -> usize {
        self.inner.bytes().len()
    }

    /// Returns the raw bytes of the filter.
    fn bytes(&self) -> &[u8] {
        self.inner.bytes()
    }

    /// Returns `true` if the item may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(Self::get_hash(key))
    }

    /// Returns `true` if the hash may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    fn contains_hash(&self, hash: CompositeHash) -> bool {
        let (mut h1, mut h2) = hash;

        for i in 1..=(self.k as u64) {
            let idx = h1 % (self.m as u64);

            // NOTE: should be in bounds because of modulo
            #[allow(clippy::expect_used)]
            if !self.has_bit(idx as usize) {
                return false;
            }

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(i);
        }

        true
    }
}

impl Encode for StandardBloomFilter {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        writer.write_u8(BloomFilterType::StandardBloom as u8)?;

        // NOTE: Hash type (unused)
        writer.write_u8(0)?;

        writer.write_u64::<LittleEndian>(self.m as u64)?;
        writer.write_u64::<LittleEndian>(self.k as u64)?;
        writer.write_all(self.inner.bytes())?;

        Ok(())
    }
}

#[allow(clippy::len_without_is_empty)]
impl StandardBloomFilter {
    // To be used by AMQFilter after magic bytes and filter type have been read and parsed
    pub(super) fn decode_from<R: Read>(reader: &mut R) -> Result<BloomFilter, DecodeError> {
        // NOTE: Hash type (unused)
        let hash_type = reader.read_u8()?;
        assert_eq!(0, hash_type, "Invalid bloom hash type");

        let m = reader.read_u64::<LittleEndian>()? as usize;
        let k = reader.read_u64::<LittleEndian>()? as usize;

        let mut bytes = vec![0; m / 8];
        reader.read_exact(&mut bytes)?;

        Ok(BloomFilter::StandardBloom(Self::from_raw(
            m,
            k,
            bytes.into(),
        )))
    }

    fn from_raw(m: usize, k: usize, slice: crate::Slice) -> Self {
        Self {
            inner: BitArrayReader::new(slice),
            m,
            k,
        }
    }

    /// Returns `true` if the bit at `idx` is `1`.
    fn has_bit(&self, idx: usize) -> bool {
        self.inner.get(idx)
    }

    /// Gets the hash of a key.
    fn get_hash(key: &[u8]) -> CompositeHash {
        Builder::get_hash(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::segment::filter::{AMQFilterBuilder, BloomFilter};

    use super::*;
    use std::fs::File;
    use test_log::test;

    #[test]
    fn bloom_serde_round_trip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("bf");
        let mut file = File::create(&path)?;

        let mut filter = Builder::with_fp_rate(10, 0.0001);

        let keys = &[
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ];

        for key in keys {
            filter.set_with_hash(StandardBloomFilter::get_hash(*key));
        }

        let filter = filter.build();

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
        let filter_copy = AMQFilterBuilder::decode_from(&mut file)?;

        assert_eq!(filter.inner.bytes(), filter_copy.bytes());
        assert!(matches!(filter_copy, BloomFilter::StandardBloom(_)));

        for key in keys {
            assert!(filter.contains(&**key));
        }
        assert!(!filter_copy.contains(b"asdasads"));
        assert!(!filter_copy.contains(b"item10"));
        assert!(!filter_copy.contains(b"cxycxycxy"));

        Ok(())
    }

    #[test]
    fn bloom_basic() {
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

    #[test]
    fn bloom_bpk() {
        let item_count = 1_000;
        let bpk = 5;

        let mut filter = Builder::with_bpk(item_count, bpk);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(Builder::get_hash(key));
        }

        let filter = filter.build();

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

        let mut filter = Builder::with_fp_rate(item_count, wanted_fpr);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(Builder::get_hash(key));
        }

        let filter = filter.build();

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

        let mut filter = Builder::with_fp_rate(item_count, wanted_fpr);

        for key in (0..item_count).map(|_| nanoid::nanoid!()) {
            let key = key.as_bytes();

            filter.set_with_hash(Builder::get_hash(key));
        }

        let filter = filter.build();

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
