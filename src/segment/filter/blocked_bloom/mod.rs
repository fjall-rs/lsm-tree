// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod builder;
use super::{bit_array::BitArrayReader, AMQFilter, BloomFilter, BloomFilterType, CACHE_LINE_BYTES};
use crate::{
    coding::{DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
};
pub use builder::Builder;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use core::num;
use std::io::{Read, Write};

/// Two hashes that are used for double hashing
pub type CompositeHash = (u64, u64);

#[derive(Debug, PartialEq)]
pub struct BlockedBloomFilter {
    /// Raw bytes exposed as bit array
    inner: BitArrayReader,

    /// Number of hash functions
    k: usize,

    /// Number of blocks in the blocked bloom filter
    num_blocks: usize,
}

impl AMQFilter for BlockedBloomFilter {
    fn bytes(&self) -> &[u8] {
        self.inner.bytes()
    }

    /// Size of bloom filter in bytes
    #[must_use]
    fn len(&self) -> usize {
        self.inner.bytes().len()
    }

    /// Returns `true` if the hash may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    fn contains_hash(&self, (mut h1, mut h2): CompositeHash) -> bool {
        let block_idx = h1 % (self.num_blocks as u64);

        for i in 1..(self.k as u64) {
            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(i);

            let bit_idx = h1 % (CACHE_LINE_BYTES as u64 * 8);

            // NOTE: should be in bounds because of modulo
            #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
            if !self.has_bit(block_idx as usize, bit_idx as usize) {
                return false;
            }
        }

        true
    }

    /// Returns `true` if the item may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(Self::get_hash(key))
    }
}

impl Encode for BlockedBloomFilter {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        writer.write_u8(BloomFilterType::BlockedBloom as u8)?;

        // NOTE: Hash type (unused)
        writer.write_u8(0)?;

        writer.write_u64::<LittleEndian>(self.num_blocks as u64)?;
        writer.write_u64::<LittleEndian>(self.k as u64)?;
        writer.write_all(self.inner.bytes())?;

        Ok(())
    }
}

impl BlockedBloomFilter {
    // To be used by AMQFilter after magic bytes and filter type have been read and parsed
    pub(super) fn decode_from<R: Read>(reader: &mut R) -> Result<BloomFilter, DecodeError> {
        // NOTE: Hash type (unused)
        let hash_type = reader.read_u8()?;
        assert_eq!(0, hash_type, "Invalid bloom hash type");

        let num_blocks = reader.read_u64::<LittleEndian>()? as usize;
        let k = reader.read_u64::<LittleEndian>()? as usize;

        let mut bytes = vec![0; num_blocks * CACHE_LINE_BYTES];
        reader.read_exact(&mut bytes)?;

        Ok(BloomFilter::BlockedBloom(Self::from_raw(
            num_blocks,
            k,
            bytes.into(),
        )))
    }

    fn from_raw(num_blocks: usize, k: usize, slice: crate::Slice) -> Self {
        Self {
            inner: BitArrayReader::new(slice),
            k,
            num_blocks,
        }
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
    use crate::segment::filter::{AMQFilterBuilder, BloomFilter};

    use std::fs::File;
    use test_log::test;

    #[test]
    fn blocked_bloom_serde_round_trip() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("bf");
        let mut file = File::create(&path)?;

        let mut filter = Builder::with_fp_rate(10, 0.0001);

        let keys = &[
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ];

        for key in keys {
            filter.set_with_hash(BlockedBloomFilter::get_hash(*key));
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
        assert!(matches!(filter_copy, BloomFilter::BlockedBloom(_)));

        for key in keys {
            assert!(filter.contains(&**key));
        }
        assert!(!filter_copy.contains(b"asdasads"));
        assert!(!filter_copy.contains(b"item10"));
        assert!(!filter_copy.contains(b"cxycxycxy"));

        Ok(())
    }

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
