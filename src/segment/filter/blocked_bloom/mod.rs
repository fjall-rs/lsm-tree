// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod builder;

pub use builder::Builder;

use super::bit_array::BitArrayReader;
use crate::{
    file::MAGIC_BYTES,
    segment::filter::{standard_bloom::builder::secondary_hash, FilterType},
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};

const CACHE_LINE_BYTES: usize = 64;

/// A blocked bloom filter
///
/// Allows buffering the key hashes before actual filter construction
/// which is needed to properly calculate the filter size, as the amount of items
/// are unknown during segment construction.
///
/// The filter uses double hashing instead of `k` hash functions, see:
/// <https://fjall-rs.github.io/post/bloom-filter-hash-sharing>
#[derive(Debug)]
pub struct BlockedBloomFilterReader<'a> {
    /// Raw bytes exposed as bit array
    inner: BitArrayReader<'a>,

    /// Number of hash functions
    k: usize,

    /// Number of blocks in the blocked bloom filter
    num_blocks: usize,
}

impl<'a> BlockedBloomFilterReader<'a> {
    pub fn new(slice: &'a [u8]) -> crate::Result<Self> {
        let mut reader = Cursor::new(slice);

        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(crate::Error::Decode(crate::DecodeError::InvalidHeader(
                "BloomFilter",
            )));
        }

        // NOTE: Filter type
        let filter_type = reader.read_u8()?;
        let filter_type = FilterType::try_from(filter_type)?;
        assert_eq!(
            FilterType::BlockedBloom,
            filter_type,
            "Invalid filter type, got={filter_type:?}, expected={:?}",
            FilterType::BlockedBloom,
        );

        // NOTE: Hash type (unused)
        let hash_type = reader.read_u8()?;
        assert_eq!(0, hash_type, "Invalid bloom hash type");

        let num_blocks = reader.read_u64::<LittleEndian>()? as usize;
        let k = reader.read_u64::<LittleEndian>()? as usize;

        let offset = reader.position() as usize;

        #[allow(clippy::indexing_slicing)]
        Ok(Self {
            k,
            num_blocks,
            inner: BitArrayReader::new(slice.get(offset..).expect("should be in bounds")),
        })
    }

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
    pub fn contains_hash(&self, mut h1: u64) -> bool {
        let mut h2 = secondary_hash(h1);

        let block_idx = h1 % (self.num_blocks as u64);

        for i in 1..(self.k as u64) {
            let bit_idx = h1 % (CACHE_LINE_BYTES as u64 * 8);

            if !self.has_bit(block_idx as usize, bit_idx as usize) {
                return false;
            }

            h1 = h1.wrapping_add(h2);
            h2 = h2.wrapping_mul(i);
        }

        true
    }

    /// Returns `true` if the bit at `idx` is `1`.
    fn has_bit(&self, block_idx: usize, idx_in_block: usize) -> bool {
        self.inner
            .get(Builder::get_bit_idx(block_idx, idx_in_block))
    }

    /// Gets the hash of a key.
    pub fn get_hash(key: &[u8]) -> u64 {
        Builder::get_hash(key)
    }

    /// Returns `true` if the item may be contained.
    ///
    /// Will never have a false negative.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.contains_hash(Self::get_hash(key))
    }
}

// impl<'a> Encode for BlockedBloomFilter<'a> {
//     fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
//         // Write header
//         writer.write_all(&MAGIC_BYTES)?;

//         writer.write_u8(BloomFilterType::BlockedBloom as u8)?;

//         // NOTE: Hash type (unused)
//         writer.write_u8(0)?;

//         writer.write_u64::<LittleEndian>(self.num_blocks as u64)?;
//         writer.write_u64::<LittleEndian>(self.k as u64)?;
//         writer.write_all(self.inner.bytes())?;

//         Ok(())
//     }
// }

// impl<'a> BlockedBloomFilter<'a> {
//     // To be used by AMQFilter after magic bytes and filter type have been read and parsed
//     pub(super) fn decode_from<R: Read>(reader: &mut R) -> Result<AMQFilter, DecodeError> {
//         // NOTE: Hash type (unused)
//         let hash_type = reader.read_u8()?;
//         assert_eq!(0, hash_type, "Invalid bloom hash type");

//         let num_blocks = reader.read_u64::<LittleEndian>()? as usize;
//         let k = reader.read_u64::<LittleEndian>()? as usize;

//         let mut bytes = vec![0; num_blocks * CACHE_LINE_BYTES];
//         reader.read_exact(&mut bytes)?;

//         Ok(AMQFilter::BlockedBloom(Self::from_raw(
//             num_blocks,
//             k,
//             bytes.into(),
//         )))
//     }

//     fn from_raw(num_blocks: usize, k: usize, slice: crate::Slice) -> Self {
//         Self {
//             inner: BitArrayReader::new(slice),
//             k,
//             num_blocks,
//         }
//     }

//     /// Gets the hash of a key.
//     pub fn get_hash(key: &[u8]) -> CompositeHash {
//         Builder::get_hash(key)
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use test_log::test;

    // #[test]
    // fn blocked_bloom_serde_round_trip() -> crate::Result<()> {
    //     let dir = tempfile::tempdir()?;

    //     let path = dir.path().join("bf");
    //     let mut file = File::create(&path)?;

    //     let mut filter = Builder::with_fp_rate(10, 0.0001);

    //     let keys = &[
    //         b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
    //         b"item8", b"item9",
    //     ];

    //     for key in keys {
    //         filter.set_with_hash(BlockedBloomFilter::get_hash(*key));
    //     }

    //     let filter = filter.build();

    //     for key in keys {
    //         assert!(filter.contains(&**key));
    //     }
    //     assert!(!filter.contains(b"asdasads"));
    //     assert!(!filter.contains(b"item10"));
    //     assert!(!filter.contains(b"cxycxycxy"));

    //     filter.encode_into(&mut file)?;
    //     file.sync_all()?;
    //     drop(file);

    //     let mut file = File::open(&path)?;
    //     let filter_copy = AMQFilterBuilder::decode_from(&mut file)?;

    //     assert_eq!(filter.inner.bytes(), filter_copy.bytes());
    //     assert!(matches!(filter_copy, AMQFilter::BlockedBloom(_)));

    //     for key in keys {
    //         assert!(filter.contains(&**key));
    //     }
    //     assert!(!filter_copy.contains(b"asdasads"));
    //     assert!(!filter_copy.contains(b"item10"));
    //     assert!(!filter_copy.contains(b"cxycxycxy"));

    //     Ok(())
    // }

    // #[test]
    // fn blocked_bloom_basic() {
    //     let mut filter = Builder::with_fp_rate(10, 0.0001);
    //     let keys = [
    //         b"item0" as &[u8],
    //         b"item1",
    //         b"item2",
    //         b"item3",
    //         b"item4",
    //         b"item5",
    //         b"item6",
    //         b"item7",
    //         b"item8",
    //         b"item9",
    //     ];

    //     for key in &keys {
    //         filter.set_with_hash(Builder::get_hash(key));
    //     }

    //     let filter = filter.build();

    //     for key in &keys {
    //         assert!(filter.contains(key));
    //     }

    //     assert!(!filter.contains(b"asdasdasdasdasdasdasd"));
    // }
}
