// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod bit_array;
pub mod blocked_bloom;
pub mod standard_bloom;

use crate::{coding::DecodeError, file::MAGIC_BYTES};
use blocked_bloom::BlockedBloomFilter;
use byteorder::ReadBytesExt;
use standard_bloom::{Builder as StandardBloomFilterBuilder, StandardBloomFilter};
use std::io::Read;

const CACHE_LINE_BYTES: usize = 64;

#[derive(Copy, Clone, Debug)]
pub enum BloomConstructionPolicy {
    BitsPerKey(u8),
    FpRate(f32),
}

impl Default for BloomConstructionPolicy {
    fn default() -> Self {
        Self::BitsPerKey(10)
    }
}

impl BloomConstructionPolicy {
    #[must_use]
    pub fn init(&self, n: usize) -> StandardBloomFilterBuilder {
        use standard_bloom::Builder;

        match self {
            Self::BitsPerKey(bpk) => Builder::with_bpk(n, *bpk),
            Self::FpRate(fpr) => Builder::with_fp_rate(n, *fpr),
        }
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        match self {
            Self::BitsPerKey(bpk) => *bpk > 0,
            Self::FpRate(fpr) => *fpr > 0.0,
        }
    }
}

#[enum_dispatch::enum_dispatch]
pub trait AMQ {
    fn bytes(&self) -> &[u8];
    fn len(&self) -> usize;
    fn contains(&self, item: &[u8]) -> bool;
    fn contains_hash(&self, hash: (u64, u64)) -> bool;
}

#[enum_dispatch::enum_dispatch(AMQ)]
#[derive(PartialEq, Debug)]
pub enum AMQFilter {
    StandardBloom(StandardBloomFilter),
    BlockedBloom(BlockedBloomFilter),
}

pub enum BloomFilterType {
    StandardBloom = 0,
    BlockedBloom = 1,
}

impl TryFrom<u8> for BloomFilterType {
    type Error = ();
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::StandardBloom),
            1 => Ok(Self::BlockedBloom),
            _ => Err(()),
        }
    }
}

pub struct AMQFilterBuilder {}

impl AMQFilterBuilder {
    pub fn decode_from<R: Read>(reader: &mut R) -> Result<AMQFilter, DecodeError> {
        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(DecodeError::InvalidHeader("BloomFilter"));
        }

        let filter_type = reader.read_u8()?;

        match BloomFilterType::try_from(filter_type) {
            Ok(BloomFilterType::StandardBloom) => {
                StandardBloomFilter::decode_from(reader).map_err(|e| DecodeError::from(e))
            }
            Ok(BloomFilterType::BlockedBloom) => {
                BlockedBloomFilter::decode_from(reader).map_err(|e| DecodeError::from(e))
            }
            _ => Err(DecodeError::InvalidHeader("Unknown filter type")),
        }
    }
}
