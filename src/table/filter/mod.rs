// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod bit_array;
pub mod block;
pub mod blocked_bloom;
pub mod standard_bloom;

use standard_bloom::Builder as StandardBloomFilterBuilder;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum BloomConstructionPolicy {
    BitsPerKey(f32),
    FalsePositiveRate(f32),
}

impl Default for BloomConstructionPolicy {
    fn default() -> Self {
        Self::BitsPerKey(10.0)
    }
}

impl BloomConstructionPolicy {
    #[must_use]
    pub fn init(&self, n: usize) -> StandardBloomFilterBuilder {
        use standard_bloom::Builder;

        match self {
            Self::BitsPerKey(bpk) => Builder::with_bpk(n, *bpk),
            Self::FalsePositiveRate(fpr) => Builder::with_fp_rate(n, *fpr),
        }
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        match self {
            Self::BitsPerKey(bpk) => *bpk > 0.0,
            Self::FalsePositiveRate(fpr) => *fpr > 0.0,
        }
    }

    #[must_use]
    pub fn estimated_key_bits(&self, n: usize) -> f32 {
        match self {
            Self::BitsPerKey(bpk) => *bpk,
            Self::FalsePositiveRate(fpr) => {
                let m = StandardBloomFilterBuilder::calculate_m(n, *fpr);
                (m / n) as f32
            }
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum FilterType {
    StandardBloom,
    BlockedBloom,
}

impl TryFrom<u8> for FilterType {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::StandardBloom),
            1 => Ok(Self::BlockedBloom),
            _ => Err(crate::Error::InvalidTag(("FilterType", value))),
        }
    }
}

impl From<FilterType> for u8 {
    fn from(value: FilterType) -> Self {
        match value {
            FilterType::StandardBloom => 0,
            FilterType::BlockedBloom => 1,
        }
    }
}
