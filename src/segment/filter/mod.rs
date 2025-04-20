// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod bit_array;
pub mod blocked_bloom;
pub mod standard_bloom;

const CACHE_LINE_BYTES: usize = 64;

use blocked_bloom::Builder as BlockedBloomFilterBuilder;
use standard_bloom::Builder as StandardBloomFilterBuilder;

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
