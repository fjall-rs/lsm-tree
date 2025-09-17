// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

const BIT_MASK: u8 = 0b1000_0000_u8;

/// Gets a bit from the byte
fn get_bit(byte: u8, idx: usize) -> bool {
    let bit_mask = BIT_MASK >> idx;

    let masked = byte & bit_mask;
    masked > 0
}

/// Fixed-size bit array reader
#[derive(Debug)]
pub struct BitArrayReader<'a>(&'a [u8]);

impl<'a> BitArrayReader<'a> {
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        self.0
    }

    /// Gets the i-th bit.
    #[must_use]
    pub fn get(&self, idx: usize) -> bool {
        let byte_idx = idx / 8;

        // NOTE: We trust the caller
        #[allow(clippy::expect_used)]
        let byte = self.0.get(byte_idx).expect("should be in bounds");

        let bit_idx = idx % 8;
        get_bit(*byte, bit_idx)
    }
}
