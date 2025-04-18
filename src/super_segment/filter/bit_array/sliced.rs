// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Slice;

const BIT_MASK: u8 = 0b1000_0000_u8;

/// Gets a bit from the byte
fn get_bit(byte: u8, idx: usize) -> bool {
    let bit_mask = BIT_MASK >> idx;

    let masked = byte & bit_mask;
    masked > 0
}

/// Fixed-size bit array
#[derive(Debug, Eq, PartialEq)]
pub struct BitArray(Slice);

impl BitArray {
    #[must_use]
    pub fn new(slice: Slice) -> Self {
        Self(slice)
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Gets the i-th bit
    #[must_use]
    pub fn get(&self, idx: usize) -> bool {
        let byte_idx = idx / 8;
        let byte = self.0.get(byte_idx).expect("should be in bounds");

        let bit_idx = idx % 8;
        get_bit(*byte, bit_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::super_segment::filter::bit_array::set_bit;
    use test_log::test;

    #[test]
    fn bit_set_get() {
        assert_eq!(0b1111_1101, set_bit(0xFF, 6, false));
        assert_eq!(0b0111_1111, set_bit(0xFF, 0, false));
        assert_eq!(0b1011_1111, set_bit(0xFF, 1, false));

        assert!(!get_bit(0b0100_0110, 0));
        assert!(get_bit(0b0100_0110, 1));
        assert!(get_bit(0b0100_0110, 6));
        assert!(!get_bit(0b0100_0110, 7));
    }
}
