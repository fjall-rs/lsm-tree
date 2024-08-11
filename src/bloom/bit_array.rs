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

/// Sets a bit in the byte
fn set_bit(byte: u8, idx: usize, value: bool) -> u8 {
    let bit_mask = BIT_MASK >> idx;

    if value {
        byte | bit_mask
    } else {
        byte & !bit_mask
    }
}

/// Fixed-size bit array
#[derive(Debug, Eq, PartialEq)]
pub struct BitArray(Box<[u8]>);

impl BitArray {
    #[must_use]
    pub fn with_capacity(bytes: usize) -> Self {
        let vec = vec![0; bytes];
        Self(vec.into_boxed_slice())
    }

    #[must_use]
    pub fn from_bytes(bytes: Box<[u8]>) -> Self {
        Self(bytes)
    }

    /// Size in bytes
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Sets the i-th bit
    pub fn set(&mut self, idx: usize, val: bool) {
        let byte_idx = idx / 8;
        let byte = self.0.get_mut(byte_idx).expect("should be in bounds");

        let bit_idx = idx % 8;
        *byte = set_bit(*byte, bit_idx, val);
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
    use test_log::test;

    #[test]
    fn bit_set_true() {
        assert_eq!(0b0000_0010, set_bit(0, 6, true));
        assert_eq!(0b1000_0000, set_bit(0, 0, true));
        assert_eq!(0b0100_0000, set_bit(0, 1, true));
        assert_eq!(0b0100_0110, set_bit(0b0000_0110, 1, true));
    }

    #[test]
    fn bit_set_false() {
        assert_eq!(0b1111_1101, set_bit(0xFF, 6, false));
        assert_eq!(0b0111_1111, set_bit(0xFF, 0, false));
        assert_eq!(0b1011_1111, set_bit(0xFF, 1, false));

        assert_eq!(0b0000_0110, set_bit(0b0100_0110, 1, false));
    }

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
