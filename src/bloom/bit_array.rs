// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

const BIT_MASK: u8 = 0b1000_0000_u8;

/// Gets a bit from the byte.
fn get_bit(byte: u8, idx: usize) -> bool {
    let bit_mask = BIT_MASK >> idx;
    let masked = byte & bit_mask;
    masked > 0
}

/// Enables the given bit in the byte.
fn enable_bit(byte: u8, idx: usize) -> u8 {
    let bit_mask = BIT_MASK >> idx;
    byte | bit_mask
}

/// Fixed-size bit array
#[derive(Debug, Eq, PartialEq)]
pub struct BitArray(Box<[u8]>);

impl BitArray {
    /// Creates a new bit array with the given size in bytes.
    #[must_use]
    pub fn with_capacity(bytes: usize) -> Self {
        let vec = vec![0; bytes];
        Self(vec.into_boxed_slice())
    }

    /// Treats the given byte array as bit array.
    #[must_use]
    pub fn from_bytes(bytes: Box<[u8]>) -> Self {
        Self(bytes)
    }

    /// Returns the inner data.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Sets the i-th bit to `true`.
    pub fn enable(&mut self, idx: usize) {
        let byte_idx = idx / 8;
        let byte = self.0.get_mut(byte_idx).expect("should be in bounds");

        let bit_idx: usize = idx % 8;
        *byte = enable_bit(*byte, bit_idx);
    }

    /// Gets the i-th bit.
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
        assert_eq!(0b0000_0010, enable_bit(0, 6));
        assert_eq!(0b1000_0000, enable_bit(0, 0));
        assert_eq!(0b0100_0000, enable_bit(0, 1));
        assert_eq!(0b0100_0110, enable_bit(0b0000_0110, 1));
    }

    #[test]
    fn bit_set_get() {
        assert!(!get_bit(0b0100_0110, 0));
        assert!(get_bit(0b0100_0110, 1));
        assert!(get_bit(0b0100_0110, 6));
        assert!(!get_bit(0b0100_0110, 7));
    }
}
