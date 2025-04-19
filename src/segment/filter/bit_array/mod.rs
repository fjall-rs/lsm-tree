// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod sliced;

pub use sliced::BitArray as BitArrayReader;

const BIT_MASK: u8 = 0b1000_0000_u8;

/// Sets a bit in the byte
#[must_use]
pub fn set_bit(byte: u8, idx: usize, value: bool) -> u8 {
    let bit_mask = BIT_MASK >> idx;

    if value {
        byte | bit_mask
    } else {
        byte & !bit_mask
    }
}

/// Fixed-size bit array
#[derive(Debug, Eq, PartialEq)]
pub struct Builder(Box<[u8]>);

impl Builder {
    #[must_use]
    pub fn with_capacity(bytes: usize) -> Self {
        let vec = vec![0; bytes];
        Self(vec.into_boxed_slice())
    }

    #[must_use]
    pub fn from_bytes(bytes: Box<[u8]>) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Sets the i-th bit
    pub fn enable_bit(&mut self, idx: usize) {
        let byte_idx = idx / 8;
        let byte = self.0.get_mut(byte_idx).expect("should be in bounds");

        let bit_idx = idx % 8;
        *byte = set_bit(*byte, bit_idx, true);
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
    fn bit_array_builder_basic() {
        let mut builder = Builder::with_capacity(1);
        assert_eq!(&[0], builder.bytes());

        builder.enable_bit(0);
        assert_eq!(&[0b1000_0000], builder.bytes());

        builder.enable_bit(7);
        assert_eq!(&[0b1000_0001], builder.bytes());
    }
}
