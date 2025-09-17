// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

const BIT_MASK: u8 = 0b1000_0000_u8;

/// Sets a bit in the byte to `true`
#[must_use]
pub fn enable_bit(byte: u8, idx: usize) -> u8 {
    let bit_mask = BIT_MASK >> idx;
    byte | bit_mask
}

/// Fixed-size bit array
#[derive(Debug)]
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

        // NOTE: We trust the caller
        #[allow(clippy::expect_used)]
        let byte = self.0.get_mut(byte_idx).expect("should be in bounds");

        let bit_idx = idx % 8;
        *byte = enable_bit(*byte, bit_idx);
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
    fn bit_array_builder_basic() {
        let mut builder = Builder::with_capacity(1);
        assert_eq!(&[0], builder.bytes());

        builder.enable_bit(0);
        assert_eq!(&[0b1000_0000], builder.bytes());

        builder.enable_bit(7);
        assert_eq!(&[0b1000_0001], builder.bytes());
    }
}
