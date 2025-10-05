// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{calculate_bucket_position, MARKER_CONFLICT, MARKER_FREE};
use byteorder::WriteBytesExt;

/// With 254, pointers [0 - 253] can be indexed.
pub const MAX_POINTERS_FOR_HASH_INDEX: usize = 254;

/// Builds a block hash index
#[derive(Debug)]
pub struct Builder(Vec<u8>);

impl Builder {
    /// Initializes a new builder with the given number of buckets.
    #[must_use]
    pub fn with_bucket_count(bucket_count: u32) -> Self {
        Self(vec![MARKER_FREE; bucket_count as usize])
    }

    #[must_use]
    pub fn with_hash_ratio(item_count: usize, hash_ratio: f32) -> Self {
        Self::with_bucket_count(Self::calculate_bucket_count(item_count, hash_ratio))
    }

    fn calculate_bucket_count(item_count: usize, hash_ratio: f32) -> u32 {
        assert!(
            hash_ratio.is_sign_positive(),
            "hash_ratio may not be negative",
        );

        if hash_ratio > 0.0 {
            ((item_count as f32 * hash_ratio) as u32).max(1)
        } else {
            0
        }
    }

    // NOTE: We know the hash index has a bucket count <= u8
    #[allow(clippy::cast_possible_truncation)]
    /// Returns the number of buckets.
    #[must_use]
    pub fn bucket_count(&self) -> u32 {
        self.0.len() as u32
    }

    /// Tries to map the given key to the binary index position.
    pub fn set(&mut self, key: &[u8], binary_index_pos: u8) -> bool {
        debug_assert!(
            binary_index_pos <= 253,
            "restart index too high for hash index"
        );

        assert!(self.bucket_count() > 0, "no buckets to insert into");

        let bucket_pos = calculate_bucket_position(key, self.bucket_count());

        // SAFETY: We use modulo in `calculate_bucket_position`
        #[allow(unsafe_code)]
        let curr_marker = unsafe { *self.0.get_unchecked(bucket_pos) };

        match curr_marker {
            MARKER_CONFLICT => false,
            MARKER_FREE => {
                // SAFETY: We previously asserted that the slot exists
                #[allow(unsafe_code)]
                unsafe {
                    *self.0.get_unchecked_mut(bucket_pos) = binary_index_pos;
                }

                true
            }
            x if x == binary_index_pos => {
                // NOTE: If different keys map to the same bucket, we can keep
                // the mapping
                true
            }
            _ => {
                // NOTE: Mark as conflicted

                // SAFETY: We previously asserted that the slot exists
                #[allow(unsafe_code)]
                unsafe {
                    *self.0.get_unchecked_mut(bucket_pos) = MARKER_CONFLICT;
                }

                false
            }
        }
    }

    /// Consumes the builder, returning its raw bytes.
    ///
    /// Only used for tests/benchmarks
    #[must_use]
    #[doc(hidden)]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    /// Appends the raw index bytes to a writer.
    pub fn write<W: std::io::Write>(self, writer: &mut W) -> std::io::Result<()> {
        for byte in self.0 {
            writer.write_u8(byte)?;
        }
        Ok(())
    }
}
