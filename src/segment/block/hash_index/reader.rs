// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{calculate_bucket_position, MARKER_CONFLICT, MARKER_FREE};

/// Helper to read from an embedded block hash index
pub struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    /// Initializes a new hash index reader.
    #[must_use]
    pub fn new(bytes: &'a [u8], offset: u32, len: u32) -> Self {
        let offset = offset as usize;
        let len = len as usize;
        let end = offset + len;

        // NOTE: We consider the caller to be trustworthy
        #[warn(clippy::indexing_slicing)]
        Self(&bytes[offset..end])
    }

    /// Returns the number of buckets.
    #[must_use]
    pub fn bucket_count(&self) -> usize {
        self.0.len()
    }

    // NOTE: Only used in metrics, so no need to be hyper-optimized
    #[allow(clippy::naive_bytecount)]
    /// Returns the amount of empty slots in the hash index.
    #[must_use]
    pub fn free_count(&self) -> usize {
        self.0.iter().filter(|&&byte| byte == MARKER_FREE).count()
    }

    // NOTE: Only used in metrics, so no need to be hyper-optimized
    /// Returns the amount of conflict markers in the hash index.
    #[must_use]
    #[allow(clippy::naive_bytecount)]
    pub fn conflict_count(&self) -> usize {
        self.0
            .iter()
            .filter(|&&byte| byte == MARKER_CONFLICT)
            .count()
    }

    /// Returns the binary index position if the key is not conflicted.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> u8 {
        // NOTE: Even with very high hash ratio, there will be nearly enough items to
        // cause us to create u32 buckets
        #[allow(clippy::cast_possible_truncation)]
        let bucket_count = self.0.len() as u32;

        let bucket_pos = calculate_bucket_position(key, bucket_count);

        // SAFETY: We use modulo in `calculate_bucket_position`
        // SAFETY: Also we already did a bounds check in the constructor using indexing slicing
        *unsafe { self.0.get_unchecked(bucket_pos) }
    }
}
