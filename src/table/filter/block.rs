// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::table::{filter::standard_bloom::StandardBloomFilterReader, Block};

#[derive(Clone)]
pub struct FilterBlock(Block);

impl FilterBlock {
    #[must_use]
    pub fn new(block: Block) -> Self {
        Self(block)
    }

    pub fn maybe_contains_hash(&self, hash: u64) -> crate::Result<bool> {
        Ok(StandardBloomFilterReader::new(&self.0.data)?.contains_hash(hash))
    }

    /// Returns Ok(Some(true)) if the key's first extracted prefix may be
    /// contained, Ok(Some(false)) if the filter indicates that prefix is
    /// not present, or Ok(None) if the key is out of the extractor's
    /// domain (`extract_first` returns None).
    ///
    /// Only `extract_first` is consulted. For multi-prefix extractors that
    /// want to probe the most-specific prefix, use the hash-based probe
    /// path on `Table` instead.
    pub fn maybe_contains_prefix(
        &self,
        key: &[u8],
        extractor: &dyn crate::prefix::PrefixExtractor,
    ) -> crate::Result<Option<bool>> {
        Ok(StandardBloomFilterReader::new(&self.0.data)?.contains_prefix(key, extractor))
    }

    /// Returns the block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.0.size()
    }
}
