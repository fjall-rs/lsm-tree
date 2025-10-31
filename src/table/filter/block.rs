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

    /// Returns the block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.0.size()
    }
}
