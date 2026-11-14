// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{prefix::SharedPrefixExtractor, table::filter::standard_bloom::Builder};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Bits set per prefix; about 1% false positives at 10 bits per prefix
const PROBES: usize = 6;

/// One cache line: every bit of a prefix lands in the same block
#[repr(align(64))]
struct Block([AtomicU64; 8]);

/// A bloom filter over the prefixes of a memtable's keys, which writers add
/// to concurrently.
///
/// Bits are set before the key enters the skiplist, and only ever set, so a
/// reader that can see a key can see its prefixes' bits: the writer
/// publishes the key (skiplist insert, then the visible seqno) with release
/// ordering after setting them. A writer that finds the bits set and skips
/// the write read them before publishing, so by read-read coherence its
/// readers see them too. The bit operations need no ordering of their own.
pub struct PrefixBloom {
    extractor: SharedPrefixExtractor,
    blocks: Box<[Block]>,

    #[cfg(feature = "metrics")]
    metrics: std::sync::Arc<crate::Metrics>,
}

impl PrefixBloom {
    /// Creates a filter of `bytes` (rounded up to 64) for `extractor`'s prefixes
    pub fn new(
        extractor: SharedPrefixExtractor,
        bytes: usize,
        #[cfg(feature = "metrics")] metrics: std::sync::Arc<crate::Metrics>,
    ) -> Self {
        let block_count = bytes
            .div_ceil(size_of::<Block>())
            .clamp(1, u32::MAX as usize);

        Self {
            extractor,
            blocks: std::iter::repeat_with(|| Block(Default::default()))
                .take(block_count)
                .collect(),

            #[cfg(feature = "metrics")]
            metrics,
        }
    }

    pub fn extractor(&self) -> &SharedPrefixExtractor {
        &self.extractor
    }

    /// Adds every prefix of `key`
    pub fn insert(&self, key: &[u8]) {
        self.extractor.extract(key, &mut |len| {
            if let Some(prefix) = key.get(..len) {
                self.insert_hash(Builder::get_hash(prefix));
            }
        });
    }

    fn insert_hash(&self, hash: u64) {
        let (block, masks) = self.locate(hash);

        for (word, mask) in block.0.iter().zip(masks) {
            // Prefixes repeat across keys, so most bits are set already:
            // checking first keeps hot cache lines shared between writers
            if mask != 0 && word.load(Relaxed) & mask != mask {
                word.fetch_or(mask, Relaxed);
            }
        }
    }

    /// Returns `false` if no key added so far has the prefix with this hash.
    pub fn may_contain(&self, hash: u64) -> bool {
        let (block, masks) = self.locate(hash);

        let contains = block
            .0
            .iter()
            .zip(masks)
            .all(|(word, mask)| word.load(Relaxed) & mask == mask);

        #[cfg(feature = "metrics")]
        {
            self.metrics
                .memtable_prefix_bloom_queries
                .fetch_add(1, Relaxed);

            if !contains {
                self.metrics
                    .memtable_reads_skipped_by_prefix_bloom
                    .fetch_add(1, Relaxed);
            }
        }

        contains
    }

    /// Picks the block from the upper half of `hash` and the bits from the
    /// lower half, like `RocksDB`'s `FastLocalBloom`.
    fn locate(&self, hash: u64) -> (&Block, [u64; 8]) {
        // The product of two numbers below 2^32, shifted down by 32, is below
        // the block count
        let index = (((hash >> 32) * self.blocks.len() as u64) >> 32) as usize;

        let mut masks = [0u64; 8];

        #[expect(clippy::cast_possible_truncation, reason = "the lower half")]
        let mut bits = hash as u32;

        for _ in 0..PROBES {
            // The top 9 bits pick one of the block's 512 bits
            bits = bits.wrapping_mul(0x9E37_79B9);
            let bit = bits >> (32 - 9);

            #[expect(clippy::indexing_slicing, reason = "bit >> 6 is below 8")]
            {
                masks[(bit >> 6) as usize] |= 1 << (bit & 63);
            }
        }

        #[expect(clippy::indexing_slicing, reason = "index is below the block count")]
        (&self.blocks[index], masks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prefix::FixedPrefixExtractor;
    use std::sync::Arc;

    fn bloom(bytes: usize) -> PrefixBloom {
        PrefixBloom::new(
            Arc::new(FixedPrefixExtractor::new(4)),
            bytes,
            #[cfg(feature = "metrics")]
            Arc::default(),
        )
    }

    #[test]
    fn holds_every_added_prefix() {
        let bloom = bloom(64);

        for i in 0..5_000u32 {
            bloom.insert(format!("{i:04}_suffix").as_bytes());
        }

        for i in 0..5_000u32 {
            let hash = Builder::get_hash(format!("{i:04}").as_bytes());
            assert!(bloom.may_contain(hash), "lost prefix {i:04}");
        }
    }

    #[test]
    fn false_positive_rate_at_10_bits_per_prefix() {
        let prefixes = 100_000u32;
        let bloom = bloom(prefixes as usize * 10 / 8);

        for i in 0..prefixes {
            bloom.insert(&i.to_be_bytes());
        }

        let false_positives = (prefixes..2 * prefixes)
            .filter(|i| bloom.may_contain(Builder::get_hash(&i.to_be_bytes())))
            .count();

        // A standard bloom filter has 0.8% at this size; one cache line per
        // prefix costs a little more
        assert!(
            false_positives < prefixes as usize * 2 / 100,
            "{false_positives} false positives in {prefixes}",
        );
    }

    #[test]
    fn short_keys_add_nothing() {
        let bloom = bloom(64);
        bloom.insert(b"abc");
        assert!(bloom
            .blocks
            .iter()
            .flat_map(|b| &b.0)
            .all(|w| w.load(Relaxed) == 0));
    }

    #[test]
    fn concurrent_writers_lose_no_prefix() {
        let bloom = bloom(1_024);

        std::thread::scope(|scope| {
            for thread in 0..8u32 {
                let bloom = &bloom;
                scope.spawn(move || {
                    for i in 0..2_000u32 {
                        bloom.insert(&(thread * 2_000 + i).to_be_bytes());
                    }
                });
            }
        });

        for i in 0..16_000u32 {
            assert!(bloom.may_contain(Builder::get_hash(&i.to_be_bytes())));
        }
    }
}
