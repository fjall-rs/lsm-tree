// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

#[derive(Debug, Default)]
pub struct Metrics {
    /// Number of blocks that were actually read from disk
    pub(crate) block_load_io: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) block_load_cached: AtomicUsize,

    /// Number of bloom filter queries that were performed
    pub(crate) bloom_filter_queries: AtomicUsize,

    /// Number of IOs that were skipped due to bloom filter hits
    pub(crate) bloom_filter_hits: AtomicUsize,
}

#[allow(clippy::cast_precision_loss)]
impl Metrics {
    /// Number of blocks that were read from disk.
    pub fn block_loads_io(&self) -> usize {
        self.block_load_io.load(Relaxed)
    }

    /// Number of blocks that were accessed.
    pub fn block_loads(&self) -> usize {
        self.block_load_cached.load(Relaxed) + self.block_load_io.load(Relaxed)
    }

    /// Block cache efficiency in percent (0.0 - 1.0).
    pub fn block_cache_efficiency(&self) -> f64 {
        let queries = self.block_loads() as f64;
        let hits = self.block_load_cached.load(Relaxed) as f64;
        hits / queries
    }

    /// Filter efficiency in percent (0.0 - 1.0).
    pub fn bloom_filter_efficiency(&self) -> f64 {
        let queries = self.bloom_filter_queries.load(Relaxed) as f64;
        let hits = self.bloom_filter_hits.load(Relaxed) as f64;
        hits / queries
    }
}
