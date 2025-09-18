// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

/// Runtime metrics
///
/// Are not stored durably, so metrics will reset after a restart/crash.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Number of index blocks that were actually read from disk
    pub(crate) index_block_load_io: AtomicUsize,

    /// Number of filter blocks that were actually read from disk
    pub(crate) filter_block_load_io: AtomicUsize,

    /// Number of blocks that were actually read from disk
    pub(crate) data_block_load_io: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) index_block_load_cached: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) filter_block_load_cached: AtomicUsize,

    /// Number of blocks that were read from block cache
    pub(crate) data_block_load_cached: AtomicUsize,

    /// Number of filter queries that were performed
    pub(crate) filter_queries: AtomicUsize,

    /// Number of IOs that were skipped due to filter
    pub(crate) io_skipped_by_filter: AtomicUsize,
}

#[allow(clippy::cast_precision_loss)]
impl Metrics {
    /// Number of data blocks that were accessed.
    pub fn data_block_loads(&self) -> usize {
        self.data_block_load_cached.load(Relaxed) + self.data_block_load_io.load(Relaxed)
    }

    /// Number of index blocks that were accessed.
    pub fn index_block_loads(&self) -> usize {
        self.index_block_load_cached.load(Relaxed) + self.index_block_load_io.load(Relaxed)
    }

    /// Number of filter blocks that were accessed.
    pub fn filter_block_loads(&self) -> usize {
        self.filter_block_load_cached.load(Relaxed) + self.filter_block_load_io.load(Relaxed)
    }

    /// Number of blocks that were loaded from disk or OS page cache.
    pub fn block_loads_io(&self) -> usize {
        self.data_block_load_io.load(Relaxed)
            + self.index_block_load_io.load(Relaxed)
            + self.filter_block_load_io.load(Relaxed)
    }

    /// Number of blocks that were loaded from disk or OS page cache.
    pub fn block_loads_cached(&self) -> usize {
        self.data_block_load_cached.load(Relaxed)
            + self.index_block_load_cached.load(Relaxed)
            + self.filter_block_load_cached.load(Relaxed)
    }

    /// Number of blocks that were accessed.
    pub fn block_loads(&self) -> usize {
        self.block_loads_io() + self.block_loads_cached()
    }

    /// Block cache efficiency in percent (0.0 - 1.0).
    pub fn block_cache_hit_rate(&self) -> f64 {
        let queries = self.block_loads() as f64;
        let hits = self.block_loads_cached() as f64;
        hits / queries
    }

    /// Filter efficiency in percent (0.0 - 1.0).
    ///
    /// Represents the ratio of I/O operations avoided due to filter.
    pub fn filter_efficiency(&self) -> f64 {
        let queries = self.filter_queries.load(Relaxed) as f64;
        let io_skipped = self.io_skipped_by_filter.load(Relaxed) as f64;
        io_skipped / queries
    }

    /// Number of filter queries performed.
    pub fn filter_queries(&self) -> usize {
        self.filter_queries.load(Relaxed)
    }

    /// Number of I/O operations skipped by filter.
    pub fn io_skipped_by_filter(&self) -> usize {
        self.io_skipped_by_filter.load(Relaxed)
    }
}
