// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// Runtime metrics
///
/// Are not stored durably, so metrics will reset after a restart/crash.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Number of times a table file was opened using `fopen()`
    pub(crate) table_file_opened: AtomicUsize,

    /// Number of times a table file was retrieved from descriptor cache
    pub(crate) table_file_opened_cached: AtomicUsize,

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

    /// Number of data block bytes that were requested from OS or disk
    pub(crate) data_block_io_requested: AtomicU64,

    /// Number of index block bytes that were requested from OS or disk
    pub(crate) index_block_io_requested: AtomicU64,

    /// Number of filter block bytes that were requested from OS or disk
    pub(crate) filter_block_io_requested: AtomicU64,
}

#[expect(
    clippy::cast_precision_loss,
    reason = "metrics can accept precision loss"
)]
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

    /// Number of index blocks that were loaded from disk or OS page cache.
    pub fn index_block_loads_cached(&self) -> usize {
        self.index_block_load_cached.load(Relaxed)
    }

    /// Number of filter blocks that were loaded from disk or OS page cache.
    pub fn filter_block_loads_cached(&self) -> usize {
        self.filter_block_load_cached.load(Relaxed)
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

    /// Filter block cache efficiency in percent (0.0 - 1.0).
    pub fn filter_block_cache_hit_rate(&self) -> f64 {
        let queries = self.filter_block_loads() as f64;
        let hits = self.filter_block_loads_cached() as f64;

        if queries == 0.0 {
            1.0
        } else {
            hits / queries
        }
    }

    /// Index block cache efficiency in percent (0.0 - 1.0).
    pub fn index_block_cache_hit_rate(&self) -> f64 {
        let queries = self.index_block_loads() as f64;
        let hits = self.index_block_loads_cached() as f64;

        if queries == 0.0 {
            1.0
        } else {
            hits / queries
        }
    }

    /// Block cache efficiency in percent (0.0 - 1.0).
    pub fn block_cache_hit_rate(&self) -> f64 {
        let queries = self.block_loads() as f64;
        let hits = self.block_loads_cached() as f64;

        if queries == 0.0 {
            1.0
        } else {
            hits / queries
        }
    }

    /// Filter efficiency in percent (0.0 - 1.0).
    ///
    /// Represents the ratio of I/O operations avoided due to filter.
    pub fn filter_efficiency(&self) -> f64 {
        let queries = self.filter_queries.load(Relaxed) as f64;
        let io_skipped = self.io_skipped_by_filter.load(Relaxed) as f64;

        if queries == 0.0 {
            1.0
        } else {
            io_skipped / queries
        }
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
