use clap::ValueEnum;
use lsm_tree::{
    config::{BlockSizePolicy, CompressionPolicy},
    AnyTree, Cache, CompressionType, Config, SequenceNumberCounter,
};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("none"),
            Self::Lz4 => f.write_str("lz4"),
            Self::Zstd => f.write_str("zstd"),
        }
    }
}

impl Compression {
    pub fn to_lsm(self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            Self::Lz4 => CompressionType::Lz4,
            Self::Zstd => CompressionType::Zstd(3),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub num: u64,
    pub key_size: usize,
    pub value_size: usize,
    pub threads: usize,
    pub cache_mb: u64,
    pub compression: Compression,
    pub block_size: u32,
    pub use_blob_tree: bool,
}

impl BenchConfig {
    /// Bytes per key-value pair (for throughput calculation).
    pub fn entry_size(&self) -> usize {
        self.key_size + self.value_size
    }
}

/// Create an lsm-tree at the given path using the benchmark configuration.
pub fn create_tree(path: &Path, config: &BenchConfig) -> lsm_tree::Result<AnyTree> {
    let cache_bytes = config.cache_mb.checked_mul(1024 * 1024).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "requested cache size overflows u64",
        )
    })?;
    let cache = Arc::new(Cache::with_capacity_bytes(cache_bytes));

    let compression_policy = CompressionPolicy::all(config.compression.to_lsm());
    let block_size_policy = BlockSizePolicy::all(config.block_size);

    let mut builder = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(block_size_policy)
    .data_block_compression_policy(compression_policy)
    .use_cache(cache);

    if config.use_blob_tree {
        builder = builder.with_kv_separation(Some(Default::default()));
    }

    builder.open()
}
