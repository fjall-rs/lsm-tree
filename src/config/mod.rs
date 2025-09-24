// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod block_size;
mod compression;
mod filter;
mod hash_ratio;
mod pinning;
mod restart_interval;

pub use block_size::BlockSizePolicy;
pub use compression::CompressionPolicy;
pub use filter::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry};
pub use hash_ratio::HashRatioPolicy;
pub use pinning::PinningPolicy;
pub use restart_interval::RestartIntervalPolicy;

use crate::{path::absolute_path, BlobTree, Cache, CompressionType, DescriptorTable, Tree};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// LSM-tree type
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TreeType {
    /// Standard LSM-tree, see [`Tree`]
    Standard,

    /// Key-value separated LSM-tree, see [`BlobTree`]
    Blob,
}

impl From<TreeType> for u8 {
    fn from(val: TreeType) -> Self {
        match val {
            TreeType::Standard => 0,
            TreeType::Blob => 1,
        }
    }
}

impl TryFrom<u8> for TreeType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Standard),
            1 => Ok(Self::Blob),
            _ => Err(()),
        }
    }
}

const DEFAULT_FILE_FOLDER: &str = ".lsm.data";

#[derive(Clone)]
/// Tree configuration builder
pub struct Config {
    /// Folder path
    #[doc(hidden)]
    pub path: PathBuf,

    /// Block cache to use
    #[doc(hidden)]
    pub cache: Arc<Cache>,

    /// Descriptor table to use
    #[doc(hidden)]
    pub descriptor_table: Arc<DescriptorTable>,

    /// Tree type (unused)
    #[allow(unused)]
    pub tree_type: TreeType,

    /// Number of levels of the LSM tree (depth of tree)
    pub level_count: u8,

    /// What type of compression is used for data blocks
    pub data_block_compression_policy: CompressionPolicy,

    /// What type of compression is used for index blocks
    pub index_block_compression_policy: CompressionPolicy,

    /// Restart interval inside data blocks
    pub data_block_restart_interval_policy: RestartIntervalPolicy,

    /// Restart interval inside index blocks
    pub index_block_restart_interval_policy: RestartIntervalPolicy,

    /// Block size of data blocks
    pub data_block_size_policy: BlockSizePolicy,

    /// Block size of index blocks
    pub index_block_size_policy: BlockSizePolicy,

    /// Whether to pin index blocks
    pub index_block_pinning_policy: PinningPolicy,

    /// Whether to pin filter blocks
    pub filter_block_pinning_policy: PinningPolicy,

    /// Data block hash ratio
    pub data_block_hash_ratio_policy: HashRatioPolicy,

    /// If `true`, the last level will not build filters, reducing the filter size of a database
    /// by ~90% typically
    pub(crate) expect_point_read_hits: bool,

    /// Filter construction policy
    pub filter_policy: FilterPolicy,

    /// What type of compression is used for blobs
    pub blob_compression: CompressionType,

    /// Blob file (value log segment) target size in bytes
    #[doc(hidden)]
    pub blob_file_target_size: u64,

    /// Key-value separation threshold in bytes
    #[doc(hidden)]
    pub blob_file_separation_threshold: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: absolute_path(Path::new(DEFAULT_FILE_FOLDER)),
            descriptor_table: Arc::new(DescriptorTable::new(256)),

            cache: Arc::new(Cache::with_capacity_bytes(/* 16 MiB */ 16 * 1_024 * 1_024)),

            data_block_restart_interval_policy: RestartIntervalPolicy::all(16),
            index_block_restart_interval_policy: RestartIntervalPolicy::all(1),

            level_count: 7,
            tree_type: TreeType::Standard,

            data_block_size_policy: BlockSizePolicy::default(),
            index_block_size_policy: BlockSizePolicy::default(),

            index_block_pinning_policy: PinningPolicy::new(&[true, true, false]),
            filter_block_pinning_policy: PinningPolicy::new(&[true, false]),

            data_block_compression_policy: CompressionPolicy::default(),
            index_block_compression_policy:CompressionPolicy::all(CompressionType::None),

            data_block_hash_ratio_policy: HashRatioPolicy::all(0.0),

            blob_compression: CompressionType::None,

            filter_policy: FilterPolicy::default(),

            blob_file_target_size: /* 64 MiB */ 64 * 1_024 * 1_024,
            blob_file_separation_threshold: /* 4 KiB */ 4 * 1_024,

            expect_point_read_hits: false,
        }
    }
}

impl Config {
    /// Initializes a new config
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: absolute_path(path.as_ref()),
            ..Default::default()
        }
    }

    /// Sets the global cache.
    ///
    /// You can create a global [`Cache`] and share it between multiple
    /// trees to cap global cache memory usage.
    ///
    /// Defaults to a cache with 8 MiB of capacity *per tree*.
    #[must_use]
    pub fn use_cache(mut self, cache: Arc<Cache>) -> Self {
        self.cache = cache;
        self
    }

    #[must_use]
    #[doc(hidden)]
    pub fn use_descriptor_table(mut self, descriptor_table: Arc<DescriptorTable>) -> Self {
        self.descriptor_table = descriptor_table;
        self
    }

    /// If `true`, the last level will not build filters, reducing the filter size of a database
    /// by ~90% typically.
    ///
    /// **Enable this only if you know that point reads generally are expected to find a key-value pair.**
    #[must_use]
    pub fn expect_point_read_hits(mut self, b: bool) -> Self {
        self.expect_point_read_hits = b;
        self
    }

    /// Sets the pinning policy for filter blocks.
    #[must_use]
    pub fn filter_block_pinning_policy(mut self, policy: PinningPolicy) -> Self {
        self.filter_block_pinning_policy = policy;
        self
    }

    /// Sets the pinning policy for index blocks.
    #[must_use]
    pub fn index_block_pinning_policy(mut self, policy: PinningPolicy) -> Self {
        self.index_block_pinning_policy = policy;
        self
    }

    /// Sets the restart interval inside data blocks.
    ///
    /// A higher restart interval saves space while increasing lookup times
    /// inside data blocks.
    ///
    /// Default = 16
    #[must_use]
    pub fn data_block_restart_interval_policy(mut self, policy: RestartIntervalPolicy) -> Self {
        self.data_block_restart_interval_policy = policy;
        self
    }

    /// Sets the restart interval inside index blocks.
    ///
    /// A higher restart interval saves space while increasing lookup times
    /// inside index blocks.
    ///
    /// Default = 1
    #[must_use]
    pub fn index_block_restart_interval_policy(mut self, policy: RestartIntervalPolicy) -> Self {
        self.index_block_restart_interval_policy = policy;
        self
    }

    /// Sets the filter construction policy.
    #[must_use]
    pub fn filter_policy(mut self, policy: FilterPolicy) -> Self {
        self.filter_policy = policy;
        self
    }

    /// Sets the compression method for data blocks.
    #[must_use]
    pub fn data_block_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.data_block_compression_policy = policy;
        self
    }

    /// Sets the compression method for index blocks.
    #[must_use]
    pub fn index_block_compression_policy(mut self, policy: CompressionPolicy) -> Self {
        self.index_block_compression_policy = policy;
        self
    }

    /// Sets the blob compression method.
    #[must_use]
    pub fn blob_compression(mut self, compression: CompressionType) -> Self {
        self.blob_compression = compression;
        self
    }

    /// Sets the number of levels of the LSM tree (depth of tree).
    ///
    /// Defaults to 7, like `LevelDB` and `RocksDB`.
    ///
    /// Cannot be changed once set.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    #[must_use]
    pub fn level_count(mut self, n: u8) -> Self {
        assert!(n > 0);

        self.level_count = n;
        self
    }

    /// Sets the data block size policy.
    #[must_use]
    pub fn data_block_size_policy(mut self, policy: BlockSizePolicy) -> Self {
        self.data_block_size_policy = policy;
        self
    }

    /// Sets the index block size policy.
    #[must_use]
    pub fn index_block_size_policy(mut self, policy: BlockSizePolicy) -> Self {
        self.index_block_size_policy = policy;
        self
    }

    /// Sets the hash ratio policy for data blocks.
    ///
    /// If greater than 0.0, a hash index is embedded into data blocks that can speed up reads
    /// inside the data block.
    #[must_use]
    pub fn data_block_hash_ratio_policy(mut self, policy: HashRatioPolicy) -> Self {
        self.data_block_hash_ratio_policy = policy;
        self
    }

    /// Sets the target size of blob files.
    ///
    /// Smaller blob files allow more granular garbage collection
    /// which allows lower space amp for lower write I/O cost.
    ///
    /// Larger blob files decrease the number of files on disk and maintenance
    /// overhead.
    ///
    /// Defaults to 64 MiB.
    ///
    /// This option has no effect when not used for opening a blob tree.
    #[must_use]
    pub fn blob_file_target_size(mut self, bytes: u64) -> Self {
        self.blob_file_target_size = bytes;
        self
    }

    /// Sets the key-value separation threshold in bytes.
    ///
    /// Smaller value will reduce compaction overhead and thus write amplification,
    /// at the cost of lower read performance.
    ///
    /// Defaults to 4KiB.
    ///
    /// This option has no effect when not used for opening a blob tree.
    #[must_use]
    pub fn blob_file_separation_threshold(mut self, bytes: u32) -> Self {
        self.blob_file_separation_threshold = bytes;
        self
    }

    /// Opens a tree using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open(self) -> crate::Result<Tree> {
        Tree::open(self)
    }

    /// Opens a blob tree using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open_as_blob_tree(mut self) -> crate::Result<BlobTree> {
        self.tree_type = TreeType::Blob;
        BlobTree::open(self)
    }
}
