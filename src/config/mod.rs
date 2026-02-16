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

/// Partitioning policy for indexes and filters
pub type PartitioningPolicy = PinningPolicy;

use crate::{
    compaction::filter::CompactionFilterFactory, path::absolute_path, version::DEFAULT_LEVEL_COUNT,
    AnyTree, BlobTree, Cache, CompressionType, DescriptorTable, SequenceNumberCounter, Tree,
};
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

/// Options for key-value separation
#[derive(Clone, Debug, PartialEq)]
pub struct KvSeparationOptions {
    /// What type of compression is used for blobs
    #[doc(hidden)]
    pub compression: CompressionType,

    /// Blob file target size in bytes
    #[doc(hidden)]
    pub file_target_size: u64,

    /// Key-value separation threshold in bytes
    #[doc(hidden)]
    pub separation_threshold: u32,

    #[doc(hidden)]
    pub staleness_threshold: f32,

    #[doc(hidden)]
    pub age_cutoff: f32,
}

impl Default for KvSeparationOptions {
    fn default() -> Self {
        Self {
            #[cfg(feature="lz4")]
            compression:   CompressionType::Lz4,

            #[cfg(not(feature="lz4"))]
            compression: CompressionType::None,

            file_target_size: /* 64 MiB */ 64 * 1_024 * 1_024,
            separation_threshold: /* 1 KiB */ 1_024,

            staleness_threshold: 0.25,
            age_cutoff: 0.25,
        }
    }
}

impl KvSeparationOptions {
    /// Sets the blob compression method.
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
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
    #[must_use]
    pub fn file_target_size(mut self, bytes: u64) -> Self {
        self.file_target_size = bytes;
        self
    }

    /// Sets the key-value separation threshold in bytes.
    ///
    /// Smaller value will reduce compaction overhead and thus write amplification,
    /// at the cost of lower read performance.
    ///
    /// Defaults to 1 KiB.
    #[must_use]
    pub fn separation_threshold(mut self, bytes: u32) -> Self {
        self.separation_threshold = bytes;
        self
    }

    /// Sets the staleness threshold percentage.
    ///
    /// The staleness percentage determines how much a blob file needs to be fragmented to be
    /// picked up by the garbage collection.
    ///
    /// Defaults to 33%.
    #[must_use]
    pub fn staleness_threshold(mut self, ratio: f32) -> Self {
        self.staleness_threshold = ratio;
        self
    }

    /// Sets the age cutoff threshold.
    ///
    /// Defaults to 20%.
    #[must_use]
    pub fn age_cutoff(mut self, ratio: f32) -> Self {
        self.age_cutoff = ratio;
        self
    }
}

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
    pub descriptor_table: Option<Arc<DescriptorTable>>,

    /// Number of levels of the LSM tree (depth of tree)
    ///
    /// Once set, the level count is fixed (in the "manifest" file)
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

    /// Whether to pin index blocks
    pub index_block_pinning_policy: PinningPolicy,

    /// Whether to pin filter blocks
    pub filter_block_pinning_policy: PinningPolicy,

    /// Whether to pin top level index of partitioned index
    pub top_level_index_block_pinning_policy: PinningPolicy,

    /// Whether to pin top level index of partitioned filter
    pub top_level_filter_block_pinning_policy: PinningPolicy,

    /// Data block hash ratio
    pub data_block_hash_ratio_policy: HashRatioPolicy,

    /// Whether to partition index blocks
    pub index_block_partitioning_policy: PartitioningPolicy,

    /// Whether to partition filter blocks
    pub filter_block_partitioning_policy: PartitioningPolicy,

    /// Partition size when using partitioned indexes
    pub index_block_partition_size_policy: BlockSizePolicy,

    /// Partition size when using partitioned filters
    pub filter_block_partition_size_policy: BlockSizePolicy,

    /// If `true`, the last level will not build filters, reducing the filter size of a database
    /// by ~90% typically
    pub(crate) expect_point_read_hits: bool,

    /// Filter construction policy
    pub filter_policy: FilterPolicy,

    /// Compaction filter factory
    pub compaction_filter_factory: Option<Box<dyn CompactionFilterFactory>>,

    #[doc(hidden)]
    pub kv_separation_opts: Option<KvSeparationOptions>,

    /// The global sequence number generator
    ///
    /// Should be shared between multple trees of a database
    pub(crate) seqno: SequenceNumberCounter,

    pub(crate) visible_seqno: SequenceNumberCounter,
}

// TODO: remove default?
impl Default for Config {
    fn default() -> Self {
        Self {
            path: absolute_path(Path::new(DEFAULT_FILE_FOLDER)),
            descriptor_table: Some(Arc::new(DescriptorTable::new(256))),
            seqno: SequenceNumberCounter::default(),
            visible_seqno: SequenceNumberCounter::default(),

            cache: Arc::new(Cache::with_capacity_bytes(
                /* 16 MiB */ 16 * 1_024 * 1_024,
            )),

            data_block_restart_interval_policy: RestartIntervalPolicy::all(16),
            index_block_restart_interval_policy: RestartIntervalPolicy::all(1),

            level_count: DEFAULT_LEVEL_COUNT,

            data_block_size_policy: BlockSizePolicy::all(4_096),

            index_block_pinning_policy: PinningPolicy::new([true, true, false]),
            filter_block_pinning_policy: PinningPolicy::new([true, false]),

            top_level_index_block_pinning_policy: PinningPolicy::all(true), // TODO: implement
            top_level_filter_block_pinning_policy: PinningPolicy::all(true), // TODO: implement

            index_block_partitioning_policy: PinningPolicy::new([false, false, false, true]),
            filter_block_partitioning_policy: PinningPolicy::new([false, false, false, true]),

            index_block_partition_size_policy: BlockSizePolicy::all(4_096), // TODO: implement
            filter_block_partition_size_policy: BlockSizePolicy::all(4_096), // TODO: implement

            data_block_compression_policy: ({
                #[cfg(feature = "lz4")]
                let c = CompressionPolicy::new([CompressionType::None, CompressionType::Lz4]);

                #[cfg(not(feature = "lz4"))]
                let c = CompressionPolicy::new([CompressionType::None]);

                c
            }),
            index_block_compression_policy: CompressionPolicy::all(CompressionType::None),

            data_block_hash_ratio_policy: HashRatioPolicy::all(0.0),

            filter_policy: FilterPolicy::all(FilterPolicyEntry::Bloom(
                BloomConstructionPolicy::BitsPerKey(10.0),
            )),

            compaction_filter_factory: None,

            expect_point_read_hits: false,

            kv_separation_opts: None,
        }
    }
}

impl Config {
    /// Initializes a new config
    pub fn new<P: AsRef<Path>>(
        path: P,
        seqno: SequenceNumberCounter,
        visible_seqno: SequenceNumberCounter,
    ) -> Self {
        Self {
            path: absolute_path(path.as_ref()),
            seqno,
            visible_seqno,
            ..Default::default()
        }
    }

    /// Sets the global cache.
    ///
    /// You can create a global [`Cache`] and share it between multiple
    /// trees to cap global cache memory usage.
    ///
    /// Defaults to a cache with 16 MiB of capacity *per tree*.
    #[must_use]
    pub fn use_cache(mut self, cache: Arc<Cache>) -> Self {
        self.cache = cache;
        self
    }

    /// Sets the file descriptor cache.
    ///
    /// Can be shared across trees.
    #[must_use]
    pub fn use_descriptor_table(mut self, descriptor_table: Option<Arc<DescriptorTable>>) -> Self {
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

    /// Sets the partitioning policy for filter blocks.
    #[must_use]
    pub fn filter_block_partitioning_policy(mut self, policy: PinningPolicy) -> Self {
        self.filter_block_partitioning_policy = policy;
        self
    }

    /// Sets the partitioning policy for index blocks.
    #[must_use]
    pub fn index_block_partitioning_policy(mut self, policy: PinningPolicy) -> Self {
        self.index_block_partitioning_policy = policy;
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

    // TODO: not supported yet in index blocks
    // /// Sets the restart interval inside index blocks.
    // ///
    // /// A higher restart interval saves space while increasing lookup times
    // /// inside index blocks.
    // ///
    // /// Default = 1
    // #[must_use]
    // pub fn index_block_restart_interval_policy(mut self, policy: RestartIntervalPolicy) -> Self {
    //     self.index_block_restart_interval_policy = policy;
    //     self
    // }

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

    // TODO: level count is fixed to 7 right now
    // /// Sets the number of levels of the LSM tree (depth of tree).
    // ///
    // /// Defaults to 7, like `LevelDB` and `RocksDB`.
    // ///
    // /// Cannot be changed once set.
    // ///
    // /// # Panics
    // ///
    // /// Panics if `n` is 0.
    // #[must_use]
    // pub fn level_count(mut self, n: u8) -> Self {
    //     assert!(n > 0);

    //     self.level_count = n;
    //     self
    // }

    /// Sets the data block size policy.
    #[must_use]
    pub fn data_block_size_policy(mut self, policy: BlockSizePolicy) -> Self {
        self.data_block_size_policy = policy;
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

    /// Toggles key-value separation.
    #[must_use]
    pub fn with_kv_separation(mut self, opts: Option<KvSeparationOptions>) -> Self {
        self.kv_separation_opts = opts;
        self
    }

    /// Installs a custom compaction filter.
    #[must_use]
    pub fn with_compaction_filter_factory(
        mut self,
        factory: Option<Box<dyn CompactionFilterFactory>>,
    ) -> Self {
        self.compaction_filter_factory = factory;
        self
    }

    /// Opens a tree using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn open(self) -> crate::Result<AnyTree> {
        Ok(if self.kv_separation_opts.is_some() {
            AnyTree::Blob(BlobTree::open(self)?)
        } else {
            AnyTree::Standard(Tree::open(self)?)
        })
    }
}
