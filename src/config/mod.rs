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
    compaction::filter::Factory,
    comparator::{self, SharedComparator},
    encryption::EncryptionProvider,
    file::TABLES_FOLDER,
    fs::{Fs, StdFs},
    merge_operator::MergeOperator,
    path::absolute_path,
    prefix::PrefixExtractor,
    version::DEFAULT_LEVEL_COUNT,
    AnyTree, BlobTree, Cache, CompressionType, DescriptorTable, SequenceNumberCounter,
    SharedSequenceNumberGenerator, Tree,
};
use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Per-level filesystem routing entry for tiered storage.
///
/// Maps a range of LSM levels to a base directory and filesystem backend.
/// Tables at these levels are stored under `path/tables/`.
///
/// # Example
///
/// ```
/// use lsm_tree::config::LevelRoute;
/// use lsm_tree::fs::StdFs;
/// use std::sync::Arc;
///
/// // Hot tier: L0-L1 on NVMe
/// let hot = LevelRoute {
///     levels: 0..2,
///     path: "/mnt/nvme/db".into(),
///     fs: Arc::new(StdFs),
/// };
///
/// // Cold tier: L4-L6 on HDD
/// let cold = LevelRoute {
///     levels: 4..7,
///     path: "/mnt/hdd/db".into(),
///     fs: Arc::new(StdFs),
/// };
/// ```
#[derive(Clone)]
pub struct LevelRoute {
    /// LSM levels this route covers (e.g., `0..2` for L0–L1).
    pub levels: Range<u8>,

    /// Base data directory for tables at these levels.
    pub path: PathBuf,

    /// Filesystem backend for I/O at these levels.
    pub fs: Arc<dyn Fs>,
}

impl std::fmt::Debug for LevelRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelRoute")
            .field("levels", &self.levels)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

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
///
/// The generic parameter `F` selects the filesystem backend.
/// It defaults to [`StdFs`], so existing code that writes `Config`
/// without a type parameter continues to work unchanged.
pub struct Config<F: Fs = StdFs> {
    /// Folder path
    #[doc(hidden)]
    pub path: PathBuf,

    /// Filesystem backend
    ///
    // All Config fields are `#[doc(hidden)] pub` by convention — callers use
    // builder methods or `..Default::default()`, not struct literals directly.
    // A `with_fs()` builder will be added when call-site refactoring lands.
    #[doc(hidden)]
    pub fs: Arc<F>,

    /// Per-level filesystem routing for tiered storage.
    ///
    /// When set, tables at different LSM levels can be stored on different
    /// storage devices (e.g., NVMe for L0–L1, SSD for L2–L4, HDD for L5–L6).
    /// Each entry maps a range of levels to a base directory and filesystem
    /// backend. Uncovered levels fall back to the primary `path` and `fs`.
    ///
    /// Zero additional overhead when `None` — only a single branch check;
    /// path construction allocations are unchanged.
    #[doc(hidden)]
    pub level_routes: Option<Vec<LevelRoute>>,

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
    pub compaction_filter_factory: Option<Arc<dyn Factory>>,

    /// Prefix extractor for prefix bloom filters.
    ///
    /// When set, the bloom filter indexes extracted prefixes in addition to
    /// full keys, allowing prefix scans to skip segments that contain no
    /// matching prefixes.
    pub prefix_extractor: Option<Arc<dyn PrefixExtractor>>,

    /// Merge operator for commutative operations
    ///
    /// When set, enables `merge()` operations that store partial updates
    /// which are lazily combined during reads and compaction.
    pub merge_operator: Option<Arc<dyn MergeOperator>>,

    #[doc(hidden)]
    pub kv_separation_opts: Option<KvSeparationOptions>,

    /// Custom user key comparator.
    ///
    /// When set, all key comparisons use this comparator instead of the
    /// default lexicographic byte ordering. Once a tree is opened with a
    /// comparator, it must always be re-opened with the same comparator.
    // Not `pub` — use `Config::comparator()` builder method as the public API.
    #[doc(hidden)]
    pub(crate) comparator: SharedComparator,

    /// Block-level encryption provider for encryption at rest.
    ///
    /// When set, all blocks (data, index, filter, meta) are encrypted
    /// using this provider after compression and before checksumming.
    pub(crate) encryption: Option<Arc<dyn EncryptionProvider>>,

    /// Pre-trained zstd dictionary for dictionary compression.
    ///
    /// When set together with a [`CompressionType::ZstdDict`] compression
    /// policy, data blocks are compressed using this dictionary. The
    /// dictionary must remain the same for the lifetime of the tree —
    /// opening a tree with a different dictionary will produce
    /// [`Error::ZstdDictMismatch`](crate::Error::ZstdDictMismatch) errors.
    #[cfg(feature = "zstd")]
    pub(crate) zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// The global sequence number generator.
    ///
    /// Should be shared between multiple trees of a database.
    pub(crate) seqno: SharedSequenceNumberGenerator,

    /// Sequence number watermark that is visible to readers.
    ///
    /// Used for MVCC snapshots and to control which updates are
    /// observable in a given view of the database.
    pub(crate) visible_seqno: SharedSequenceNumberGenerator,
}

// TODO: remove default?
impl Default for Config {
    fn default() -> Self {
        Self {
            path: absolute_path(Path::new(DEFAULT_FILE_FOLDER)),
            fs: Arc::new(StdFs),
            level_routes: None,
            descriptor_table: Some(Arc::new(DescriptorTable::new(256))),
            seqno: SharedSequenceNumberGenerator::from(SequenceNumberCounter::default()),
            visible_seqno: SharedSequenceNumberGenerator::from(SequenceNumberCounter::default()),

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
            merge_operator: None,

            prefix_extractor: None,

            expect_point_read_hits: false,

            kv_separation_opts: None,

            #[cfg(feature = "zstd")]
            zstd_dictionary: None,

            comparator: comparator::default_comparator(),
            encryption: None,
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
            seqno: Arc::new(seqno),
            visible_seqno: Arc::new(visible_seqno),
            ..Default::default()
        }
    }

    /// Opens a tree using the config.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    /// Returns [`Error::ZstdDictMismatch`](crate::Error::ZstdDictMismatch) if
    /// the compression policy references a `dict_id` that doesn't match the
    /// configured dictionary.
    pub fn open(self) -> crate::Result<AnyTree> {
        #[cfg(feature = "zstd")]
        self.validate_zstd_dictionary()?;

        Ok(if self.kv_separation_opts.is_some() {
            AnyTree::Blob(BlobTree::open(self)?)
        } else {
            AnyTree::Standard(Tree::open(self)?)
        })
    }

    /// Validates that every `ZstdDict` entry in compression policies references
    /// a `dict_id` that matches the configured dictionary. Catches mismatches
    /// at open time rather than at first block write/read.
    #[cfg(feature = "zstd")]
    fn validate_zstd_dictionary(&self) -> crate::Result<()> {
        let dict_id = self.zstd_dictionary.as_ref().map(|d| d.id());

        // NOTE: Only data block policies are validated. Index blocks never
        // carry a dictionary — Writer::use_index_block_compression() downgrades
        // ZstdDict to plain Zstd. Validating index policies here would reject
        // configs that use ZstdDict solely for index blocks even though the
        // writer handles them correctly.
        for ct in self.data_block_compression_policy.iter() {
            if let &CompressionType::ZstdDict {
                dict_id: required, ..
            } = ct
            {
                match dict_id {
                    None => {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: required,
                            got: None,
                        });
                    }
                    Some(actual) if actual != required => {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: required,
                            got: Some(actual),
                        });
                    }
                    _ => {}
                }
            }
        }

        // Blob files don't support dictionary compression — reject early.
        if let Some(ref kv_opts) = self.kv_separation_opts {
            if matches!(kv_opts.compression, CompressionType::ZstdDict { .. }) {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "zstd dictionary compression is not supported for blob files",
                )));
            }
        }

        Ok(())
    }

    /// Like [`Config::new`], but accepts pre-built shared generators.
    ///
    /// This is useful when the caller already has
    /// [`SharedSequenceNumberGenerator`] instances (e.g., from a higher-level
    /// database that shares generators across multiple trees).
    pub fn new_with_generators<P: AsRef<Path>>(
        path: P,
        seqno: SharedSequenceNumberGenerator,
        visible_seqno: SharedSequenceNumberGenerator,
    ) -> Self {
        Self {
            path: absolute_path(path.as_ref()),
            seqno,
            visible_seqno,
            ..Default::default()
        }
    }
}

impl<F: Fs> Config<F> {
    /// Returns the tables folder path and [`Fs`] backend for the given level.
    ///
    /// If [`level_routes`](Self::level_routes) has an entry covering this
    /// level, uses that entry's path and `Fs`. Otherwise falls back to the
    /// primary [`path`](Self::path) and [`fs`](Self::fs).
    #[must_use]
    pub fn tables_folder_for_level(&self, level: u8) -> (PathBuf, Arc<dyn Fs>) {
        if let Some(routes) = &self.level_routes {
            for route in routes {
                if route.levels.contains(&level) {
                    return (route.path.join(TABLES_FOLDER), route.fs.clone());
                }
            }
        }
        (self.path.join(TABLES_FOLDER), self.fs.clone())
    }

    /// Returns all unique tables folders that need to be scanned during
    /// recovery: the primary folder plus every [`LevelRoute`] folder.
    #[must_use]
    pub fn all_tables_folders(&self) -> Vec<(PathBuf, Arc<dyn Fs>)> {
        let primary_fs: Arc<dyn Fs> = self.fs.clone();
        let mut folders: Vec<(PathBuf, Arc<dyn Fs>)> =
            vec![(self.path.join(TABLES_FOLDER), primary_fs)];

        if let Some(routes) = &self.level_routes {
            for route in routes {
                let folder = route.path.join(TABLES_FOLDER);
                // Dedup by path: scanning the same directory twice would cause
                // already-recovered tables to be classified as orphans and deleted.
                if !folders.iter().any(|(p, _)| *p == folder) {
                    folders.push((folder, route.fs.clone()));
                }
            }
        }

        folders
    }

    /// Configures per-level filesystem routing for tiered storage.
    ///
    /// Each [`LevelRoute`] maps a range of LSM levels to a base directory
    /// and filesystem backend. Levels not covered by any route fall back to
    /// the primary `path` and `fs`.
    ///
    /// # Reopen contract
    ///
    /// The route configuration is **not persisted** in the manifest.
    /// On reopen, the [`Config`] must specify `level_routes` such that
    /// [`all_tables_folders`](Self::all_tables_folders) includes every
    /// directory and filesystem pair that may contain existing SST files
    /// for this tree.
    ///
    /// Changing the mapping from levels to paths is allowed as long as
    /// the previously used folders remain covered. If old folders are
    /// omitted, recovery will fail (`Unrecoverable`) because the missing
    /// tables cannot be found.
    ///
    /// # Panics
    ///
    /// Panics if any route has an empty range or if any two routes have
    /// overlapping level ranges.
    #[must_use]
    pub fn level_routes(mut self, routes: Vec<LevelRoute>) -> Self {
        // Validate no empty/inverted ranges
        for route in &routes {
            assert!(
                route.levels.start < route.levels.end,
                "empty or inverted level route range: {:?}",
                route.levels,
            );
        }

        // Validate no overlapping ranges
        for (i, a) in routes.iter().enumerate() {
            for b in routes.iter().skip(i + 1) {
                assert!(
                    a.levels.end <= b.levels.start || b.levels.end <= a.levels.start,
                    "overlapping level routes: {:?} and {:?}",
                    a.levels,
                    b.levels,
                );
            }
        }
        self.level_routes = if routes.is_empty() {
            None
        } else {
            // Normalize paths the same way Config::new normalizes self.path
            Some(
                routes
                    .into_iter()
                    .map(|mut r| {
                        r.path = absolute_path(&r.path);
                        r
                    })
                    .collect(),
            )
        };
        self
    }

    /// Overrides the sequence number generator.
    ///
    /// By default, [`SequenceNumberCounter`] is used. This allows plugging in
    /// a custom generator (e.g., HLC for distributed databases).
    #[must_use]
    pub fn seqno_generator(mut self, generator: SharedSequenceNumberGenerator) -> Self {
        self.seqno = generator;
        self
    }

    /// Overrides the visible sequence number generator.
    #[must_use]
    pub fn visible_seqno_generator(mut self, generator: SharedSequenceNumberGenerator) -> Self {
        self.visible_seqno = generator;
        self
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
    pub fn with_compaction_filter_factory(mut self, factory: Option<Arc<dyn Factory>>) -> Self {
        self.compaction_filter_factory = factory;
        self
    }

    /// Sets the prefix extractor for prefix bloom filters.
    ///
    /// When configured, bloom filters will index key prefixes returned by
    /// the extractor. Prefix scans can then skip segments whose bloom
    /// filter reports no match for the scan prefix.
    #[must_use]
    pub fn prefix_extractor(mut self, extractor: Arc<dyn PrefixExtractor>) -> Self {
        self.prefix_extractor = Some(extractor);
        self
    }

    /// Installs a merge operator for commutative operations.
    ///
    /// When set, enables [`crate::AbstractTree::merge`] which stores partial updates
    /// (operands) that are lazily combined during reads and compaction.
    #[must_use]
    pub fn with_merge_operator(mut self, op: Option<Arc<dyn MergeOperator>>) -> Self {
        self.merge_operator = op;
        self
    }

    /// Sets a custom user key comparator.
    ///
    /// When configured, all key ordering (memtable, block index, merge,
    /// range scans) uses this comparator instead of the default lexicographic
    /// byte ordering.
    ///
    /// # Important
    ///
    /// The comparator's [`crate::UserComparator::name`] is persisted when a tree is
    /// first created. On subsequent opens the stored name is compared against
    /// the supplied comparator's name — a mismatch causes the open to fail
    /// with [`Error::ComparatorMismatch`](crate::Error::ComparatorMismatch).
    #[must_use]
    pub fn comparator(mut self, comparator: SharedComparator) -> Self {
        self.comparator = comparator;
        self
    }

    /// Sets the block-level encryption provider for encryption at rest.
    ///
    /// When set, all blocks written to SST files are encrypted after
    /// compression and before checksumming, using the provided
    /// [`EncryptionProvider`].
    ///
    /// The caller is responsible for key management and rotation.
    /// See [`crate::Aes256GcmProvider`] (behind the `encryption` feature)
    /// for a ready-to-use AES-256-GCM implementation.
    ///
    /// **Important constraints:**
    /// - Encryption state is NOT recorded in SST metadata. Opening an
    ///   encrypted tree without the correct provider (or vice versa) will
    ///   cause block validation errors, not silent corruption.
    /// - Blob files (KV-separated large values) are NOT covered by
    ///   block-level encryption. Large values stored via KV separation
    ///   remain in plaintext on disk.
    #[must_use]
    pub fn with_encryption(mut self, encryption: Option<Arc<dyn EncryptionProvider>>) -> Self {
        self.encryption = encryption;
        self
    }

    /// Sets the pre-trained zstd dictionary for dictionary compression.
    ///
    /// When set, data blocks using [`CompressionType::ZstdDict`] will be
    /// compressed and decompressed with this dictionary. The dictionary
    /// should be trained on representative data samples for best results.
    ///
    /// Create a dictionary with [`ZstdDictionary::new`](crate::ZstdDictionary::new),
    /// then use [`CompressionType::zstd_dict`] to create a matching
    /// compression type:
    ///
    /// ```ignore
    /// use lsm_tree::{CompressionType, ZstdDictionary};
    ///
    /// let dict = ZstdDictionary::new(&training_data);
    /// let compression = CompressionType::zstd_dict(3, dict.id()).unwrap();
    ///
    /// config
    ///     .zstd_dictionary(Some(Arc::new(dict)))
    ///     .data_block_compression_policy(CompressionPolicy::all(compression));
    /// ```
    #[cfg(feature = "zstd")]
    #[must_use]
    pub fn zstd_dictionary(
        mut self,
        dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
    ) -> Self {
        self.zstd_dictionary = dictionary;
        self
    }
}
