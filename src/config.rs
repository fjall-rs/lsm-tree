// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    path::absolute_path, prefix::SharedPrefixExtractor, BlobTree, Cache, CompressionType,
    DescriptorTable, Tree,
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

#[derive(Clone)]
/// Tree configuration builder
pub struct Config {
    /// Folder path
    #[doc(hidden)]
    pub path: PathBuf,

    /// Tree type (unused)
    #[allow(unused)]
    pub tree_type: TreeType,

    /// What type of compression is used
    pub compression: CompressionType,

    /// What type of compression is used for blobs
    pub blob_compression: CompressionType,

    /// Restart interval inside data blocks
    pub data_block_restart_interval: u8,

    /// Hash bytes per key in data blocks
    pub data_block_hash_ratio: f32,

    /// Block size of data blocks
    pub data_block_size: u32,

    /// Block size of index blocks
    pub index_block_size: u32,

    /// Number of levels of the LSM tree (depth of tree)
    pub level_count: u8,

    /// Bits per key for levels that are not L0, L1, L2
    // NOTE: bloom_bits_per_key is not conditionally compiled,
    // because that would change the file format
    #[doc(hidden)]
    pub bloom_bits_per_key: i8,

    /// Block cache to use
    #[doc(hidden)]
    pub cache: Arc<Cache>,

    /// Blob file (value log segment) target size in bytes
    #[doc(hidden)]
    pub blob_file_target_size: u64,

    /// Key-value separation threshold in bytes
    #[doc(hidden)]
    pub blob_file_separation_threshold: u32,

    /// Descriptor table to use
    #[doc(hidden)]
    pub descriptor_table: Arc<DescriptorTable>,

    /// Prefix extractor for filters
    #[doc(hidden)]
    pub prefix_extractor: Option<SharedPrefixExtractor>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: absolute_path(Path::new(DEFAULT_FILE_FOLDER)),
            descriptor_table: Arc::new(DescriptorTable::new(256)),

            cache: Arc::new(Cache::with_capacity_bytes(/* 16 MiB */ 16 * 1_024 * 1_024)),

            data_block_restart_interval: 16,
            data_block_hash_ratio: 0.0,

            data_block_size: /* 4 KiB */ 4_096,
            index_block_size: /* 4 KiB */ 4_096,
            level_count: 7,
            tree_type: TreeType::Standard,
            // table_type: TableType::Block,
            compression: CompressionType::None,
            blob_compression: CompressionType::None,
            bloom_bits_per_key: 10,
            prefix_extractor: None,

            blob_file_target_size: /* 64 MiB */ 64 * 1_024 * 1_024,
            blob_file_separation_threshold: /* 4 KiB */ 4 * 1_024,
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

    /// Sets the restart interval inside data blocks.
    ///
    /// A higher restart interval saves space while increasing lookup times
    /// inside data blocks.
    ///
    /// Default = 16
    #[must_use]
    pub fn data_block_restart_interval(mut self, i: u8) -> Self {
        self.data_block_restart_interval = i;
        self
    }

    /// Sets the hash ratio for the hash index in data blocks.
    ///
    /// The hash index speeds up point queries by using an embedded
    /// hash map in data blocks, but uses more space/memory.
    ///
    /// Something along the lines of 1.0 - 2.0 is sensible.
    ///
    /// If 0, the hash index is not constructed.
    ///
    /// Default = 0.0
    #[must_use]
    pub fn data_block_hash_ratio(mut self, ratio: f32) -> Self {
        self.data_block_hash_ratio = ratio;
        self
    }

    /// Sets the bits per key to use for bloom filters
    /// in levels that are not L0 or L1.
    ///
    /// Use -1 to disable bloom filters even in L0, L1, L2.
    ///
    /// Defaults to 10 bits.
    ///
    /// # Panics
    ///
    /// Panics if `n` is less than -1.
    #[must_use]
    pub fn bloom_bits_per_key(mut self, bits: i8) -> Self {
        assert!(bits >= -1, "invalid bits_per_key value");

        self.bloom_bits_per_key = bits;
        self
    }

    /// Sets the compression method.
    ///
    /// Using some compression is recommended.
    ///
    /// Default = None
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the compression method.
    ///
    /// Using some compression is recommended.
    ///
    /// Default = None
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

    /// Sets the data block size.
    ///
    /// Defaults to 4 KiB (4096 bytes).
    ///
    /// For point read heavy workloads (get) a sensible default is
    /// somewhere between 4 - 8 KiB, depending on the average value size.
    ///
    /// For scan heavy workloads (range, prefix), use 16 - 64 KiB
    /// which also increases compression efficiency.
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB or larger than 512 KiB.
    #[must_use]
    pub fn data_block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);
        assert!(block_size <= 512 * 1_024);
        self.data_block_size = block_size;
        self
    }

    /// Sets the index block size.
    ///
    /// Defaults to 4 KiB (4096 bytes).
    ///
    /// For point read heavy workloads (get) a sensible default is
    /// somewhere between 4 - 8 KiB, depending on the average value size.
    ///
    /// For scan heavy workloads (range, prefix), use 16 - 64 KiB
    /// which also increases compression efficiency.
    ///
    /// # Panics
    ///
    /// Panics if the block size is smaller than 1 KiB or larger than 512 KiB.
    #[must_use]
    pub fn index_block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);
        assert!(block_size <= 512 * 1_024);
        self.index_block_size = block_size;
        self
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

    #[must_use]
    #[doc(hidden)]
    pub fn descriptor_table(mut self, descriptor_table: Arc<DescriptorTable>) -> Self {
        self.descriptor_table = descriptor_table;
        self
    }

    /// Sets the prefix extractor for filters.
    ///
    /// A prefix extractor allows filters to index prefixes of keys
    /// instead of (or in addition to) the full keys. This enables efficient
    /// filtering for prefix-based queries.
    ///
    /// # Example
    ///
    /// ```
    /// # use lsm_tree::Config;
    /// use lsm_tree::prefix::FixedPrefixExtractor;
    /// use std::sync::Arc;
    ///
    /// # let path = tempfile::tempdir()?;
    /// let config = Config::new(path)
    ///     .prefix_extractor(Arc::new(FixedPrefixExtractor::new(8)));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[must_use]
    pub fn prefix_extractor(mut self, extractor: SharedPrefixExtractor) -> Self {
        self.prefix_extractor = Some(extractor);
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
