use crate::{
    descriptor_table::FileDescriptorTable,
    path::absolute_path,
    segment::meta::{CompressionType, TableType},
    serde::{Deserializable, Serializable},
    BlobTree, BlockCache, DeserializeError, SerializeError, Tree,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use value_log::BlobCache;

pub const CONFIG_HEADER_MAGIC: &[u8] = &[b'L', b'S', b'M', b'T', b'C', b'F', b'G', b'2'];

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

/// Tree configuration
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub struct PersistedConfig {
    /// Tree type (unused)
    pub r#type: TreeType,

    /// What type of compression is used
    pub compression: CompressionType,

    /// Table type (unused)
    table_type: TableType,

    /// Block size of data blocks
    pub data_block_size: u32,

    /// Block size of index blocks
    pub index_block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    pub level_count: u8,
}

const DEFAULT_FILE_FOLDER: &str = ".lsm.data";

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            data_block_size: 4_096,
            index_block_size: 4_096,
            level_count: 7,
            r#type: TreeType::Standard,
            table_type: TableType::Block,
            compression: CompressionType::None,
        }
    }
}

impl Serializable for PersistedConfig {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(CONFIG_HEADER_MAGIC)?;

        writer.write_u8(self.r#type.into())?;
        self.compression.serialize(writer)?;
        writer.write_u8(self.table_type.into())?;

        writer.write_u32::<BigEndian>(self.data_block_size)?;
        writer.write_u32::<BigEndian>(self.index_block_size)?;

        writer.write_u8(self.level_count)?;

        Ok(())
    }
}

impl Deserializable for PersistedConfig {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Check header
        let mut magic = [0u8; CONFIG_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != CONFIG_HEADER_MAGIC {
            return Err(DeserializeError::InvalidHeader("Config"));
        }

        let tree_type = reader.read_u8()?;
        let tree_type = TreeType::try_from(tree_type)
            .map_err(|()| DeserializeError::InvalidTag(("TreeType", tree_type)))?;

        let compression = CompressionType::deserialize(reader)?;

        let table_type = reader.read_u8()?;
        let table_type = TableType::try_from(table_type)
            .map_err(|()| DeserializeError::InvalidTag(("TableType", table_type)))?;

        let data_block_size = reader.read_u32::<BigEndian>()?;
        let index_block_size = reader.read_u32::<BigEndian>()?;

        let level_count = reader.read_u8()?;

        Ok(Self {
            r#type: tree_type,
            compression,
            table_type,
            data_block_size,
            index_block_size,
            level_count,
        })
    }
}

#[derive(Clone)]
/// Tree configuration builder
pub struct Config {
    /// Persistent configuration
    ///
    /// Once set, this configuration is permanent
    #[doc(hidden)]
    pub inner: PersistedConfig,

    /// Folder path
    #[doc(hidden)]
    pub path: PathBuf,

    /// Block cache to use
    #[doc(hidden)]
    pub block_cache: Arc<BlockCache>,

    /// Blob cache to use
    #[doc(hidden)]
    pub blob_cache: Arc<BlobCache>,

    /// Blob file (value log segment) target size in bytes
    #[doc(hidden)]
    pub blob_file_target_size: u64,

    /// Key-value separation threshold in bytes
    #[doc(hidden)]
    pub blob_file_separation_threshold: u32,

    /// Descriptor table to use
    #[doc(hidden)]
    pub descriptor_table: Arc<FileDescriptorTable>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: absolute_path(DEFAULT_FILE_FOLDER),
            block_cache: Arc::new(BlockCache::with_capacity_bytes(8 * 1_024 * 1_024)),
            descriptor_table: Arc::new(FileDescriptorTable::new(128, 2)),

            blob_cache: Arc::new(BlobCache::with_capacity_bytes(8 * 1_024 * 1_024)),
            blob_file_target_size: 64 * 1_024 * 1_024,
            blob_file_separation_threshold: 4 * 1_024,

            inner: PersistedConfig::default(),
        }
    }
}

impl Config {
    /// Initializes a new config
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let inner = PersistedConfig::default();

        Self {
            inner,
            path: absolute_path(path),
            ..Default::default()
        }
    }

    /// Sets the compression method.
    ///
    /// Using some compression is recommended.
    ///
    /// Default = None
    #[must_use]
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.inner.compression = compression;
        self
    }

    /// Sets the amount of levels of the LSM tree (depth of tree).
    ///
    /// Defaults to 7, like `LevelDB` and `RocksDB`.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    #[must_use]
    pub fn level_count(mut self, n: u8) -> Self {
        assert!(n > 0);

        self.inner.level_count = n;
        self
    }

    /// Sets the block size.
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
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);
        assert!(block_size <= 512 * 1_024);

        self.inner.data_block_size = block_size;
        self.inner.index_block_size = block_size;

        self
    }

    /// Sets the block cache.
    ///
    /// You can create a global [`BlockCache`] and share it between multiple
    /// trees to cap global cache memory usage.
    ///
    /// Defaults to a block cache with 8 MiB of capacity *per tree*.
    #[must_use]
    pub fn block_cache(mut self, block_cache: Arc<BlockCache>) -> Self {
        self.block_cache = block_cache;
        self
    }

    /// Sets the block cache.
    ///
    /// You can create a global [`BlobCache`] and share it between multiple
    /// trees and their value logs to cap global cache memory usage.
    ///
    /// Defaults to a block cache with 8 MiB of capacity *per tree*.
    ///
    /// This function has no effect when not used for opening a blob tree.
    #[must_use]
    pub fn blob_cache(mut self, blob_cache: Arc<BlobCache>) -> Self {
        self.blob_cache = blob_cache;
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
    /// This function has no effect when not used for opening a blob tree.
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
    /// This function has no effect when not used for opening a blob tree.
    #[must_use]
    pub fn blob_file_separation_threshold(mut self, bytes: u64) -> Self {
        self.blob_file_target_size = bytes;
        self
    }

    #[must_use]
    #[doc(hidden)]
    pub fn descriptor_table(mut self, descriptor_table: Arc<FileDescriptorTable>) -> Self {
        self.descriptor_table = descriptor_table;
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
        self.inner.r#type = TreeType::Blob;
        BlobTree::open(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn tree_config_raw() -> crate::Result<()> {
        let config = PersistedConfig {
            r#type: TreeType::Standard,
            compression: CompressionType::None,
            table_type: TableType::Block,
            data_block_size: 4_096,
            index_block_size: 4_096,
            level_count: 7,
        };

        let mut bytes = vec![];
        config.serialize(&mut bytes)?;

        #[rustfmt::skip]
        let raw = &[
            // Magic
            b'L', b'S', b'M', b'T', b'C', b'F', b'G', b'2',

            // Tree type
            0,

            // Compression
            0, 0,

            // Table type
            0,

            // Data block size
            0, 0, 0x10, 0x00,

            // Index block size
            0, 0, 0x10, 0x00,

            // Levels
            7,
        ];

        assert_eq!(bytes, raw);

        Ok(())
    }

    #[test]
    fn tree_config_serde_round_trip() -> crate::Result<()> {
        let config = PersistedConfig {
            r#type: TreeType::Standard,
            compression: CompressionType::None,
            table_type: TableType::Block,
            data_block_size: 4_096,
            index_block_size: 4_096,
            level_count: 7,
        };

        let mut bytes = vec![];
        config.serialize(&mut bytes)?;

        let mut cursor = Cursor::new(bytes);
        let config_copy = PersistedConfig::deserialize(&mut cursor)?;

        assert_eq!(config, config_copy);

        Ok(())
    }
}
