use crate::{
    descriptor_table::FileDescriptorTable,
    path::absolute_path,
    segment::meta::{CompressionType, TableType},
    serde::{Deserializable, Serializable},
    BlockCache, DeserializeError, SerializeError, Tree,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

pub const CONFIG_HEADER_MAGIC: &[u8] = &[b'F', b'J', b'L', b'L', b'C', b'F', b'G', b'1'];

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum TreeType {
    Standard,
}

impl From<TreeType> for u8 {
    fn from(val: TreeType) -> Self {
        match val {
            TreeType::Standard => 0,
        }
    }
}

impl TryFrom<u8> for TreeType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Standard),
            _ => Err(()),
        }
    }
}

/// Tree configuration
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub struct PersistedConfig {
    /// Tree type (unused)
    r#type: TreeType,

    /// What type of compression is used
    compression: CompressionType,

    /// Table type (unused)
    table_type: TableType,

    /// Block size of data and index blocks
    pub block_size: u32,

    /// Amount of levels of the LSM tree (depth of tree)
    pub level_count: u8,
}

const DEFAULT_FILE_FOLDER: &str = ".lsm.data";

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            block_size: 4_096,
            level_count: 7,
            r#type: TreeType::Standard,
            compression: CompressionType::Lz4,
            table_type: TableType::Block,
        }
    }
}

impl Serializable for PersistedConfig {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(CONFIG_HEADER_MAGIC)?;

        writer.write_u8(self.r#type.into())?;
        writer.write_u8(self.compression.into())?;
        writer.write_u8(self.table_type.into())?;
        writer.write_u32::<BigEndian>(self.block_size)?;
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

        let compression = reader.read_u8()?;
        let compression = CompressionType::try_from(compression)
            .map_err(|()| DeserializeError::InvalidTag(("CompressionType", compression)))?;

        let table_type = reader.read_u8()?;
        let table_type = TableType::try_from(table_type)
            .map_err(|()| DeserializeError::InvalidTag(("TableType", table_type)))?;

        let block_size = reader.read_u32::<BigEndian>()?;

        let level_count = reader.read_u8()?;

        Ok(Self {
            r#type: tree_type,
            compression,
            table_type,
            block_size,
            level_count,
        })
    }
}

// TODO: breaking, prefix all config setters with set_?

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

    /// Descriptor table to use
    #[doc(hidden)]
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate).
    ///
    /// This is the exponential growth of the from one
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1)
    #[allow(clippy::doc_markdown)]
    #[doc(hidden)]
    pub level_ratio: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: absolute_path(DEFAULT_FILE_FOLDER),
            block_cache: Arc::new(BlockCache::with_capacity_bytes(8 * 1_024 * 1_024)),
            descriptor_table: Arc::new(FileDescriptorTable::new(128, 2)),
            inner: PersistedConfig::default(),
            level_ratio: 8,
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

    /// Sets the size ratio between levels of the LSM tree (a.k.a. fanout, growth rate).
    ///
    /// Defaults to 8.
    ///
    /// # Panics
    ///
    /// Panics if `n` is less than 2.
    #[must_use]
    pub fn level_ratio(mut self, n: u8) -> Self {
        assert!(n > 1);

        self.level_ratio = n;
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
    /// Panics if the block size is smaller than 1 KiB (1024 bytes).
    #[must_use]
    pub fn block_size(mut self, block_size: u32) -> Self {
        assert!(block_size >= 1_024);

        self.inner.block_size = block_size;
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
            compression: CompressionType::Lz4,
            table_type: TableType::Block,
            block_size: 4_096,
            level_count: 7,
        };

        let mut bytes = vec![];
        config.serialize(&mut bytes)?;

        #[rustfmt::skip]
        let raw = &[
            // Magic
            b'F', b'J', b'L', b'L', b'C', b'F', b'G', b'1',

            // Tree type
            0,

            // Compression
            1,

            // Table type
            0,

            // Block size
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
            compression: CompressionType::Lz4,
            table_type: TableType::Block,
            block_size: 4_096,
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
