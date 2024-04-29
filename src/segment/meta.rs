use super::writer::Writer;
use crate::{
    file::{fsync_directory, SEGMENT_METADATA_FILE},
    key_range::KeyRange,
    time::unix_timestamp,
    value::SeqNo,
    version::Version,
};
use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, io::Write, path::Path, sync::Arc};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    Lz4,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lz4")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    pub version: Version,

    /// Segment ID
    pub id: Arc<str>,

    /// Creation time as unix timestamp (in µs)
    pub created_at: u128,

    /// Number of items in the segment
    ///
    /// This may include tombstones and multiple versions of the same key
    pub item_count: u64,

    /// Number of unique keys in the segment
    ///
    /// This may include tombstones
    pub key_count: u64,

    /// Block size (uncompressed)
    pub block_size: u32,

    /// Number of written blocks
    pub block_count: u32,

    /// What type of compression is used
    pub compression: CompressionType,

    /// compressed size in bytes (on disk)
    pub file_size: u64,

    /// true size in bytes (if no compression were used)
    pub uncompressed_size: u64,

    /// Key range
    pub key_range: KeyRange,

    /// Sequence number range
    pub seqnos: (SeqNo, SeqNo),

    /// Number of tombstones
    pub tombstone_count: u64,
}

impl Metadata {
    /// Consumes a writer and its metadata to create the segment metadata
    pub fn from_writer(id: Arc<str>, writer: Writer) -> crate::Result<Self> {
        Ok(Self {
            id,
            version: Version::V0,
            block_count: writer.block_count as u32,
            block_size: writer.opts.block_size,

            // NOTE: Using seconds is not granular enough
            // But because millis already returns u128, might as well use micros :)
            created_at: unix_timestamp().as_micros(),

            file_size: writer.file_pos,
            compression: CompressionType::Lz4,
            item_count: writer.item_count as u64,
            key_count: writer.key_count as u64,

            key_range: KeyRange::new((
                writer
                    .first_key
                    .expect("should have written at least 1 item"),
                writer
                    .last_key
                    .expect("should have written at least 1 item"),
            )),
            seqnos: (writer.lowest_seqno, writer.highest_seqno),
            tombstone_count: writer.tombstone_count as u64,
            uncompressed_size: writer.uncompressed_size,
        })
    }

    /// Stores segment metadata at a folder
    ///
    /// Will be stored as JSON
    pub fn write_to_file<P: AsRef<Path>>(&self, folder_path: P) -> std::io::Result<()> {
        let mut writer = OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(folder_path.as_ref().join(SEGMENT_METADATA_FILE))?;

        writer.write_all(
            serde_json::to_string_pretty(self)
                .expect("Failed to serialize to JSON")
                .as_bytes(),
        )?;
        writer.flush()?;
        writer.sync_all()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(&folder_path)?;

        Ok(())
    }

    /// Reads and parses a Segment metadata file
    pub fn from_disk<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file_content = std::fs::read_to_string(path)?;
        let item = serde_json::from_str(&file_content)?;
        Ok(item)
    }
}
