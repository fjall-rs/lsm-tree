mod compression;
mod table_type;

use super::writer::Writer;
use crate::{
    key_range::KeyRange,
    serde::{Deserializable, Serializable},
    time::unix_timestamp,
    value::SeqNo,
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Cursor, Read, Write},
    path::Path,
};
pub use {compression::CompressionType, table_type::TableType};

pub const METADATA_HEADER_MAGIC: &[u8] = &[b'F', b'J', b'L', b'L', b'S', b'M', b'D', b'1'];

pub type SegmentId = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Metadata {
    /// Segment ID
    pub id: SegmentId,

    /// Creation time as unix timestamp (in µs)
    pub created_at: u128,

    /// Number of KV-pairs in the segment
    ///
    /// This may include tombstones and multiple versions of the same key
    pub item_count: u64,

    /// Number of unique keys in the segment
    ///
    /// This may include tombstones
    pub key_count: u64,

    /// Number of tombstones
    pub tombstone_count: u64,

    /// Number of range tombstones
    pub(crate) range_tombstone_count: u64,

    /// compressed size in bytes (on disk) (without the fixed size trailer)
    pub file_size: u64,

    /// true size in bytes (if no compression were used)
    pub uncompressed_size: u64,

    /// Block size (uncompressed)
    pub block_size: u32,

    /// Number of written blocks
    pub block_count: u32,

    /// What type of compression is used
    pub compression: CompressionType,

    /// Type of table (unused)
    pub(crate) table_type: TableType,

    /// Sequence number range
    pub seqnos: (SeqNo, SeqNo),

    /// Key range
    pub key_range: KeyRange,
}

impl Serializable for Metadata {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(METADATA_HEADER_MAGIC)?;

        writer.write_u64::<BigEndian>(self.id)?;

        writer.write_u128::<BigEndian>(self.created_at)?;

        writer.write_u64::<BigEndian>(self.item_count)?;
        writer.write_u64::<BigEndian>(self.key_count)?;
        writer.write_u64::<BigEndian>(self.tombstone_count)?;
        writer.write_u64::<BigEndian>(self.range_tombstone_count)?;

        writer.write_u64::<BigEndian>(self.file_size)?;
        writer.write_u64::<BigEndian>(self.uncompressed_size)?;

        writer.write_u32::<BigEndian>(self.block_size)?;
        writer.write_u32::<BigEndian>(self.block_count)?;

        writer.write_u8(self.compression.into())?;
        writer.write_u8(self.table_type.into())?;

        writer.write_u64::<BigEndian>(self.seqnos.0)?;
        writer.write_u64::<BigEndian>(self.seqnos.1)?;

        self.key_range.serialize(writer)?;

        Ok(())
    }
}

impl Deserializable for Metadata {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Check header
        let mut magic = [0u8; METADATA_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != METADATA_HEADER_MAGIC {
            return Err(DeserializeError::InvalidHeader("SegmentMetadata"));
        }

        let id = reader.read_u64::<BigEndian>()?;

        let created_at = reader.read_u128::<BigEndian>()?;

        let item_count = reader.read_u64::<BigEndian>()?;
        let key_count = reader.read_u64::<BigEndian>()?;
        let tombstone_count = reader.read_u64::<BigEndian>()?;
        let range_tombstone_count = reader.read_u64::<BigEndian>()?;

        let file_size = reader.read_u64::<BigEndian>()?;
        let uncompressed_size = reader.read_u64::<BigEndian>()?;

        let block_size = reader.read_u32::<BigEndian>()?;
        let block_count = reader.read_u32::<BigEndian>()?;

        let compression = reader.read_u8()?;
        let compression = CompressionType::try_from(compression)
            .map_err(|()| DeserializeError::InvalidTag(("CompressionType", compression)))?;

        let table_type = reader.read_u8()?;
        let table_type = TableType::try_from(table_type)
            .map_err(|()| DeserializeError::InvalidTag(("TableType", table_type)))?;

        let seqno_min = reader.read_u64::<BigEndian>()?;
        let seqno_max = reader.read_u64::<BigEndian>()?;

        let key_range = KeyRange::deserialize(reader)?;

        Ok(Self {
            id,
            created_at,

            item_count,
            key_count,
            tombstone_count,
            range_tombstone_count,

            file_size,
            uncompressed_size,

            block_size,
            block_count,

            compression,
            table_type,

            seqnos: (seqno_min, seqno_max),

            key_range,
        })
    }
}

impl Metadata {
    /// Consumes a writer and its metadata to create the segment metadata
    pub fn from_writer(id: SegmentId, writer: &Writer) -> crate::Result<Self> {
        Ok(Self {
            id,
            block_count: writer.block_count as u32,
            block_size: writer.opts.block_size,

            // NOTE: Using seconds is not granular enough
            // But because millis already returns u128, might as well use micros :)
            created_at: unix_timestamp().as_micros(),

            file_size: writer.file_pos,
            compression: CompressionType::Lz4,
            table_type: TableType::Block,
            item_count: writer.item_count as u64,
            key_count: writer.key_count as u64,

            key_range: KeyRange::new((
                writer
                    .first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                writer
                    .last_key
                    .clone()
                    .expect("should have written at least 1 item"),
            )),

            seqnos: (writer.lowest_seqno, writer.highest_seqno),
            tombstone_count: writer.tombstone_count as u64,
            range_tombstone_count: 0, // TODO:
            uncompressed_size: writer.uncompressed_size,
        })
    }

    /// Reads and parses a Segment metadata file
    pub fn from_disk<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let file_content = std::fs::read(path)?;
        let mut cursor = Cursor::new(file_content);
        let meta = Self::deserialize(&mut cursor)?;
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn segment_metadata_serde_round_trip() -> crate::Result<()> {
        let metadata = Metadata {
            block_count: 0,
            block_size: 0,
            created_at: 5,
            id: 632_632,
            file_size: 1,
            compression: CompressionType::Lz4,
            table_type: TableType::Block,
            item_count: 0,
            key_count: 0,
            key_range: KeyRange::new((vec![2].into(), vec![5].into())),
            tombstone_count: 0,
            range_tombstone_count: 0,
            uncompressed_size: 0,
            seqnos: (0, 5),
        };

        let mut bytes = vec![];
        metadata.serialize(&mut bytes)?;

        let mut cursor = Cursor::new(bytes);
        let metadata_copy = Metadata::deserialize(&mut cursor)?;

        assert_eq!(metadata, metadata_copy);

        Ok(())
    }
}
