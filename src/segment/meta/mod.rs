// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod compression;
mod table_type;

use super::writer::Writer;
use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
    time::unix_timestamp,
    value::SeqNo,
    KeyRange,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::{Cursor, Read, Write},
    path::Path,
};
pub use {compression::CompressionType, table_type::TableType};

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

    /// Data block size (uncompressed)
    pub data_block_size: u32,

    /// Index block size (uncompressed)
    pub index_block_size: u32,

    /// Number of written data blocks
    pub data_block_count: u32,

    /// Number of written index blocks
    pub index_block_count: u32,

    /// What type of compression is used
    pub compression: CompressionType,

    /// Type of table (unused)
    pub(crate) table_type: TableType,

    /// Sequence number range
    pub seqnos: (SeqNo, SeqNo),

    /// Key range
    pub key_range: KeyRange,
}

impl Encode for Metadata {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        writer.write_u64::<BigEndian>(self.id)?;

        writer.write_u128::<BigEndian>(self.created_at)?;

        writer.write_u64::<BigEndian>(self.item_count)?;
        writer.write_u64::<BigEndian>(self.key_count)?;
        writer.write_u64::<BigEndian>(self.tombstone_count)?;
        writer.write_u64::<BigEndian>(self.range_tombstone_count)?;

        writer.write_u64::<BigEndian>(self.file_size)?;
        writer.write_u64::<BigEndian>(self.uncompressed_size)?;

        writer.write_u32::<BigEndian>(self.data_block_size)?;
        writer.write_u32::<BigEndian>(self.index_block_size)?;

        writer.write_u32::<BigEndian>(self.data_block_count)?;
        writer.write_u32::<BigEndian>(self.index_block_count)?;

        self.compression.encode_into(writer)?;

        writer.write_u8(self.table_type.into())?;

        writer.write_u64::<BigEndian>(self.seqnos.0)?;
        writer.write_u64::<BigEndian>(self.seqnos.1)?;

        self.key_range.encode_into(writer)?;

        Ok(())
    }
}

impl Decode for Metadata {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(DecodeError::InvalidHeader("SegmentMetadata"));
        }

        let id = reader.read_u64::<BigEndian>()?;

        let created_at = reader.read_u128::<BigEndian>()?;

        let item_count = reader.read_u64::<BigEndian>()?;
        let key_count = reader.read_u64::<BigEndian>()?;
        let tombstone_count = reader.read_u64::<BigEndian>()?;
        let range_tombstone_count = reader.read_u64::<BigEndian>()?;

        let file_size = reader.read_u64::<BigEndian>()?;
        let uncompressed_size = reader.read_u64::<BigEndian>()?;

        let data_block_size = reader.read_u32::<BigEndian>()?;
        let index_block_size = reader.read_u32::<BigEndian>()?;

        let data_block_count = reader.read_u32::<BigEndian>()?;
        let index_block_count = reader.read_u32::<BigEndian>()?;

        let compression = CompressionType::decode_from(reader)?;

        let table_type = reader.read_u8()?;
        let table_type = TableType::try_from(table_type)
            .map_err(|()| DecodeError::InvalidTag(("TableType", table_type)))?;

        let seqno_min = reader.read_u64::<BigEndian>()?;
        let seqno_max = reader.read_u64::<BigEndian>()?;

        let key_range = KeyRange::decode_from(reader)?;

        Ok(Self {
            id,
            created_at,

            item_count,
            key_count,
            tombstone_count,
            range_tombstone_count,

            file_size,
            uncompressed_size,

            data_block_size,
            index_block_size,

            data_block_count,
            index_block_count,

            compression,
            table_type,

            seqnos: (seqno_min, seqno_max),

            key_range,
        })
    }
}

impl Metadata {
    /// Consumes a writer and its metadata to create the segment metadata.
    ///
    /// The writer should not be empty.
    pub fn from_writer(id: SegmentId, writer: &Writer) -> crate::Result<Self> {
        Ok(Self {
            id,

            // NOTE: Using seconds is not granular enough
            // But because millis already returns u128, might as well use micros :)
            created_at: unix_timestamp().as_micros(),

            compression: CompressionType::None,
            table_type: TableType::Block,

            // NOTE: Truncation is OK - even with the smallest block size (1 KiB), 4 billion blocks would be 4 TB
            #[allow(clippy::cast_possible_truncation)]
            data_block_count: writer.meta.data_block_count as u32,

            // NOTE: Truncation is OK as well
            #[allow(clippy::cast_possible_truncation)]
            index_block_count: writer.meta.index_block_count as u32,

            data_block_size: writer.opts.data_block_size,
            index_block_size: writer.opts.index_block_size,

            file_size: *writer.meta.file_pos,
            uncompressed_size: writer.meta.uncompressed_size,
            item_count: writer.meta.item_count as u64,
            key_count: writer.meta.key_count as u64,

            // NOTE: from_writer should not be called when the writer wrote nothing
            #[allow(clippy::expect_used)]
            key_range: KeyRange::new((
                writer
                    .meta
                    .first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                writer
                    .meta
                    .last_key
                    .clone()
                    .expect("should have written at least 1 item"),
            )),

            seqnos: (writer.meta.lowest_seqno, writer.meta.highest_seqno),

            tombstone_count: writer.meta.tombstone_count as u64,

            // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2
            range_tombstone_count: 0,
        })
    }

    /// Reads and parses a Segment metadata file
    pub fn from_disk(path: &Path) -> crate::Result<Self> {
        let file_content = std::fs::read(path)?;
        let mut cursor = Cursor::new(file_content);
        let meta = Self::decode_from(&mut cursor)?;
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
            data_block_count: 0,
            index_block_count: 0,
            data_block_size: 4_096,
            index_block_size: 4_096,
            created_at: 5,
            id: 632_632,
            file_size: 1,
            compression: CompressionType::None,
            table_type: TableType::Block,
            item_count: 0,
            key_count: 0,
            key_range: KeyRange::new((vec![2].into(), vec![5].into())),
            tombstone_count: 0,
            range_tombstone_count: 0,
            uncompressed_size: 0,
            seqnos: (0, 5),
        };

        let bytes = metadata.encode_into_vec();
        let mut cursor = Cursor::new(bytes);
        let metadata_copy = Metadata::decode_from(&mut cursor)?;

        assert_eq!(metadata, metadata_copy);

        Ok(())
    }
}
