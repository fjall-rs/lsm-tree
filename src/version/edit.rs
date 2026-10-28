// TODO: v4
#![warn(missing_docs)]

// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::raw::{BlobFileRef, TableRef};
use crate::{
    blob_tree::FragmentationMap,
    coding::{Decode, Encode},
    vlog::BlobFileId,
    Checksum, SeqNo, TableId,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use varint_rs::{VarintReader, VarintWriter};

/// TODO: WIP
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub enum EditRecord {
    /// Some tables were added to a level
    AddTables {
        /// Destination level.
        level: usize,

        /// Tables added as a new run.
        tables: Vec<TableRef>,
    },

    /// Some blob files were created
    AddBlobFiles {
        /// Blob files added to the value log.
        blob_files: Vec<BlobFileRef>,
    },

    // TODO: add lvl param
    /// Some tables were dropped
    DropTables(Vec<TableId>),

    /// Some tables were moved
    MoveTables {
        /// Destination level.
        dst_lvl: usize,

        /// Tables moved to the destination level.
        tables: Vec<TableId>,
    },

    /// Some blob files were dropped
    DropBlobFiles(Vec<BlobFileId>),

    /// The version seqno was set
    SetVersionSeqno(SeqNo),

    /// Merges a fragmentation-map diff into the current map.
    UpdateFragMap(FragmentationMap),

    // TODO: remove?
    /// An application-defined edit payload.
    Custom(Vec<u8>),
}

impl Encode for EditRecord {
    fn encode_into<W: std::io::prelude::Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        match self {
            Self::Custom(bytes) => {
                writer.write_u8(0xFF)?; // enum discriminant
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "version edit payloads are bounded by practical limits"
                )]
                writer.write_u32_varint(bytes.len() as u32)?;
                writer.write_all(bytes)?;
            }
            Self::AddTables { level, tables } => {
                writer.write_u8(0x0)?; // enum discriminant

                #[expect(clippy::cast_possible_truncation, reason = "level count fits in u32")]
                writer.write_u32_varint(*level as u32)?;

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion tables per edit"
                )]
                writer.write_u32_varint(tables.len() as u32)?;

                for table in tables {
                    writer.write_u64_varint(table.id)?;
                    writer.write_u128::<LittleEndian>(table.checksum.into_u128())?;
                }
            }
            Self::AddBlobFiles { blob_files } => {
                writer.write_u8(0x1)?; // enum discriminant

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion blob files per edit"
                )]
                writer.write_u32_varint(blob_files.len() as u32)?;

                for blob_file in blob_files {
                    writer.write_u64_varint(blob_file.id)?;
                    writer.write_u128::<LittleEndian>(blob_file.checksum.into_u128())?;
                }
            }
            Self::DropTables(tables) => {
                writer.write_u8(0x2)?; // enum discriminant

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion tables per edit"
                )]
                writer.write_u32_varint(tables.len() as u32)?;

                for id in tables {
                    writer.write_u64_varint(*id)?;
                }
            }
            Self::MoveTables { dst_lvl, tables } => {
                writer.write_u8(0x3)?; // enum discriminant

                #[expect(clippy::cast_possible_truncation, reason = "level count fits in u32")]
                writer.write_u32_varint(*dst_lvl as u32)?;

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion tables per edit"
                )]
                writer.write_u32_varint(tables.len() as u32)?;

                for id in tables {
                    writer.write_u64_varint(*id)?;
                }
            }
            Self::DropBlobFiles(ids) => {
                writer.write_u8(0x4)?; // enum discriminant

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "there are always less than 4 billion blob files per edit"
                )]
                writer.write_u32_varint(ids.len() as u32)?;

                for id in ids {
                    writer.write_u64_varint(*id)?;
                }
            }
            Self::SetVersionSeqno(seqno) => {
                writer.write_u8(0x5)?; // enum discriminant
                writer.write_u64_varint(*seqno)?;
            }
            Self::UpdateFragMap(map) => {
                writer.write_u8(0x6)?; // enum discriminant
                map.encode_into(writer)?;
            }
        }

        Ok(())
    }
}

impl Decode for EditRecord {
    fn decode_from<R: std::io::prelude::Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        let tag = reader.read_u8()?;
        Ok(match tag {
            0xFF => {
                let len = reader.read_u32_varint()? as usize;

                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;

                Self::Custom(buf)
            }
            0x00 => {
                let level = reader.read_u32_varint()? as usize;
                let cnt = reader.read_u32_varint()? as usize;

                let mut tables = Vec::with_capacity(cnt);

                for _ in 0..cnt {
                    let id = reader.read_u64_varint()?;
                    let checksum = Checksum::from_raw(reader.read_u128::<LittleEndian>()?);
                    tables.push(TableRef { id, checksum });
                }

                Self::AddTables { level, tables }
            }
            0x01 => {
                let cnt = reader.read_u32_varint()? as usize;

                let mut blob_files = Vec::with_capacity(cnt);

                for _ in 0..cnt {
                    let id = reader.read_u64_varint()?;
                    let checksum = Checksum::from_raw(reader.read_u128::<LittleEndian>()?);
                    blob_files.push(BlobFileRef { id, checksum });
                }

                Self::AddBlobFiles { blob_files }
            }
            0x02 => {
                let cnt = reader.read_u32_varint()? as usize;

                let mut tables = Vec::with_capacity(cnt);

                for _ in 0..cnt {
                    tables.push(reader.read_u64_varint()?);
                }

                Self::DropTables(tables)
            }
            0x03 => {
                let dst_lvl = reader.read_u32_varint()? as usize;
                let cnt = reader.read_u32_varint()? as usize;

                let mut tables = Vec::with_capacity(cnt);

                for _ in 0..cnt {
                    tables.push(reader.read_u64_varint()?);
                }

                Self::MoveTables { dst_lvl, tables }
            }
            0x04 => {
                let cnt = reader.read_u32_varint()? as usize;
                let mut ids = Vec::with_capacity(cnt);

                for _ in 0..cnt {
                    ids.push(reader.read_u64_varint()?);
                }

                Self::DropBlobFiles(ids)
            }
            0x05 => {
                let seqno = reader.read_u64_varint()?;
                Self::SetVersionSeqno(seqno)
            }
            0x06 => {
                let map = FragmentationMap::decode_from(reader)?;
                Self::UpdateFragMap(map)
            }
            other => return Err(crate::Error::InvalidTag(("EditRecord", other))),
        })
    }
}

#[derive(Debug, Eq, PartialEq, Default)]
#[cfg_attr(test, derive(serde::Serialize, serde::Deserialize))]
pub struct VersionEdit(Vec<EditRecord>);

impl Encode for VersionEdit {
    fn encode_into<W: std::io::prelude::Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        writer.write_u64_varint(self.0.len() as u64)?;

        for record in &self.0 {
            record.encode_into(writer)?;
        }

        Ok(())
    }
}

impl Decode for VersionEdit {
    fn decode_from<R: std::io::prelude::Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized,
    {
        let count = reader.read_u64_varint()? as usize;

        let mut recs = Vec::with_capacity(count);

        for _ in 0..count {
            recs.push(EditRecord::decode_from(reader)?);
        }

        Ok(Self(recs))
    }
}

impl VersionEdit {
    #[must_use]
    pub fn empty() -> Self {
        Self(Vec::default())
    }

    #[must_use]
    pub fn new(recs: Vec<EditRecord>) -> Self {
        Self(recs)
    }

    pub fn push(&mut self, record: EditRecord) {
        self.0.push(record);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &EditRecord> {
        self.0.iter()
    }
}
