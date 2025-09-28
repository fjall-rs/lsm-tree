// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::offset::BlockOffset;
use super::Checksum;
use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use crate::file::MAGIC_BYTES;
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockType {
    Data,
    Index,
    Filter,
    Meta,
    Regions,
}

impl From<BlockType> for u8 {
    fn from(val: BlockType) -> Self {
        match val {
            BlockType::Data => 0,
            BlockType::Index => 1,
            BlockType::Filter => 2,
            BlockType::Meta => 3,
            BlockType::Regions => 4,
        }
    }
}

impl TryFrom<u8> for BlockType {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Index),
            2 => Ok(Self::Filter),
            3 => Ok(Self::Meta),
            4 => Ok(Self::Regions),
            _ => Err(DecodeError::InvalidTag(("BlockType", value))),
        }
    }
}

/// Header of a disk-based block
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Header {
    pub block_type: BlockType,

    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// File offset of previous block - only used for data blocks
    pub previous_block_offset: BlockOffset, // TODO: 3.0.0 remove?

    /// On-disk size of data segment
    pub data_length: u32,

    /// Uncompressed size of data segment
    pub uncompressed_length: u32,
}

impl Header {
    #[must_use]
    pub const fn serialized_len() -> usize {
        MAGIC_BYTES.len()
            // Block type
            + std::mem::size_of::<BlockType>()
            // Checksum
            + std::mem::size_of::<Checksum>()
            // Backlink
            + std::mem::size_of::<u64>()
            // On-disk size
            + std::mem::size_of::<u32>()
            // Uncompressed data length
            + std::mem::size_of::<u32>()
    }
}

impl Encode for Header {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        // Write block type
        writer.write_u8(self.block_type.into())?;

        // Write checksum
        writer.write_u128::<LittleEndian>(*self.checksum)?;

        // Write prev offset
        writer.write_u64::<LittleEndian>(*self.previous_block_offset)?;

        // Write on-disk size length
        writer.write_u32::<LittleEndian>(self.data_length)?;

        // Write uncompressed data length
        writer.write_u32::<LittleEndian>(self.uncompressed_length)?;

        Ok(())
    }
}

impl Decode for Header {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        // Check header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(DecodeError::InvalidHeader("Block"));
        }

        // Read block type
        let block_type = reader.read_u8()?;
        let block_type = BlockType::try_from(block_type)?;

        // Read checksum
        let checksum = reader.read_u128::<LittleEndian>()?;

        // Read prev offset
        let previous_block_offset = reader.read_u64::<LittleEndian>()?;

        // Read data length
        let data_length = reader.read_u32::<LittleEndian>()?;

        // Read data length
        let uncompressed_length = reader.read_u32::<LittleEndian>()?;

        Ok(Self {
            block_type,
            checksum: Checksum::from_raw(checksum),
            previous_block_offset: BlockOffset(previous_block_offset),
            data_length,
            uncompressed_length,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn v3_block_header_serde_roundtrip() -> crate::Result<()> {
        let header = Header {
            block_type: BlockType::Data,
            checksum: Checksum::from_raw(5),
            data_length: 252_356,
            previous_block_offset: BlockOffset(35),
            uncompressed_length: 124_124_124,
        };

        let bytes = header.encode_into_vec();

        assert_eq!(bytes.len(), Header::serialized_len());
        assert_eq!(header, Header::decode_from(&mut &bytes[..])?);

        Ok(())
    }
}
