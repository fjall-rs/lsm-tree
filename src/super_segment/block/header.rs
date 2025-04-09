// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Encode, EncodeError,Decode,DecodeError};
use crate::{file::MAGIC_BYTES, segment::block::offset::BlockOffset, Checksum};
use byteorder::LittleEndian;
use byteorder::{ReadBytesExt,WriteBytesExt};
use std::io::{Read, Write};

/// Header of a disk-based block
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Header {
    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// File offset of previous block - only used for data blocks
    pub previous_block_offset: BlockOffset,

    /// On-disk size of data segment
    pub data_length: u32,

    /// Uncompressed size of data segment
    pub uncompressed_length: u32,
}

impl Header {
    #[must_use]
    pub const fn serialized_len() -> usize {
        MAGIC_BYTES.len()
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

        // Write checksum
        writer.write_u64::<LittleEndian>(*self.checksum)?;

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


        // Read checksum
        let checksum = reader.read_u64::<LittleEndian>()?;

        // Read prev offset
        let previous_block_offset = reader.read_u64::<LittleEndian>()?;

        // Read data length
        let data_length = reader.read_u32::<LittleEndian>()?;

        // Read data length
        let uncompressed_length = reader.read_u32::<LittleEndian>()?;

        Ok(Self {
            checksum: Checksum::from_raw(checksum),
            previous_block_offset: BlockOffset(previous_block_offset),
            data_length,
            uncompressed_length,
        })
    }
}