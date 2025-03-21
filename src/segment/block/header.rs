// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{checksum::Checksum, offset::BlockOffset};
use crate::{
    file::MAGIC_BYTES, segment::meta::CompressionType, Decode, DecodeError, Encode, EncodeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Header of a disk-based block
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    /// Compression type used
    pub compression: CompressionType,

    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// File offset of previous block - only used for data blocks
    pub previous_block_offset: BlockOffset,

    /// Compressed size of data segment
    pub data_length: u32,

    /// Uncompressed size of data segment
    pub uncompressed_length: u32,
}

impl Header {
    #[must_use]
    pub const fn serialized_len() -> usize {
        MAGIC_BYTES.len()
            // NOTE: Compression is 2 bytes
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u8>()
            // Checksum
            + std::mem::size_of::<Checksum>()
            // Backlink
            + std::mem::size_of::<u64>()
            // Data length
            + std::mem::size_of::<u32>()
            // Uncompressed data length
            + std::mem::size_of::<u32>()
    }
}

impl Encode for Header {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(&MAGIC_BYTES)?;

        self.compression.encode_into(writer)?;

        // Write checksum
        writer.write_u64::<BigEndian>(*self.checksum)?;

        // Write prev offset
        writer.write_u64::<BigEndian>(*self.previous_block_offset)?;

        // Write data length
        writer.write_u32::<BigEndian>(self.data_length)?;

        // Write uncompressed data length
        writer.write_u32::<BigEndian>(self.uncompressed_length)?;

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

        let compression = CompressionType::decode_from(reader)?;

        // Read checksum
        let checksum = reader.read_u64::<BigEndian>()?;

        // Read prev offset
        let previous_block_offset = reader.read_u64::<BigEndian>()?;

        // Read data length
        let data_length = reader.read_u32::<BigEndian>()?;

        // Read data length
        let uncompressed_length = reader.read_u32::<BigEndian>()?;

        Ok(Self {
            compression,
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
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn block_header_raw() -> crate::Result<()> {
        let header = Header {
            compression: CompressionType::None,
            checksum: Checksum::from_raw(4),
            previous_block_offset: BlockOffset(2),
            data_length: 15,
            uncompressed_length: 15,
        };

        #[rustfmt::skip]
        let bytes = &[
            // Header
            b'L', b'S', b'M', 2,
            
            // Compression
            0, 0,
            
            // Checksum
            0, 0, 0, 0, 0, 0, 0, 4,

            // Backlink
            0, 0, 0, 0, 0, 0, 0, 2, 
            
            // Data length
            0, 0, 0, 0x0F,

            // Uncompressed length
            0, 0, 0, 0x0F,
        ];

        // Deserialize the empty Value
        let deserialized = Header::decode_from(&mut Cursor::new(bytes))?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(header, deserialized);

        assert_eq!(Header::serialized_len(), bytes.len());

        Ok(())
    }
}
