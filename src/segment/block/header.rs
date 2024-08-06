use crate::{
    segment::meta::CompressionType,
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub const BLOCK_HEADER_MAGIC: &[u8] = &[b'L', b'S', b'M', b'T', b'B', b'L', b'K', b'2'];

type Checksum = u64;

/// Header of a disk-based block
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    /// Compression type used
    pub compression: CompressionType,

    /// Checksum value to verify integrity of data
    pub checksum: Checksum,

    /// File offset of previous block - only used for data blocks
    pub previous_block_offset: u64,

    /// Compressed size of data segment
    pub data_length: u32,

    /// Uncompressed size of data segment
    pub uncompressed_length: u32,
}

impl Header {
    #[must_use]
    pub const fn serialized_len() -> usize {
        BLOCK_HEADER_MAGIC.len()
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

impl Serializable for Header {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(BLOCK_HEADER_MAGIC)?;

        self.compression.serialize(writer)?;

        // Write checksum
        writer.write_u64::<BigEndian>(self.checksum)?;

        // Write prev offset
        writer.write_u64::<BigEndian>(self.previous_block_offset)?;

        // Write data length
        writer.write_u32::<BigEndian>(self.data_length)?;

        // Write uncompressed data length
        writer.write_u32::<BigEndian>(self.uncompressed_length)?;

        Ok(())
    }
}

impl Deserializable for Header {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Check header
        let mut magic = [0u8; BLOCK_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != BLOCK_HEADER_MAGIC {
            return Err(DeserializeError::InvalidHeader("Block"));
        }

        let compression = CompressionType::deserialize(reader)?;

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
            checksum,
            previous_block_offset,
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
            checksum: 4,
            previous_block_offset: 2,
            data_length: 15,
            uncompressed_length: 15,
        };

        #[rustfmt::skip]
        let bytes = &[
            // Header
            b'L', b'S', b'M', b'T', b'B', b'L', b'K', b'2',
            
            // Compression
            0, 0,
            
            // Checksum
            0, 0, 0, 0, 0, 0, 0, 4,

            0, 0, 0, 0, 0, 0, 0, 2, 
            
            // Data length
            0, 0, 0, 0x0F,

            // Uncompressed length
            0, 0, 0, 0x0F,
        ];

        // Deserialize the empty Value
        let deserialized = Header::deserialize(&mut Cursor::new(bytes))?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(header, deserialized);

        assert_eq!(Header::serialized_len(), bytes.len());

        Ok(())
    }
}
