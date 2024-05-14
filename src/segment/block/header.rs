use crate::{
    segment::meta::CompressionType,
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub const BLOCK_HEADER_MAGIC: &[u8] = &[b'L', b'S', b'M', b'T', b'B', b'L', b'K', b'1'];

/// Header of a disk-based block
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Header {
    /// Compression type used
    pub compression: CompressionType,

    /// CRC value to verify integrity of data
    pub crc: u32,

    /// Compressed size of data segment
    pub data_length: u32,
}

impl Header {
    #[must_use]
    pub const fn serialized_len() -> usize {
        BLOCK_HEADER_MAGIC.len()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u32>()
            + std::mem::size_of::<u32>()
    }
}

impl Serializable for Header {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write header
        writer.write_all(BLOCK_HEADER_MAGIC)?;

        // Write compression type
        writer.write_u8(self.compression.into())?;

        // Write CRC
        writer.write_u32::<BigEndian>(self.crc)?;

        // Write data length
        writer.write_u32::<BigEndian>(self.data_length)?;

        Ok(())
    }
}

impl Deserializable for Header {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Check header
        let mut magic = [0u8; BLOCK_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != BLOCK_HEADER_MAGIC {
            return Err(DeserializeError::InvalidBlockHeader);
        }

        // Read compression type
        let compression = reader.read_u8()?;
        let compression = CompressionType::try_from(compression).expect("invalid compression type");

        // Read CRC
        let crc = reader.read_u32::<BigEndian>()?;

        // Read data length
        let data_length = reader.read_u32::<BigEndian>()?;

        Ok(Self {
            compression,
            crc,
            data_length,
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
            compression: CompressionType::Lz4,
            crc: 4,
            data_length: 15,
        };

        #[rustfmt::skip]
        let bytes = &[
            // Header
            b'L', b'S', b'M', b'T', b'B', b'L', b'K', b'1',
            
            // Compression
            1,
            
            // CRC
            0, 0, 0, 4,
            
            // Data length
            0, 0, 0, 0x0F,
        ];

        // Deserialize the empty Value
        let deserialized = Header::deserialize(&mut Cursor::new(bytes))?;

        // Check if deserialized Value is equivalent to the original empty Value
        assert_eq!(header, deserialized);

        Ok(())
    }
}
