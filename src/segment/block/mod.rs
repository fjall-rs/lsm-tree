pub mod header;

use crate::serde::{Deserializable, DeserializeError, Serializable, SerializeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use header::Header as BlockHeader;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::io::{Cursor, Read, Write};

/// A disk-based block
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
#[derive(Clone, Debug)]
pub struct Block<T: Clone + Serializable + Deserializable> {
    pub header: BlockHeader,
    pub items: Box<[T]>,
}

impl<T: Clone + Serializable + Deserializable> Block<T> {
    pub fn from_reader_compressed<R: Read>(reader: &mut R, size: u32) -> crate::Result<Self> {
        let mut bytes = vec![0u8; size as usize];
        reader.read_exact(&mut bytes)?;

        let bytes = decompress_size_prepended(&bytes)?;
        let mut bytes = Cursor::new(bytes);

        let block = Self::deserialize(&mut bytes)?;

        Ok(block)
    }

    pub fn from_file_compressed<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        offset: u64,
        size: u32,
    ) -> crate::Result<Self> {
        reader.seek(std::io::SeekFrom::Start(offset))?;
        Self::from_reader_compressed(reader, size)
    }

    /// Calculates the CRC from a list of values
    pub fn create_crc(items: &[T]) -> crate::Result<u32> {
        let mut hasher = crc32fast::Hasher::new();

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        hasher.update(&(items.len() as u32).to_be_bytes());

        for value in items {
            let mut serialized_value = Vec::new();
            value.serialize(&mut serialized_value)?;

            hasher.update(&serialized_value);
        }

        Ok(hasher.finalize())
    }

    #[allow(unused)]
    pub(crate) fn check_crc(&self, expected_crc: u32) -> crate::Result<bool> {
        let crc = Self::create_crc(&self.items)?;
        Ok(crc == expected_crc)
    }

    #[must_use]
    pub fn to_bytes_compressed(block: &Self) -> Vec<u8> {
        // Serialize block
        let mut bytes = Vec::with_capacity(u16::MAX.into());
        block.serialize(&mut bytes).expect("should serialize block");

        // Compress using LZ4
        compress_prepend_size(&bytes)
    }
}

impl<T: Clone + Serializable + Deserializable> Serializable for Block<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        // Write block header
        BlockHeader {
            crc: self.header.crc,
            compression: super::meta::CompressionType::Lz4,
            data_length: 0,
        }
        .serialize(writer)?;

        // Write number of items

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u32::<BigEndian>(self.items.len() as u32)?;

        // Serialize each value
        for value in self.items.iter() {
            value.serialize(writer)?;
        }

        Ok(())
    }
}

impl<T: Clone + Serializable + Deserializable> Deserializable for Block<T> {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        // Read block header
        let header = BlockHeader::deserialize(reader)?;

        // Read number of items
        let item_count = reader.read_u32::<BigEndian>()? as usize;

        // Deserialize each value
        let mut items = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            items.push(T::deserialize(reader)?);
        }

        Ok(Self {
            header,
            items: items.into_boxed_slice(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{value::ValueType, Value};
    use test_log::test;

    #[test]
    fn disk_block_deserialization_success() -> crate::Result<()> {
        let item1 = Value::new(vec![1, 2, 3], vec![4, 5, 6], 42, ValueType::Value);
        let item2 = Value::new(vec![7, 8, 9], vec![10, 11, 12], 43, ValueType::Value);

        let items = vec![item1.clone(), item2.clone()];
        let crc = Block::create_crc(&items)?;

        let block = Block {
            items: items.into_boxed_slice(),
            header: BlockHeader {
                crc,
                compression: crate::segment::meta::CompressionType::Lz4,
                data_length: 0,
            },
        };

        // Serialize to bytes
        let mut serialized = Vec::new();
        block.serialize(&mut serialized)?;

        // Deserialize from bytes
        let deserialized = Block::deserialize(&mut &serialized[..]);

        match deserialized {
            Ok(block) => {
                assert_eq!(2, block.items.len());
                assert_eq!(block.items.first().cloned(), Some(item1));
                assert_eq!(block.items.get(1).cloned(), Some(item2));
                assert_eq!(crc, block.header.crc);
            }
            Err(error) => panic!("Deserialization failed: {error:?}"),
        }

        Ok(())
    }

    #[test]
    fn disk_block_deserialization_failure_crc() -> crate::Result<()> {
        let item1 = Value::new(vec![1, 2, 3], vec![4, 5, 6], 42, ValueType::Value);
        let item2 = Value::new(vec![7, 8, 9], vec![10, 11, 12], 43, ValueType::Value);

        let block = Block {
            items: [item1, item2].into(),
            header: BlockHeader {
                crc: 12345,
                compression: crate::segment::meta::CompressionType::Lz4,
                data_length: 0,
            },
        };

        // Serialize to bytes
        let mut serialized = Vec::new();
        block.serialize(&mut serialized)?;

        // Deserialize from bytes
        let deserialized = Block::<Value>::deserialize(&mut &serialized[..])?;

        assert!(!deserialized.check_crc(54321)?);

        Ok(())
    }
}
