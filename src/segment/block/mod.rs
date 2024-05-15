pub mod header;

use crate::serde::{Deserializable, Serializable};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use header::Header as BlockHeader;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use std::io::{Cursor, Read};

/// A disk-based block
///
/// A block is split into its header and a compressed blob of data.
///
/// \[ header \]
/// \[  data  \]
///
/// The integrity of a block can be checked using the CRC value that is saved in it.
#[derive(Clone, Debug)]
pub struct Block<T: Clone + Serializable + Deserializable> {
    pub header: BlockHeader,
    pub items: Box<[T]>,
}

impl<T: Clone + Serializable + Deserializable> Block<T> {
    pub fn from_reader_compressed<R: Read>(reader: &mut R) -> crate::Result<Self> {
        // Read block header
        let header = BlockHeader::deserialize(reader)?;

        let mut bytes = vec![0u8; header.data_length as usize];
        reader.read_exact(&mut bytes)?;

        let bytes = decompress_size_prepended(&bytes)?;
        let mut bytes = Cursor::new(bytes);

        // Read number of items
        let item_count = bytes.read_u32::<BigEndian>()? as usize;

        // Deserialize each value
        let mut items = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            items.push(T::deserialize(&mut bytes)?);
        }

        Ok(Self {
            header,
            items: items.into_boxed_slice(),
        })
    }

    pub fn from_file_compressed<R: std::io::Read + std::io::Seek>(
        reader: &mut R,
        offset: u64,
    ) -> crate::Result<Self> {
        reader.seek(std::io::SeekFrom::Start(offset))?;
        Self::from_reader_compressed(reader)
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

    pub fn to_bytes_compressed(items: &[T]) -> crate::Result<(BlockHeader, Vec<u8>)> {
        let packed = Self::pack_items(items)?;

        let header = BlockHeader {
            crc: Self::create_crc(items)?,
            compression: super::meta::CompressionType::Lz4,
            data_length: packed.len() as u32,
        };

        Ok((header, packed))
    }

    fn pack_items(items: &[T]) -> crate::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(u16::MAX.into());

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        buf.write_u32::<BigEndian>(items.len() as u32)?;

        // Serialize each value
        for value in items {
            value.serialize(&mut buf)?;
        }

        Ok(compress_prepend_size(&buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{segment::value_block::ValueBlock, value::ValueType, Value};
    use std::io::Write;
    use test_log::test;

    #[test]
    fn disk_block_deserialization_success() -> crate::Result<()> {
        let item1 = Value::new(vec![1, 2, 3], vec![4, 5, 6], 42, ValueType::Value);
        let item2 = Value::new(vec![7, 8, 9], vec![10, 11, 12], 43, ValueType::Value);

        let items = vec![item1.clone(), item2.clone()];
        let crc = Block::create_crc(&items)?;

        // Serialize to bytes
        let mut serialized = Vec::new();

        let (header, data) = ValueBlock::to_bytes_compressed(&items)?;

        header.serialize(&mut serialized)?;
        serialized.write_all(&data)?;

        assert_eq!(serialized.len(), BlockHeader::serialized_len() + data.len());

        // Deserialize from bytes
        let mut cursor = Cursor::new(serialized);
        let block = ValueBlock::from_reader_compressed(&mut cursor)?;

        assert_eq!(2, block.items.len());
        assert_eq!(block.items.first().cloned(), Some(item1));
        assert_eq!(block.items.get(1).cloned(), Some(item2));
        assert_eq!(crc, block.header.crc);

        Ok(())
    }

    #[test]
    fn disk_block_deserialization_failure_crc() -> crate::Result<()> {
        let item1 = Value::new(vec![1, 2, 3], vec![4, 5, 6], 42, ValueType::Value);
        let item2 = Value::new(vec![7, 8, 9], vec![10, 11, 12], 43, ValueType::Value);

        let items = vec![item1, item2];

        // Serialize to bytes
        let mut serialized = Vec::new();

        let (header, data) = ValueBlock::to_bytes_compressed(&items)?;

        header.serialize(&mut serialized)?;
        serialized.write_all(&data)?;

        // Deserialize from bytes
        let mut cursor = Cursor::new(serialized);
        let block = ValueBlock::from_reader_compressed(&mut cursor)?;

        assert!(!block.check_crc(54321)?);

        Ok(())
    }
}
