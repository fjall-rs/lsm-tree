use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Debug, Default)]
pub struct FileOffsets {
    pub index_block_ptr: u64,
    pub tli_ptr: u64,
    pub bloom_ptr: u64,

    // TODO: #46 https://github.com/fjall-rs/lsm-tree/issues/46
    pub rf_ptr: u64,

    // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2
    pub range_tombstones_ptr: u64,

    pub metadata_ptr: u64,
}

impl FileOffsets {
    /// Returns the on-disk size
    #[must_use]
    pub const fn serialized_len() -> usize {
        6 * std::mem::size_of::<u64>()
    }
}

impl Serializable for FileOffsets {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.index_block_ptr)?;
        writer.write_u64::<BigEndian>(self.tli_ptr)?;
        writer.write_u64::<BigEndian>(self.bloom_ptr)?;
        writer.write_u64::<BigEndian>(self.rf_ptr)?;
        writer.write_u64::<BigEndian>(self.range_tombstones_ptr)?;
        writer.write_u64::<BigEndian>(self.metadata_ptr)?;
        Ok(())
    }
}

impl Deserializable for FileOffsets {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let index_block_ptr = reader.read_u64::<BigEndian>()?;
        let tli_ptr = reader.read_u64::<BigEndian>()?;
        let bloom_ptr = reader.read_u64::<BigEndian>()?;
        let rf_ptr = reader.read_u64::<BigEndian>()?;
        let range_tombstones_ptr = reader.read_u64::<BigEndian>()?;
        let metadata_ptr = reader.read_u64::<BigEndian>()?;

        Ok(Self {
            index_block_ptr,
            tli_ptr,
            bloom_ptr,
            rf_ptr,
            range_tombstones_ptr,
            metadata_ptr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn file_offsets_serialized_len() -> crate::Result<()> {
        let mut buf = vec![];
        FileOffsets::default().serialize(&mut buf)?;

        assert_eq!(FileOffsets::serialized_len(), buf.len());

        Ok(())
    }
}
