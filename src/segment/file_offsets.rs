// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct FileOffsets {
    pub metadata_ptr: u64,
    pub index_block_ptr: u64,
    pub tli_ptr: u64,
    pub bloom_ptr: u64,

    // TODO: #46 https://github.com/fjall-rs/lsm-tree/issues/46
    pub range_filter_ptr: u64,

    // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2
    pub range_tombstones_ptr: u64,

    // TODO: prefix filter for l0, l1?
    pub pfx_ptr: u64,
}

impl FileOffsets {
    /// Returns the on-disk size
    #[must_use]
    pub const fn serialized_len() -> usize {
        7 * std::mem::size_of::<u64>()
    }
}

impl Serializable for FileOffsets {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_u64::<BigEndian>(self.metadata_ptr)?;
        writer.write_u64::<BigEndian>(self.index_block_ptr)?;
        writer.write_u64::<BigEndian>(self.tli_ptr)?;
        writer.write_u64::<BigEndian>(self.bloom_ptr)?;
        writer.write_u64::<BigEndian>(self.range_filter_ptr)?;
        writer.write_u64::<BigEndian>(self.range_tombstones_ptr)?;
        writer.write_u64::<BigEndian>(self.pfx_ptr)?;
        Ok(())
    }
}

impl Deserializable for FileOffsets {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let metadata_ptr = reader.read_u64::<BigEndian>()?;
        let index_block_ptr = reader.read_u64::<BigEndian>()?;
        let tli_ptr = reader.read_u64::<BigEndian>()?;
        let bloom_ptr = reader.read_u64::<BigEndian>()?;
        let rf_ptr = reader.read_u64::<BigEndian>()?;
        let range_tombstones_ptr = reader.read_u64::<BigEndian>()?;
        let pfx_ptr = reader.read_u64::<BigEndian>()?;

        Ok(Self {
            index_block_ptr,
            tli_ptr,
            bloom_ptr,
            range_filter_ptr: rf_ptr,
            range_tombstones_ptr,
            pfx_ptr,
            metadata_ptr,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn file_offsets_roundtrip() -> crate::Result<()> {
        let before = FileOffsets {
            bloom_ptr: 15,
            index_block_ptr: 14,
            metadata_ptr: 17,
            pfx_ptr: 18,
            range_filter_ptr: 13,
            range_tombstones_ptr: 5,
            tli_ptr: 4,
        };

        let mut buf = vec![];
        before.serialize(&mut buf)?;

        let mut cursor = Cursor::new(buf);
        let after = FileOffsets::deserialize(&mut cursor)?;

        assert_eq!(after, before);

        Ok(())
    }

    #[test]
    fn file_offsets_serialized_len() -> crate::Result<()> {
        let mut buf = vec![];
        FileOffsets::default().serialize(&mut buf)?;

        assert_eq!(FileOffsets::serialized_len(), buf.len());

        Ok(())
    }
}
