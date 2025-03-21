// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::block::offset::BlockOffset;
use crate::{Decode, DecodeError, Encode, EncodeError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct FileOffsets {
    pub metadata_ptr: BlockOffset,
    pub index_block_ptr: BlockOffset,
    pub tli_ptr: BlockOffset,
    pub bloom_ptr: BlockOffset,

    // TODO: #46 https://github.com/fjall-rs/lsm-tree/issues/46
    pub range_filter_ptr: BlockOffset,

    // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2
    pub range_tombstones_ptr: BlockOffset,

    // TODO: prefix filter for l0, l1?
    pub pfx_ptr: BlockOffset,
}

impl FileOffsets {
    /// Returns the on-disk size
    #[must_use]
    pub const fn serialized_len() -> usize {
        7 * std::mem::size_of::<u64>()
    }
}

impl Encode for FileOffsets {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_u64::<BigEndian>(*self.metadata_ptr)?;
        writer.write_u64::<BigEndian>(*self.index_block_ptr)?;
        writer.write_u64::<BigEndian>(*self.tli_ptr)?;
        writer.write_u64::<BigEndian>(*self.bloom_ptr)?;
        writer.write_u64::<BigEndian>(*self.range_filter_ptr)?;
        writer.write_u64::<BigEndian>(*self.range_tombstones_ptr)?;
        writer.write_u64::<BigEndian>(*self.pfx_ptr)?;
        Ok(())
    }
}

impl Decode for FileOffsets {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let metadata_ptr = reader.read_u64::<BigEndian>()?;
        let index_block_ptr = reader.read_u64::<BigEndian>()?;
        let tli_ptr = reader.read_u64::<BigEndian>()?;
        let bloom_ptr = reader.read_u64::<BigEndian>()?;
        let rf_ptr = reader.read_u64::<BigEndian>()?;
        let range_tombstones_ptr = reader.read_u64::<BigEndian>()?;
        let pfx_ptr = reader.read_u64::<BigEndian>()?;

        Ok(Self {
            index_block_ptr: BlockOffset(index_block_ptr),
            tli_ptr: BlockOffset(tli_ptr),
            bloom_ptr: BlockOffset(bloom_ptr),
            range_filter_ptr: BlockOffset(rf_ptr),
            range_tombstones_ptr: BlockOffset(range_tombstones_ptr),
            pfx_ptr: BlockOffset(pfx_ptr),
            metadata_ptr: BlockOffset(metadata_ptr),
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
            bloom_ptr: BlockOffset(15),
            index_block_ptr: BlockOffset(14),
            metadata_ptr: BlockOffset(17),
            pfx_ptr: BlockOffset(18),
            range_filter_ptr: BlockOffset(13),
            range_tombstones_ptr: BlockOffset(5),
            tli_ptr: BlockOffset(4),
        };

        let buf = before.encode_into_vec();

        let mut cursor = Cursor::new(buf);
        let after = FileOffsets::decode_from(&mut cursor)?;

        assert_eq!(after, before);

        Ok(())
    }

    #[test]
    fn file_offsets_serialized_len() {
        let buf = FileOffsets::default().encode_into_vec();
        assert_eq!(FileOffsets::serialized_len(), buf.len());
    }
}
