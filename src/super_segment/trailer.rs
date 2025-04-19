// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::index_block::NewBlockHandle;
use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
};
use std::{
    fs::File,
    io::{BufReader, Read, Seek, Write},
    path::Path,
};

const TRAILER_SIZE: usize = 128;

/// The segment trailer stores offsets to the different segment disk file "zones"
///
/// ----------------
/// |  data blocks | <- implicitly start at 0
/// |--------------|
/// |      tli     |
/// |--------------|
/// | index block  | <- may not exist (if full block index is used, TLI will be dense)
/// |--------------|
/// | filter block | <- may not exist
/// |--------------|
/// |  ... TBD ... |
/// |--------------|
/// |   meta block |
/// |--------------|
/// |    trailer   | <- fixed size
/// |--------------|
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Trailer {
    pub tli: NewBlockHandle,
    pub index_blocks: Option<NewBlockHandle>,
    pub filter: Option<NewBlockHandle>, // option

    // // TODO: #2 https://github.com/fjall-rs/lsm-tree/issues/2
    // pub range_tombstones: BlockOffset,

    // // TODO: prefix filter for l0, l1?
    // pub pfx: BlockOffset,

    // // TODO: #46 https://github.com/fjall-rs/lsm-tree/issues/46
    // pub range_filter: BlockOffset,
    pub metadata: NewBlockHandle,
}

impl Trailer {
    /*    /// Returns the on-disk size
    #[must_use]
    pub const fn serialized_len() -> usize {
        4 * std::mem::size_of::<u64>()
    } */

    pub fn write_into<W: std::io::Write>(&self, writer: &mut W) -> crate::Result<()> {
        let mut v = Vec::with_capacity(TRAILER_SIZE);

        v.write_all(&MAGIC_BYTES)?;

        self.encode_into(&mut v)?;

        // Pad with remaining bytes
        v.resize(TRAILER_SIZE, 0);

        assert_eq!(
            v.len(),
            TRAILER_SIZE,
            "segment file trailer has invalid size"
        );

        writer.write_all(&v)?;

        Ok(())
    }

    // TODO: the trailer is fixed size so we can use read_at?!
    pub fn from_file(path: &Path) -> crate::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::End(-(TRAILER_SIZE as i64)))?;

        // Check trailer magic header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        // Parse pointers
        let trailer = Self::decode_from(&mut reader)?;

        if magic != MAGIC_BYTES {
            return Err(crate::Error::Decode(DecodeError::InvalidHeader(
                "SegmentTrailer",
            )));
        }

        debug_assert!(*trailer.tli.offset() > 0);
        debug_assert!(*trailer.metadata.offset() > 0);

        Ok(trailer)
    }
}

// TODO: honestly we could just store the meta offset in trailer, and the just store pointers in meta...
impl Encode for Trailer {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        self.tli.encode_into(writer)?;

        if let Some(handle) = &self.index_blocks {
            handle.encode_into(writer)
        } else {
            NewBlockHandle::default().encode_into(writer)
        }?;

        if let Some(handle) = &self.filter {
            handle.encode_into(writer)
        } else {
            NewBlockHandle::default().encode_into(writer)
        }?;

        self.metadata.encode_into(writer)?;

        Ok(())
    }
}

impl Decode for Trailer {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let tli = NewBlockHandle::decode_from(reader)?;
        let index_blocks = NewBlockHandle::decode_from(reader)?;
        let filter = NewBlockHandle::decode_from(reader)?;
        let metadata = NewBlockHandle::decode_from(reader)?;

        Ok(Self {
            index_blocks: match *index_blocks.offset() {
                0 => None,
                _ => Some(index_blocks),
            },
            tli,
            filter: match *filter.offset() {
                0 => None,
                _ => Some(filter),
            },
            metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::super_segment::BlockOffset;
    use std::io::Cursor;
    use test_log::test;

    #[test]
    fn v3_file_offsets_roundtrip() -> crate::Result<()> {
        let before = Trailer {
            tli: NewBlockHandle::new(BlockOffset(15), 5),
            index_blocks: Some(NewBlockHandle::new(BlockOffset(20), 5)),
            filter: Some(NewBlockHandle::new(BlockOffset(25), 5)),
            metadata: NewBlockHandle::new(BlockOffset(30), 5),
        };

        let buf = before.encode_into_vec();

        let mut cursor = Cursor::new(buf);
        let after = Trailer::decode_from(&mut cursor)?;

        assert_eq!(after, before);

        Ok(())
    }

    /*   #[test]
    fn v3_file_offsets_serialized_len() {
        let buf = Trailer::default().encode_into_vec();
        assert_eq!(Trailer::serialized_len(), buf.len());
    } */
}
