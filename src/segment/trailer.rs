// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::index_block::BlockHandle;
use crate::{
    coding::{Decode, DecodeError, Encode},
    file::MAGIC_BYTES,
};
use std::{
    fs::File,
    io::{Read, Seek, Write},
};

const TRAILER_SIZE: usize = 32;

/// The fixed-size segment trailer stores a block handle to the regions block
///
/// # Diagram
///
/// ----------------
/// | data blocks  | <- implicitly start at 0
/// |--------------|
/// | tli block    |
/// |--------------|
/// | index block  | <- may not exist (if full block index is used, TLI will be dense)
/// |--------------|
/// | filter block | <- may not exist
/// |--------------|
/// |  ... TBD ... |
/// |--------------|
/// | meta block   |
/// |--------------|
/// | region block |
/// |--------------|
/// | trailer      | <- fixed size
/// |--------------|
///
/// Through this indirection, we can have a variable number of region block handles.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Trailer {
    regions_block_handle: BlockHandle,
}

impl Trailer {
    pub fn from_handle(regions_block_handle: BlockHandle) -> Self {
        Self {
            regions_block_handle,
        }
    }

    pub fn regions_block_handle(&self) -> &BlockHandle {
        &self.regions_block_handle
    }

    pub fn write_into<W: std::io::Write>(&self, writer: &mut W) -> crate::Result<()> {
        let mut v = Vec::with_capacity(TRAILER_SIZE);

        v.write_all(&MAGIC_BYTES)?;

        self.regions_block_handle.encode_into(&mut v)?;

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
    // TODO: then we don't need &mut File
    pub fn from_file(file: &mut File) -> crate::Result<Self> {
        file.seek(std::io::SeekFrom::End(-(TRAILER_SIZE as i64)))?;

        let mut trailer_bytes = [0u8; TRAILER_SIZE];
        file.read_exact(&mut trailer_bytes)?;

        let mut reader = &mut &trailer_bytes[..];

        // Check trailer magic header
        let mut magic = [0u8; MAGIC_BYTES.len()];
        reader.read_exact(&mut magic)?;

        if magic != MAGIC_BYTES {
            return Err(crate::Error::Decode(DecodeError::InvalidHeader(
                "SegmentTrailer",
            )));
        }

        // Get regions block handle
        let handle = BlockHandle::decode_from(&mut reader)?;

        Ok(Self::from_handle(handle))
    }
}
