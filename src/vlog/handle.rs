// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    vlog::BlobFileId,
};
use std::{
    hash::Hash,
    io::{Read, Write},
};
use varint_rs::{VarintReader, VarintWriter};

/// A value handle points into the value log
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ValueHandle {
    /// Blob file ID
    pub blob_file_id: BlobFileId,

    /// Offset in file
    pub offset: u64,

    /// On-disk size
    pub on_disk_size: u32,
}

impl Encode for ValueHandle {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_u64_varint(self.offset)?;
        writer.write_u64_varint(self.blob_file_id)?;
        writer.write_u32_varint(self.on_disk_size)?;
        Ok(())
    }
}

impl Decode for ValueHandle {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let offset = reader.read_u64_varint()?;
        let blob_file_id = reader.read_u64_varint()?;
        let on_disk_size = reader.read_u32_varint()?;

        Ok(Self {
            blob_file_id,
            offset,
            on_disk_size,
        })
    }
}
