// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, Encode},
    vlog::ValueHandle,
};
use std::io::{Read, Write};
use varint_rs::{VarintReader, VarintWriter};

#[derive(Copy, Clone, Debug, Eq)]
pub struct BlobIndirection {
    pub(crate) vhandle: ValueHandle,
    pub(crate) size: u32,
}

impl PartialEq for BlobIndirection {
    fn eq(&self, other: &Self) -> bool {
        self.vhandle == other.vhandle && self.size == other.size
    }
}

impl Encode for BlobIndirection {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        self.vhandle.encode_into(writer)?;
        writer.write_u32_varint(self.size)?;
        Ok(())
    }
}

impl Decode for BlobIndirection {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let vhandle = ValueHandle::decode_from(reader)?;
        let size = reader.read_u32_varint()?;
        Ok(Self { vhandle, size })
    }
}
