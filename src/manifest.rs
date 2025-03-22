// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    file::MAGIC_BYTES,
    segment::meta::TableType,
    TreeType, Version,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Write;

pub struct Manifest {
    pub(crate) version: Version,
    pub(crate) tree_type: TreeType,
    pub(crate) table_type: TableType,
    pub(crate) level_count: u8,
}

impl Encode for Manifest {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_all(&MAGIC_BYTES)?;
        writer.write_u8(self.tree_type.into())?;
        writer.write_u8(self.table_type.into())?;
        writer.write_u8(self.level_count)?;
        Ok(())
    }
}

impl Decode for Manifest {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let mut header = [0; MAGIC_BYTES.len()];
        reader.read_exact(&mut header)?;

        if header != MAGIC_BYTES {
            return Err(crate::DecodeError::InvalidHeader("Manifest"));
        }

        #[allow(clippy::expect_used)]
        let version = *header.get(3).expect("header must be length 4");
        let version = Version::try_from(version).map_err(|()| DecodeError::InvalidVersion)?;

        let tree_type = reader.read_u8()?;
        let table_type = reader.read_u8()?;
        let level_count = reader.read_u8()?;

        Ok(Self {
            version,
            level_count,
            tree_type: tree_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("TreeType", tree_type)))?,
            table_type: table_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("TableType", table_type)))?,
        })
    }
}
