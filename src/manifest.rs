// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::MAGIC_BYTES,
    segment::meta::TableType,
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError, TreeType, Version,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Write;

pub struct Manifest {
    pub(crate) version: Version,
    pub(crate) tree_type: TreeType,
    pub(crate) table_type: TableType,
    pub(crate) level_count: u8,
}

impl Serializable for Manifest {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_all(&MAGIC_BYTES)?;
        writer.write_u8(self.tree_type.into())?;
        writer.write_u8(self.table_type.into())?;
        writer.write_u8(self.level_count)?;
        Ok(())
    }
}

impl Deserializable for Manifest {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::DeserializeError> {
        let mut header = [0; MAGIC_BYTES.len()];
        reader.read_exact(&mut header)?;

        if header != MAGIC_BYTES {
            return Err(crate::DeserializeError::InvalidHeader("Manifest"));
        }

        let version = *header.get(3).expect("header must be size 4");
        let version = Version::try_from(version).map_err(|()| DeserializeError::InvalidVersion)?;

        let tree_type = reader.read_u8()?;
        let table_type = reader.read_u8()?;
        let level_count = reader.read_u8()?;

        Ok(Self {
            version,
            level_count,
            tree_type: tree_type
                .try_into()
                .map_err(|()| DeserializeError::InvalidTag(("TreeType", tree_type)))?,
            table_type: table_type
                .try_into()
                .map_err(|()| DeserializeError::InvalidTag(("TableType", table_type)))?,
        })
    }
}
