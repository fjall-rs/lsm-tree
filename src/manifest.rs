// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    segment::meta::TableType,
    serde::{Deserializable, Serializable},
    SerializeError, TreeType, Version,
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
        self.version.serialize(writer)?;
        writer.write_u8(self.tree_type.into())?;
        writer.write_u8(self.table_type.into())?;
        writer.write_u8(self.level_count)?;
        Ok(())
    }
}

impl Deserializable for Manifest {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::DeserializeError> {
        let version = Version::deserialize(reader)?;
        let tree_type = reader.read_u8()?;
        let table_type = reader.read_u8()?;
        let level_count = reader.read_u8()?;

        Ok(Self {
            version,
            tree_type: tree_type.try_into().expect("invalid tree type"),
            table_type: table_type.try_into().expect("invalid table type"),
            level_count,
        })
    }
}
