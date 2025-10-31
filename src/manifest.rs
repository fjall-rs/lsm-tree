// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{FormatVersion, TreeType};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::{io::Write, path::Path};

pub struct Manifest {
    pub(crate) version: FormatVersion,
    pub(crate) tree_type: TreeType,
    pub(crate) level_count: u8,
}

impl Manifest {
    pub fn encode_into(&self, writer: &mut sfa::Writer) -> Result<(), crate::Error> {
        writer.start("format_version")?;
        writer.write_u8(self.version.into())?;

        writer.start("crate_version")?;
        writer.write_all(env!("CARGO_PKG_VERSION").as_bytes())?;

        writer.start("tree_type")?;
        writer.write_u8(self.tree_type.into())?;

        writer.start("level_count")?;
        writer.write_u8(self.level_count)?;

        Ok(())
    }
}

impl Manifest {
    pub fn decode_from(path: &Path, reader: &sfa::Reader) -> Result<Self, crate::Error> {
        let toc = reader.toc();

        let version = {
            let section = toc
                .section(b"format_version")
                .expect("format_version section must exist in manifest");

            let mut reader = section.buf_reader(path)?;
            let version = reader.read_u8()?;
            FormatVersion::try_from(version).map_err(|()| crate::Error::InvalidVersion(version))?
        };

        let tree_type = {
            let section = toc
                .section(b"tree_type")
                .expect("tree_type section must exist in manifest");

            let mut reader = section.buf_reader(path)?;
            let tree_type = reader.read_u8()?;
            tree_type
                .try_into()
                .map_err(|()| crate::Error::InvalidTag(("TreeType", tree_type)))?
        };

        let level_count = {
            let section = toc
                .section(b"level_count")
                .expect("level_count section must exist in manifest");

            let mut reader = section.buf_reader(path)?;
            reader.read_u8()?
        };

        // Currently level count is hard coded to 7
        assert_eq!(7, level_count, "level count should be 7");

        Ok(Self {
            version,
            tree_type,
            level_count,
        })
    }
}
