// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{checksum::ChecksumType, FormatVersion, TreeType};
use byteorder::ReadBytesExt;
use std::{io::Read, path::Path};

pub struct Manifest {
    pub version: FormatVersion,
    #[expect(
        dead_code,
        reason = "tree_type is not currently used, but needed in future"
    )]
    pub tree_type: TreeType,
    pub level_count: u8,
}

impl Manifest {
    pub fn decode_from(path: &Path, reader: &sfa::Reader) -> Result<Self, crate::Error> {
        let toc = reader.toc();

        let version = {
            #[expect(
                clippy::expect_used,
                reason = "format_version section must exist in manifest"
            )]
            let section = toc
                .section(b"format_version")
                .expect("format_version section must exist in manifest");

            let mut reader = section.buf_reader(path)?;
            let version = reader.read_u8()?;
            FormatVersion::try_from(version).map_err(|()| crate::Error::InvalidVersion(version))?
        };

        let tree_type = {
            #[expect(
                clippy::expect_used,
                reason = "tree_type section must exist in manifest"
            )]
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
            #[expect(
                clippy::expect_used,
                reason = "level_count section must exist in manifest"
            )]
            let section = toc
                .section(b"level_count")
                .expect("level_count section must exist in manifest");

            let mut reader = section.buf_reader(path)?;
            reader.read_u8()?
        };

        // Currently level count is hard coded to 7
        assert_eq!(7, level_count, "level count should be 7");

        {
            let filter_hash_type = {
                #[expect(
                    clippy::expect_used,
                    reason = "filter_hash_type section must exist in manifest"
                )]
                let section = toc
                    .section(b"filter_hash_type")
                    .expect("filter_hash_type section must exist in manifest");

                section
                    .buf_reader(path)?
                    .bytes()
                    .collect::<Result<Vec<_>, _>>()?
            };

            // Only one supported right now (and probably forever)
            assert_eq!(
                &[u8::from(ChecksumType::Xxh3)],
                &*filter_hash_type,
                "filter_hash_type should be XXH3"
            );
        }

        Ok(Self {
            version,
            tree_type,
            level_count,
        })
    }
}
