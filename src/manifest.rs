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
        reason = "deserialized from on-disk manifest, retained for validation"
    )]
    pub tree_type: TreeType,
    pub level_count: u8,
    pub comparator_name: String,
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
                .expect("format_version section should exist in manifest");

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
                .expect("tree_type section should exist in manifest");

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
                .expect("level_count section should exist in manifest");

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
                    .expect("filter_hash_type section should exist in manifest");

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

        // Optional section — absent in manifests written before comparator
        // identity persistence was added. The `UserComparator` trait was
        // introduced in the same release cycle, so all pre-existing trees
        // used `DefaultUserComparator` whose `name()` returns "default".
        // Custom comparators cannot exist in old manifests.
        let comparator_name = match toc.section(b"comparator_name") {
            Some(section) => {
                let limit = crate::comparator::MAX_COMPARATOR_NAME_BYTES as u64;

                if section.len() > limit {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: section.len(),
                        limit,
                    });
                }

                let mut bytes = Vec::new();
                section.buf_reader(path)?.read_to_end(&mut bytes)?;

                String::from_utf8(bytes).map_err(|e| crate::Error::Utf8(e.utf8_error()))?
            }
            None => "default".to_owned(),
        };

        Ok(Self {
            version,
            tree_type,
            level_count,
            comparator_name,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;
    use std::io::Write;

    /// Write the mandatory manifest sections (format_version, tree_type,
    /// level_count, filter_hash_type) into an sfa archive at `path`.
    /// If `comparator_name` is `Some`, also writes that section.
    fn write_test_manifest(
        path: &std::path::Path,
        comparator_name: Option<&str>,
    ) -> crate::Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        writer.start("format_version")?;
        writer.write_u8(FormatVersion::V4.into())?;

        writer.start("tree_type")?;
        writer.write_u8(TreeType::Standard.into())?;

        writer.start("level_count")?;
        writer.write_u8(7)?;

        writer.start("filter_hash_type")?;
        writer.write_u8(u8::from(ChecksumType::Xxh3))?;

        if let Some(name) = comparator_name {
            writer.start("comparator_name")?;
            writer.write_all(name.as_bytes())?;
        }

        writer.finish()?;
        Ok(())
    }

    #[test]
    fn manifest_without_comparator_name_defaults_to_default() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("manifest");

        write_test_manifest(&path, None)?;

        let reader = sfa::Reader::new(&path)?;
        let manifest = Manifest::decode_from(&path, &reader)?;
        assert_eq!(manifest.comparator_name, "default");
        Ok(())
    }

    #[test]
    fn manifest_with_comparator_name_round_trips() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("manifest");

        write_test_manifest(&path, Some("u64-big-endian"))?;

        let reader = sfa::Reader::new(&path)?;
        let manifest = Manifest::decode_from(&path, &reader)?;
        assert_eq!(manifest.comparator_name, "u64-big-endian");
        Ok(())
    }

    #[test]
    fn manifest_rejects_oversized_comparator_name() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("manifest");

        let long_name = "x".repeat(300);
        write_test_manifest(&path, Some(&long_name))?;

        let reader = sfa::Reader::new(&path)?;
        let result = Manifest::decode_from(&path, &reader);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge"
        );
        Ok(())
    }

    #[test]
    fn manifest_rejects_invalid_utf8_comparator_name() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("manifest");

        // Write manifest with invalid UTF-8 bytes in comparator_name
        let file = std::fs::File::create(&path)?;
        let mut writer = sfa::Writer::from_writer(std::io::BufWriter::new(file));

        writer.start("format_version")?;
        writer.write_u8(FormatVersion::V4.into())?;
        writer.start("tree_type")?;
        writer.write_u8(TreeType::Standard.into())?;
        writer.start("level_count")?;
        writer.write_u8(7)?;
        writer.start("filter_hash_type")?;
        writer.write_u8(u8::from(ChecksumType::Xxh3))?;
        writer.start("comparator_name")?;
        writer.write_all(&[0xFF, 0xFE])?;

        writer.finish()?;

        let reader = sfa::Reader::new(&path)?;
        let result = Manifest::decode_from(&path, &reader);
        assert!(
            matches!(result, Err(crate::Error::Utf8(_))),
            "expected Utf8 error"
        );
        Ok(())
    }
}
