// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{coding::Decode, version::VersionId, vlog::BlobFileId, Checksum, TableId};
use byteorder::{LittleEndian, ReadBytesExt};
use std::path::Path;

pub fn get_current_version(folder: &std::path::Path) -> crate::Result<VersionId> {
    use byteorder::{LittleEndian, ReadBytesExt};

    std::fs::File::open(folder.join("current"))
        .and_then(|mut f| f.read_u64::<LittleEndian>())
        .map_err(Into::into)
}

pub struct Recovery {
    pub curr_version_id: VersionId,
    pub segment_ids: Vec<Vec<Vec<(TableId, Checksum)>>>,
    pub blob_file_ids: Vec<(BlobFileId, Checksum)>,
    pub gc_stats: crate::blob_tree::FragmentationMap,
}

pub fn recover(folder: &Path) -> crate::Result<Recovery> {
    let curr_version_id = get_current_version(folder)?;
    let version_file_path = folder.join(format!("v{curr_version_id}"));

    log::info!(
        "Recovering current manifest at {}",
        version_file_path.display(),
    );

    let reader = sfa::Reader::new(&version_file_path)?;
    let toc = reader.toc();

    // // TODO: vvv move into Version::decode vvv
    let mut levels = vec![];

    {
        let mut reader = toc
            .section(b"tables")
            .expect("tables should exist")
            .buf_reader(&version_file_path)?;

        let level_count = reader.read_u8()?;

        for _ in 0..level_count {
            let mut level = vec![];
            let run_count = reader.read_u8()?;

            for _ in 0..run_count {
                let mut run = vec![];
                let table_count = reader.read_u32::<LittleEndian>()?;

                for _ in 0..table_count {
                    let id = reader.read_u64::<LittleEndian>()?;
                    let checksum_type = reader.read_u8()?;

                    if checksum_type != 0 {
                        return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                            "ChecksumType",
                            checksum_type,
                        ))));
                    }

                    let checksum = reader.read_u128::<LittleEndian>()?;
                    let checksum = Checksum::from_raw(checksum);

                    run.push((id, checksum));
                }

                level.push(run);
            }

            levels.push(level);
        }
    }

    let blob_file_ids = {
        let mut reader = toc
            .section(b"blob_files")
            .expect("blob_files should exist")
            .buf_reader(&version_file_path)?;

        let blob_file_count = reader.read_u32::<LittleEndian>()?;
        let mut blob_file_ids = Vec::with_capacity(blob_file_count as usize);

        for _ in 0..blob_file_count {
            let id = reader.read_u64::<LittleEndian>()?;

            let checksum_type = reader.read_u8()?;

            if checksum_type != 0 {
                return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                    "ChecksumType",
                    checksum_type,
                ))));
            }

            let checksum = reader.read_u128::<LittleEndian>()?;
            let checksum = Checksum::from_raw(checksum);

            blob_file_ids.push((id, checksum));
        }

        blob_file_ids
    };

    let gc_stats = {
        let mut reader = toc
            .section(b"blob_gc_stats")
            .expect("blob_gc_stats should exist")
            .buf_reader(&version_file_path)?;

        crate::blob_tree::FragmentationMap::decode_from(&mut reader)?
    };

    Ok(Recovery {
        curr_version_id,
        segment_ids: levels,
        blob_file_ids,
        gc_stats,
    })
}
