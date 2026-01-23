// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod accessor;
pub mod blob_file;
mod handle;

pub use {
    accessor::Accessor, blob_file::multi_writer::MultiWriter as BlobFileWriter,
    blob_file::scanner::Scanner as BlobFileScanner, blob_file::BlobFile, handle::ValueHandle,
};

pub type BlobFileMergeScanner<F = crate::fs::StdFileSystem> = blob_file::merge::MergeScanner<F>;

use crate::{
    fs::FileSystem,
    vlog::blob_file::{Inner as BlobFileInner, Metadata},
    Checksum,
};
use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

pub fn recover_blob_files<F: FileSystem>(
    folder: &Path,
    ids: &[(BlobFileId, Checksum)],
) -> crate::Result<(Vec<BlobFile<F>>, Vec<PathBuf>)> {
    if !F::exists(folder)? {
        return Ok((vec![], vec![]));
    }

    let cnt = ids.len();

    let progress_mod = match cnt {
        _ if cnt <= 20 => 1,
        _ if cnt <= 100 => 10,
        _ => 100,
    };

    log::debug!("Recovering {cnt} blob files from {:?}", folder.display());

    let mut blob_files = Vec::with_capacity(ids.len());
    let mut orphaned_blob_files = vec![];

    for (idx, dirent) in F::read_dir(folder)?.into_iter().enumerate() {
        let file_name = dirent.file_name();

        // https://en.wikipedia.org/wiki/.DS_Store
        if file_name == ".DS_Store" {
            continue;
        }

        // https://en.wikipedia.org/wiki/AppleSingle_and_AppleDouble_formats
        if file_name.to_string_lossy().starts_with("._") {
            continue;
        }

        let blob_file_name = file_name.to_str().ok_or_else(|| {
            log::error!("invalid table file name {}", file_name.to_string_lossy());
            crate::Error::Unrecoverable
        })?;

        let blob_file_id = blob_file_name.parse::<BlobFileId>().map_err(|e| {
            log::error!("invalid table file name {blob_file_name:?}: {e:?}");
            crate::Error::Unrecoverable
        })?;

        let blob_file_path = dirent.path().to_path_buf();
        assert!(!dirent.is_dir());

        if let Some(&(_, checksum)) = ids.iter().find(|(id, _)| id == &blob_file_id) {
            log::trace!("Recovering blob file #{blob_file_id:?}");

            let meta = {
                let reader = sfa::Reader::new(&blob_file_path)?;
                let toc = reader.toc();

                let metadata_section = toc.section(b"meta")
                .ok_or(crate::Error::Unrecoverable)
                .inspect_err(|_| {
                    log::error!("meta section in blob file #{blob_file_id} is missing - maybe the file is corrupted?");
                })?;

                let file = F::open(&blob_file_path)?;

                let metadata_slice = crate::file::read_exact(
                    &file,
                    metadata_section.pos(),
                    metadata_section.len() as usize,
                )?;

                Metadata::from_slice(&metadata_slice)?
            };

            blob_files.push(BlobFile(Arc::new(BlobFileInner {
                id: blob_file_id,
                path: blob_file_path,
                phantom: std::marker::PhantomData,
                meta,
                is_deleted: AtomicBool::new(false),
                checksum,
            })));

            if idx % progress_mod == 0 {
                log::debug!("Recovered {idx}/{cnt} blob files");
            }
        } else {
            orphaned_blob_files.push(blob_file_path.clone());
        }
    }

    if blob_files.len() < ids.len() {
        return Err(crate::Error::Unrecoverable);
    }

    log::debug!("Successfully recovered {} blob files", blob_files.len());

    Ok((blob_files, orphaned_blob_files))
}

/// The unique identifier for a value log blob file.
pub type BlobFileId = u64;

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn vlog_recovery_missing_blob_file() {
        assert!(matches!(
            recover_blob_files::<crate::fs::StdFileSystem>(
                Path::new("."),
                &[(0, Checksum::from_raw(0))]
            ),
            Err(crate::Error::Unrecoverable),
        ));
    }
}
