use crate::{
    file_accessor::FileAccessor,
    legacy::v3::RecoveredBlobFile,
    vlog::{
        blob_file::{Inner as BlobFileInner, Metadata},
        BlobFileId,
    },
    BlobFile, DescriptorTable, TreeId,
};
use std::{
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

pub fn recover_blob_files(
    folder: &Path,
    ids: &[RecoveredBlobFile],
    tree_id: TreeId,
    descriptor_table: Option<&Arc<DescriptorTable>>,
) -> crate::Result<(Vec<BlobFile>, Vec<PathBuf>)> {
    if !folder.try_exists()? {
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

    for (idx, dirent) in std::fs::read_dir(folder)?.enumerate() {
        let dirent = dirent?;
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
            log::error!("invalid table file name {}", file_name.display());
            crate::Error::Unrecoverable
        })?;

        let blob_file_id = blob_file_name.parse::<BlobFileId>().map_err(|e| {
            log::error!("invalid table file name {blob_file_name:?}: {e:?}");
            crate::Error::Unrecoverable
        })?;

        let blob_file_path = dirent.path();
        assert!(!blob_file_path.is_dir());

        if let Some(bf) = ids.iter().find(|bf| bf.id == blob_file_id) {
            log::trace!(
                "Recovering blob file #{blob_file_id:?} from {}",
                blob_file_path.display(),
            );

            let file = std::fs::File::open(&blob_file_path)?;

            let meta = {
                let reader = sfa::Reader::new(&blob_file_path)?;
                let toc = reader.toc();

                let metadata_section = toc.section(b"meta")
                  .ok_or(crate::Error::Unrecoverable)
                  .inspect_err(|_| {
                      log::error!("meta section in blob file #{blob_file_id} is missing - maybe the file is corrupted?");
                  })?;

                let metadata_slice = crate::file::read_exact(
                    &file,
                    metadata_section.pos(),
                    metadata_section.len() as usize,
                )?;

                Metadata::from_slice(&metadata_slice)?
            };

            let file_accessor = if let Some(dt) = descriptor_table.cloned() {
                FileAccessor::DescriptorTable(dt)
            } else {
                FileAccessor::File(Arc::new(file))
            };

            blob_files.push(BlobFile(Arc::new(BlobFileInner {
                id: blob_file_id,
                path: blob_file_path,
                meta,
                is_deleted: AtomicBool::new(false),
                checksum: bf.checksum,
                file_accessor,
                tree_id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Checksum;
    use test_log::test;

    #[test]
    fn vlog_recovery_missing_blob_file() {
        assert!(matches!(
            recover_blob_files(
                Path::new("."),
                &[RecoveredBlobFile {
                    id: 0,
                    checksum: Checksum::from_raw(0)
                }],
                0,
                None
            ),
            Err(crate::Error::Unrecoverable),
        ));
    }
}
