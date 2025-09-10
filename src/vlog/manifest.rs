// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    file::rewrite_atomic,
    vlog::{
        blob_file::{gc_stats::GcStats, meta::Metadata, trailer::Trailer},
        BlobFile, BlobFileId, BlobFileWriter as MultiWriter, Compressor,
    },
    HashMap, KeyRange,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::{
    io::Cursor,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

pub const VLOG_MARKER: &str = ".vlog";
pub const BLOB_FILES_FOLDER: &str = "segments"; // TODO: don't use separate folder, instead rename just <id>.blobs
const MANIFEST_FILE: &str = "vlog_manifest";

// TODO: use tree-level manifest to store blob files as well

#[allow(clippy::module_name_repetitions)]
pub struct ManifestInner<C: Compressor + Clone> {
    path: PathBuf,
    pub blob_files: RwLock<HashMap<BlobFileId, Arc<BlobFile<C>>>>,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct Manifest<C: Compressor + Clone>(Arc<ManifestInner<C>>);

impl<C: Compressor + Clone> std::ops::Deref for Manifest<C> {
    type Target = ManifestInner<C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C: Compressor + Clone> Manifest<C> {
    fn remove_unfinished_blob_files<P: AsRef<Path>>(
        folder: P,
        registered_ids: &[u64],
    ) -> crate::Result<()> {
        for dirent in std::fs::read_dir(folder)? {
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

            if dirent.file_type()?.is_file() {
                let blob_file_id = dirent
                    .file_name()
                    .to_str()
                    .expect("should be valid utf-8")
                    .parse::<u64>()
                    .expect("should be valid blob file ID");

                if !registered_ids.contains(&blob_file_id) {
                    log::trace!("Deleting unfinished vLog blob file {blob_file_id}");
                    std::fs::remove_file(dirent.path())?;
                }
            }
        }

        Ok(())
    }

    /// Parses blob file IDs from manifest file
    fn load_ids_from_disk<P: AsRef<Path>>(path: P) -> crate::Result<Vec<BlobFileId>> {
        let path = path.as_ref();
        log::debug!("Loading manifest from {}", path.display());

        let bytes = std::fs::read(path)?;

        let mut ids = vec![];

        let mut cursor = Cursor::new(bytes);

        let cnt = cursor.read_u64::<BigEndian>()?;

        for _ in 0..cnt {
            ids.push(cursor.read_u64::<BigEndian>()?);
        }

        Ok(ids)
    }

    /// Recovers a value log from disk
    pub(crate) fn recover<P: AsRef<Path>>(folder: P) -> crate::Result<Self> {
        let folder = folder.as_ref();
        let manifest_path = folder.join(MANIFEST_FILE);

        log::info!("Recovering vLog at {}", folder.display());

        let ids = Self::load_ids_from_disk(&manifest_path)?;
        let cnt = ids.len();

        let progress_mod = match cnt {
            _ if cnt <= 20 => 1,
            _ if cnt <= 100 => 10,
            _ => 100,
        };

        log::debug!(
            "Recovering {cnt} vLog blob files from {:?}",
            folder.display(),
        );

        let blob_files_folder = folder.join(BLOB_FILES_FOLDER);
        Self::remove_unfinished_blob_files(&blob_files_folder, &ids)?;

        let blob_files = {
            let mut map = HashMap::with_capacity_and_hasher(100, rustc_hash::FxBuildHasher);

            for (idx, &id) in ids.iter().enumerate() {
                log::trace!("Recovering blob file #{id:?}");

                let path = blob_files_folder.join(id.to_string());
                let trailer = Trailer::from_file(&path)?;

                map.insert(
                    id,
                    Arc::new(BlobFile {
                        id,
                        path,
                        meta: trailer.metadata,
                        gc_stats: GcStats::default(),
                        _phantom: PhantomData,
                    }),
                );

                if idx % progress_mod == 0 {
                    log::debug!("Recovered {idx}/{cnt} vLog blob files");
                }
            }

            map
        };

        if blob_files.len() < ids.len() {
            return Err(crate::Error::Unrecoverable);
        }

        Ok(Self(Arc::new(ManifestInner {
            path: manifest_path,
            blob_files: RwLock::new(blob_files),
        })))
    }

    pub(crate) fn create_new<P: AsRef<Path>>(folder: P) -> crate::Result<Self> {
        let path = folder.as_ref().join(MANIFEST_FILE);

        let m = Self(Arc::new(ManifestInner {
            path,
            blob_files: RwLock::new(HashMap::default()),
        }));
        Self::write_to_disk(&m.path, &[])?;

        Ok(m)
    }

    /// Modifies the level manifest atomically.
    pub(crate) fn atomic_swap<F: FnOnce(&mut HashMap<BlobFileId, Arc<BlobFile<C>>>)>(
        &self,
        f: F,
    ) -> crate::Result<()> {
        let mut prev_blob_files = self.blob_files.write().expect("lock is poisoned");

        // NOTE: Create a copy of the levels we can operate on
        // without mutating the current level manifest
        // If persisting to disk fails, this way the level manifest
        // is unchanged
        let mut working_copy = prev_blob_files.clone();

        f(&mut working_copy);

        let ids = working_copy.keys().copied().collect::<Vec<_>>();

        Self::write_to_disk(&self.path, &ids)?;
        *prev_blob_files = working_copy;

        // NOTE: Lock needs to live until end of function because
        // writing to disk needs to be exclusive
        drop(prev_blob_files);

        log::trace!("Swapped vLog blob file list to: {ids:?}");

        Ok(())
    }

    /// Drops all blob files.
    ///
    /// This does not delete the files from disk, but just un-refs them from the manifest.
    ///
    /// Once this function completes, the disk files can be safely removed.
    pub fn clear(&self) -> crate::Result<()> {
        self.atomic_swap(|recipe| {
            recipe.clear();
        })
    }

    /// Drops the given blob files.
    ///
    /// This does not delete the files from disk, but just un-refs them from the manifest.
    ///
    /// Once this function completes, the disk files can be safely removed.
    pub fn drop_blob_files(&self, ids: &[u64]) -> crate::Result<()> {
        self.atomic_swap(|recipe| {
            recipe.retain(|x, _| !ids.contains(x));
        })
    }

    pub fn register(&self, writer: MultiWriter<C>) -> crate::Result<()> {
        let writers = writer.finish()?;

        self.atomic_swap(move |recipe| {
            for writer in writers {
                if writer.item_count == 0 {
                    log::debug!(
                        "Writer at {} has written no data, deleting empty vLog blob file",
                        writer.path.display(),
                    );
                    if let Err(e) = std::fs::remove_file(&writer.path) {
                        log::warn!(
                            "Could not delete empty vLog blob file at {}: {e:?}",
                            writer.path.display(),
                        );
                    }
                    continue;
                }

                let blob_file_id = writer.blob_file_id;

                recipe.insert(
                    blob_file_id,
                    Arc::new(BlobFile {
                        id: blob_file_id,
                        path: writer.path,
                        meta: Metadata {
                            item_count: writer.item_count,
                            compressed_bytes: writer.written_blob_bytes,
                            total_uncompressed_bytes: writer.uncompressed_bytes,

                            // NOTE: We are checking for 0 items above
                            // so first and last key need to exist
                            #[allow(clippy::expect_used)]
                            key_range: KeyRange::new((
                                writer
                                    .first_key
                                    .clone()
                                    .expect("should have written at least 1 item"),
                                writer
                                    .last_key
                                    .clone()
                                    .expect("should have written at least 1 item"),
                            )),
                        },
                        gc_stats: GcStats::default(),
                        _phantom: PhantomData,
                    }),
                );

                log::debug!(
                    "Created blob file #{blob_file_id:?} ({} items, {} userdata bytes)",
                    writer.item_count,
                    writer.uncompressed_bytes,
                );
            }
        })?;

        // NOTE: If we crash before before finishing the index write, it's fine
        // because all new blob files will be unreferenced, and thus can be dropped because stale

        Ok(())
    }

    fn write_to_disk<P: AsRef<Path>>(path: P, blob_file_ids: &[BlobFileId]) -> crate::Result<()> {
        let path = path.as_ref();
        log::trace!("Writing blob files manifest to {}", path.display());

        let mut bytes = Vec::new();

        let cnt = blob_file_ids.len() as u64;
        bytes.write_u64::<BigEndian>(cnt)?;

        for id in blob_file_ids {
            bytes.write_u64::<BigEndian>(*id)?;
        }

        rewrite_atomic(path, &bytes)?;

        Ok(())
    }

    /// Gets a blob file.
    #[must_use]
    pub fn get_blob_file(&self, id: BlobFileId) -> Option<Arc<BlobFile<C>>> {
        self.blob_files
            .read()
            .expect("lock is poisoned")
            .get(&id)
            .cloned()
    }

    /// Lists all blob file IDs.
    #[doc(hidden)]
    #[must_use]
    pub fn list_blob_file_ids(&self) -> Vec<BlobFileId> {
        self.blob_files
            .read()
            .expect("lock is poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// Lists all blob files.
    #[must_use]
    pub fn list_blob_files(&self) -> Vec<Arc<BlobFile<C>>> {
        self.blob_files
            .read()
            .expect("lock is poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Returns the number of blob files.
    #[must_use]
    pub fn len(&self) -> usize {
        self.blob_files.read().expect("lock is poisoned").len()
    }

    /// Returns the amount of bytes on disk that are occupied by blobs.
    #[must_use]
    pub fn disk_space_used(&self) -> u64 {
        self.blob_files
            .read()
            .expect("lock is poisoned")
            .values()
            .map(|x| x.meta.compressed_bytes)
            .sum::<u64>()
    }

    /// Returns the amount of stale bytes
    #[must_use]
    pub fn total_bytes(&self) -> u64 {
        self.blob_files
            .read()
            .expect("lock is poisoned")
            .values()
            .map(|x| x.meta.total_uncompressed_bytes)
            .sum::<u64>()
    }

    /// Returns the amount of stale bytes
    #[must_use]
    pub fn stale_bytes(&self) -> u64 {
        self.blob_files
            .read()
            .expect("lock is poisoned")
            .values()
            .map(|x| x.gc_stats.stale_bytes())
            .sum::<u64>()
    }

    /// Returns the percent of dead bytes (uncompressed) in the value log
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn stale_ratio(&self) -> f32 {
        let total_bytes = self.total_bytes();
        if total_bytes == 0 {
            return 0.0;
        }

        let stale_bytes = self.stale_bytes();

        if stale_bytes == 0 {
            return 0.0;
        }

        stale_bytes as f32 / total_bytes as f32
    }

    /// Returns the approximate space amplification
    ///
    /// Returns 0.0 if there are no items or the entire value log is stale.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn space_amp(&self) -> f32 {
        let total_bytes = self.total_bytes();
        if total_bytes == 0 {
            return 0.0;
        }

        let stale_bytes = self.stale_bytes();

        let alive_bytes = total_bytes - stale_bytes;
        if alive_bytes == 0 {
            return 0.0;
        }

        total_bytes as f32 / alive_bytes as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn test_atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
