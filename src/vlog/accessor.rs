// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{version::BlobFileList, vlog::ValueHandle, Cache, GlobalTableId, TreeId, UserValue};
use std::{fs::File, path::Path, sync::Arc};

pub struct Accessor<'a>(&'a BlobFileList);

impl<'a> Accessor<'a> {
    pub fn new(blob_files: &'a BlobFileList) -> Self {
        Self(blob_files)
    }

    pub fn get(
        &self,
        tree_id: TreeId,
        base_path: &Path,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(value) = cache.get_blob(tree_id, vhandle) {
            return Ok(Some(value));
        }

        let Some(blob_file) = self.0.get(vhandle.blob_file_id) else {
            return Ok(None);
        };

        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));

        let (file, fd_cache_miss) =
            if let Some(cached_fd) = blob_file.file_accessor().access_for_blob_file(&bf_id) {
                (cached_fd, false)
            } else {
                let file = Arc::new(File::open(
                    base_path.join(vhandle.blob_file_id.to_string()),
                )?);
                (file, true)
            };

        let value = if blob_file.is_v3() {
            // NOTE: We support v3 blob files in v4
            super::blob_file::reader_v3::Reader::new(blob_file, &file).get(key, vhandle)?
        } else {
            super::blob_file::reader_v4::Reader::new(blob_file, &file).get(key, vhandle)?
        };

        cache.insert_blob(tree_id, vhandle, value.clone());

        if fd_cache_miss {
            blob_file.file_accessor().insert_for_blob_file(bf_id, file);
        }

        Ok(Some(value))
    }
}
