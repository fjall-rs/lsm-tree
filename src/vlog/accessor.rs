// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    fs::{FileSystem, StdFileSystem},
    version::BlobFileList,
    vlog::{blob_file::reader::Reader, ValueHandle},
    Cache, DescriptorTable, GlobalTableId, TreeId, UserValue,
};
use std::{path::Path, sync::Arc};

pub struct Accessor<'a, F: FileSystem = StdFileSystem>(&'a BlobFileList<F>);

impl<'a, F: FileSystem> Accessor<'a, F> {
    pub fn new(blob_files: &'a BlobFileList<F>) -> Self {
        Self(blob_files)
    }

    pub fn get(
        &self,
        tree_id: TreeId,
        base_path: &Path,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
        descriptor_table: &DescriptorTable<F>,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(value) = cache.get_blob(tree_id, vhandle) {
            return Ok(Some(value));
        }

        let Some(blob_file) = self.0.get(vhandle.blob_file_id) else {
            return Ok(None);
        };

        let bf_id = GlobalTableId::from((tree_id, blob_file.id()));

        let cached_fd = descriptor_table.access_for_blob_file(&bf_id);
        let fd_cache_miss = cached_fd.is_none();

        let file = if let Some(fd) = cached_fd {
            fd
        } else {
            Arc::new(F::open(&base_path.join(vhandle.blob_file_id.to_string()))?)
        };

        let value = Reader::new(blob_file, &file).get(key, vhandle)?;
        cache.insert_blob(tree_id, vhandle, value.clone());

        if fd_cache_miss {
            descriptor_table.insert_for_blob_file(bf_id, file);
        }

        Ok(Some(value))
    }
}
