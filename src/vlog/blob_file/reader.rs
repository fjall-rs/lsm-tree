// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    vlog::{blob_file::writer::BLOB_HEADER_LEN, ValueHandle},
    BlobFile, UserValue,
};
use std::fs::File;

/// Reads a single blob from a blob file
pub struct Reader<'a> {
    blob_file: &'a BlobFile,
    file: &'a File,
}

impl<'a> Reader<'a> {
    pub fn new(blob_file: &'a BlobFile, file: &'a File) -> Self {
        Self { blob_file, file }
    }

    pub fn get(&self, key: &'a [u8], vhandle: &'a ValueHandle) -> crate::Result<UserValue> {
        debug_assert_eq!(vhandle.blob_file_id, self.blob_file.id());

        let offset = vhandle.offset + (BLOB_HEADER_LEN as u64) + (key.len() as u64);

        let value = crate::file::read_exact(self.file, offset, vhandle.on_disk_size as usize)?;

        // TODO: decompress? save compression type into blob_file.meta

        Ok(value)
    }
}

// TODO: unit test
