// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    vlog::{blob_file::writer::BLOB_HEADER_LEN, BlobFileId, ValueHandle},
    BlobFile, Cache, DescriptorTable, GlobalSegmentId, Slice, UserValue,
};
use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

pub struct Accessor<'a>(&'a BTreeMap<BlobFileId, BlobFile>);

impl<'a> Accessor<'a> {
    pub fn new(blob_files: &'a BTreeMap<BlobFileId, BlobFile>) -> Self {
        Self(blob_files)
    }

    pub fn disk_space(&self) -> u64 {
        self.0
            .values()
            .map(|x| x.0.meta.total_uncompressed_bytes)
            .sum()
    }

    pub fn get(
        &self,
        base_path: &Path,
        key: &[u8],
        vhandle: &ValueHandle,
        cache: &Cache,
        descriptor_table: &DescriptorTable,
    ) -> crate::Result<Option<UserValue>> {
        if let Some(value) = cache.get_blob(0 /* TODO: vlog ID... */, vhandle) {
            return Ok(Some(value));
        }

        let Some(blob_file) = self.0.get(&vhandle.blob_file_id) else {
            return Ok(None);
        };

        let bf_id = GlobalSegmentId::from((0 /* TODO: vlog ID */, vhandle.blob_file_id));

        let file = if let Some(fd) = descriptor_table.access_for_blob_file(&bf_id) {
            fd
        } else {
            let file = Arc::new(File::open(
                base_path.join(vhandle.blob_file_id.to_string()),
            )?);
            descriptor_table.insert_for_blob_file(bf_id, file.clone());
            file
        };

        let offset = vhandle.offset + (BLOB_HEADER_LEN as u64) + (key.len() as u64);

        #[warn(unsafe_code)]
        let mut builder = unsafe { Slice::builder_unzeroed(vhandle.on_disk_size as usize) };

        {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;

                let bytes_read = file.read_at(&mut builder, offset)?;

                assert_eq!(
                    bytes_read,
                    vhandle.on_disk_size as usize,
                    "not enough bytes read: file has length {}",
                    file.metadata()?.len(),
                );
            }

            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;

                let bytes_read = file.seek_read(&mut builder, offset)?;

                assert_eq!(
                    bytes_read,
                    vhandle.on_disk_size as usize,
                    "not enough bytes read: file has length {}",
                    file.metadata()?.len(),
                );
            }

            #[cfg(not(any(unix, windows)))]
            {
                compile_error!("unsupported OS");
                unimplemented!();
            }
        }

        // TODO: decompress? save compression type into blobfile.meta

        Ok(Some(builder.freeze().into()))
    }
}
