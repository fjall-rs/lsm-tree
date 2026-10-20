// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    vlog::{blob_file::header::BLOB_HEADER_MAGIC, ValueHandle},
    BlobFile, Checksum, CompressionType, UserValue,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{Cursor, Read},
};

// Hard-coded now for backwards compatibility
//
// v4 added 4 more bytes (header checksum)
const BLOB_HEADER_LEN: usize = 38;

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

        let add_size = (BLOB_HEADER_LEN as u64) + (key.len() as u64);

        let value = crate::file::read_exact(
            self.file,
            vhandle.offset,
            (u64::from(vhandle.on_disk_size) + add_size) as usize,
        )?;

        let mut reader = Cursor::new(&value[..]);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if magic != BLOB_HEADER_MAGIC {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        let expected_checksum = reader.read_u128::<LittleEndian>()?;

        let _seqno = reader.read_u64::<LittleEndian>()?;
        let key_len = reader.read_u16::<LittleEndian>()?;

        let real_val_len = reader.read_u32::<LittleEndian>()?;

        let _on_disk_val_len = reader.read_u32::<LittleEndian>()? as usize;

        let key = crate::UserKey::from_reader(&mut reader, key_len.into())?;

        let raw_data = value.slice((add_size as usize)..);

        {
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(&key);
                hasher.update(&raw_data);
                hasher.digest128()
            };

            if expected_checksum != checksum {
                log::error!(
                    "Checksum mismatch for blob {vhandle:?}, got={checksum}, expected={expected_checksum}",
                );

                return Err(crate::Error::ChecksumMismatch {
                    got: Checksum::from_raw(checksum),
                    expected: Checksum::from_raw(expected_checksum),
                });
            }
        }

        #[warn(clippy::match_single_binding)]
        let value = match &self.blob_file.0.meta.compression {
            CompressionType::None => raw_data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                #[warn(unsafe_code)]
                let mut builder = unsafe { UserValue::builder_unzeroed(real_val_len as usize) };

                lz4_flex::decompress_into(&raw_data, &mut builder)
                    .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                builder.freeze().into()
            }
        };

        debug_assert_eq!(real_val_len, {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            {
                value.len() as u32
            }
        });

        Ok(value)
    }
}
