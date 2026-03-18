// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    vlog::{
        blob_file::writer::{BLOB_HEADER_LEN, BLOB_HEADER_MAGIC},
        ValueHandle,
    },
    BlobFile, Checksum, CompressionType, UserValue,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{Cursor, Read, Seek},
};

/// Safety cap on blob value size (256 MiB).
///
/// Enforced on the read path to prevent allocating unreasonably large
/// decompression buffers from crafted/malicious blob files. Mirrors the
/// write-side cap in `writer` but is intentionally independent.
const MAX_BLOB_VALUE_SIZE: usize = 256 * 1024 * 1024;

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

        // Use checked arithmetic for the allocation size to prevent truncation
        // on 32-bit targets where `as usize` from u64 could silently wrap.
        let add_size = (BLOB_HEADER_LEN as u64)
            .checked_add(key.len() as u64)
            .ok_or(crate::Error::InvalidHeader("Blob"))?;

        // Cap-check the on-disk value size BEFORE allocating the read buffer.
        // vhandle.on_disk_size comes from the value log index and could be corrupted.
        if vhandle.on_disk_size as usize > MAX_BLOB_VALUE_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(vhandle.on_disk_size),
                limit: MAX_BLOB_VALUE_SIZE as u64,
            });
        }

        let read_len = u64::from(vhandle.on_disk_size)
            .checked_add(add_size)
            .ok_or(crate::Error::InvalidHeader("Blob"))?;
        let read_len =
            usize::try_from(read_len).map_err(|_| crate::Error::InvalidHeader("Blob"))?;

        let value = crate::file::read_exact(self.file, vhandle.offset, read_len)?;

        let mut reader = Cursor::new(&value[..]);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if magic != BLOB_HEADER_MAGIC {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        let expected_checksum = reader.read_u128::<LittleEndian>()?;

        let _seqno = reader.read_u64::<LittleEndian>()?;
        let key_len = reader.read_u16::<LittleEndian>()?;

        #[allow(unused, reason = "only used in feature flagged branch")]
        let real_val_len = reader.read_u32::<LittleEndian>()? as usize;

        if real_val_len > MAX_BLOB_VALUE_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: real_val_len as u64,
                limit: MAX_BLOB_VALUE_SIZE as u64,
            });
        }

        let _on_disk_val_len = reader.read_u32::<LittleEndian>()? as usize;

        reader.seek(std::io::SeekFrom::Current(key_len.into()))?;

        let data_offset =
            usize::try_from(add_size).map_err(|_| crate::Error::InvalidHeader("Blob"))?;
        let raw_data = value.slice(data_offset..);

        {
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(key);
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

        Ok(value)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::SequenceNumberCounter;
    use test_log::test;

    #[test]
    fn blob_reader_roundtrip() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"a", 0, b"abcdef")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        assert_eq!(reader.get(b"a", &handle)?, b"abcdef");

        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_roundtrip_lz4() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        let handle0 = writer.write(b"a", 0, b"abcdef")?;
        let handle1 = writer.write(b"b", 0, b"ghi")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        assert_eq!(reader.get(b"a", &handle0)?, b"abcdef");
        assert_eq!(reader.get(b"b", &handle1)?, b"ghi");

        Ok(())
    }
}
