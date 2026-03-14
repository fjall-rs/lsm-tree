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

        let add_size = BLOB_HEADER_LEN + key.len();
        let read_len = (vhandle.on_disk_size as usize)
            .checked_add(add_size)
            .ok_or(crate::Error::Unrecoverable)?;

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

        let _on_disk_val_len = reader.read_u32::<LittleEndian>()? as usize;

        reader.seek(std::io::SeekFrom::Current(key_len.into()))?;

        let raw_data = value.slice(add_size..);

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
                // NOTE: size cap validation for real_val_len is in PR #7
                // (feat/#258-security-validate-uncompressedlength-before-decomp)
                let mut buf = vec![0u8; real_val_len];

                let bytes_written = lz4_flex::decompress_into(&raw_data, &mut buf)
                    .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != real_val_len {
                    return Err(crate::Error::Decompress(self.blob_file.0.meta.compression));
                }

                UserValue::from(buf)
            }

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(_) => {
                let decompressed = zstd::bulk::decompress(&raw_data, real_val_len)
                    .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                UserValue::from(decompressed)
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

    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_lz4_corrupted_real_val_len_triggers_decompress_error() -> crate::Result<()> {
        use byteorder::WriteBytesExt;

        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        let handle = writer.write(b"a", 0, b"abcdef")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // Tamper the real_val_len field in the blob file.
        // Header layout: MAGIC(4) + Checksum(16) + SeqNo(8) + KeyLen(2) + RealValLen(4) + ...
        // RealValLen is at offset 30 from the blob start.
        let real_val_len_offset = handle.offset + 4 + 16 + 8 + 2;

        {
            use std::io::{Seek, Write};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&blob_file.0.path)?;
            file.seek(std::io::SeekFrom::Start(real_val_len_offset))?;
            // Write a corrupted value: original len + 1
            file.write_u32::<LittleEndian>(b"abcdef".len() as u32 + 1)?;
            file.flush()?;
        }

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        match reader.get(b"a", &handle) {
            Err(crate::Error::Decompress(_)) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok"),
            Err(other) => panic!("expected Error::Decompress, got: {other:?}"),
        }

        Ok(())
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn blob_reader_roundtrip_zstd() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Zstd(3));

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
