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

/// Safety cap on the maximum blob value size (256 MiB), applied to both
/// the decompressed output and the total on-disk read (via `max_total_read_size`,
/// which adds header + key overhead on top).
///
/// This is intentionally stricter than the on-disk format limit (`u32::MAX`)
/// to guard against decompression bombs and OOM from crafted/malicious files.
///
/// NOTE: This constant is intentionally duplicated in `table::block`
/// (as `u32`) rather than shared, because blocks and blobs are independent
/// storage formats that may diverge in the future. Keep values in sync manually.
const MAX_DECOMPRESSION_SIZE: usize = 256 * 1024 * 1024;

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

        // Enforce the same key-length constraint as the writer (u16::MAX)
        // so that a caller cannot inflate the computed read size.
        if key.len() > u16::MAX as usize {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        let add_size = (BLOB_HEADER_LEN as u64) + (key.len() as u64);

        // Validate the full on-disk read size (header + key + value) against the limit.
        // Allow header+key overhead on top of the data cap.
        // NOTE: A separate `on_disk_size > MAX` check is mathematically redundant here
        // because `total > MAX + overhead` already implies `on_disk_size > MAX`.
        let max_total_read_size = (MAX_DECOMPRESSION_SIZE as u64).saturating_add(add_size);

        // on_disk_size is u32 and add_size < u32::MAX, so this cannot overflow u64.
        let total_read_size = u64::from(vhandle.on_disk_size) + add_size;

        if total_read_size > max_total_read_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: total_read_size,
                limit: max_total_read_size,
            });
        }

        // After the cap check, total_read_size <= ~256 MiB + overhead, which fits
        // in usize on all supported platforms (>= 32-bit).
        #[expect(
            clippy::cast_possible_truncation,
            reason = "bounded to MAX_DECOMPRESSION_SIZE + overhead by the check above"
        )]
        let read_len = total_read_size as usize;

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

        let real_val_len = reader.read_u32::<LittleEndian>()? as usize;

        let on_disk_val_len = reader.read_u32::<LittleEndian>()?;

        // Cross-check header fields against caller-provided inputs to catch
        // corruption or mismatched handles early, before checksum/decompression.
        if key_len as usize != key.len() || on_disk_val_len != vhandle.on_disk_size {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Validate real_val_len before checksum/decompression to fail fast
        // on malformed headers and avoid unnecessary hashing work.
        if real_val_len > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: real_val_len as u64,
                limit: MAX_DECOMPRESSION_SIZE as u64,
            });
        }

        // NOTE: This seek is a no-op for the current code path (raw_data is sliced
        // from `value` by offset, not read via `reader`), but kept to maintain the
        // cursor position in case future code reads further fields after the key.
        reader.seek(std::io::SeekFrom::Current(key_len.into()))?;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "add_size = BLOB_HEADER_LEN + key.len(); key.len() <= u16::MAX and BLOB_HEADER_LEN is a small constant, so add_size fits in usize"
        )]
        let raw_data = value.slice((add_size as usize)..);

        {
            // NOTE: Checksum is computed over the caller-provided key (not the on-disk
            // key bytes). This matches the writer, which hashes caller key + value.
            // The key_len cross-check above catches length mismatches (truncation or
            // wrong-handle reads), but not length-preserving key corruption.
            // Full content-level key verification would require changing the checksum
            // contract and is out of scope for this security hardening.
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
            CompressionType::None => {
                if real_val_len != raw_data.len() {
                    return Err(crate::Error::InvalidHeader("Blob"));
                }
                raw_data
            }

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                #[warn(unsafe_code)]
                let mut builder = unsafe { UserValue::builder_unzeroed(real_val_len) };

                let bytes_written = lz4_flex::decompress_into(&raw_data, &mut builder)
                    .map_err(|_| crate::Error::Decompress(self.blob_file.0.meta.compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != real_val_len {
                    return Err(crate::Error::Decompress(self.blob_file.0.meta.compression));
                }

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

    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_reject_absurd_real_val_len() {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir().unwrap();
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)
            .unwrap()
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        // write_raw lets us set an arbitrary uncompressed_len in the header
        let handle = writer.write_raw(b"k", 0, b"value", u32::MAX).unwrap();

        let blob_file = writer.finish().unwrap();
        let blob_file = blob_file.first().unwrap();

        let file = File::open(&blob_file.0.path).unwrap();
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"k", &handle);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_zero_real_val_len_with_data_fails_decompress() {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir().unwrap();
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)
            .unwrap()
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        // Zero real_val_len is allowed (valid for empty values), but when
        // compressed data is present, lz4 decompression fails on the mismatch.
        let handle = writer.write_raw(b"k", 0, b"value", 0).unwrap();

        let blob_file = writer.finish().unwrap();
        let blob_file = blob_file.first().unwrap();

        let file = File::open(&blob_file.0.path).unwrap();
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"k", &handle);
        assert!(
            matches!(result, Err(crate::Error::Decompress(_))),
            "expected Decompress error, got: {result:?}",
        );
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
    fn blob_reader_reject_oversized_on_disk_size() {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir().unwrap();
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)
            .unwrap()
            .use_target_size(u64::MAX);

        let mut handle = writer.write(b"a", 0, b"hello").unwrap();

        let blob_file = writer.finish().unwrap();
        let blob_file = blob_file.first().unwrap();

        // Tamper the handle to declare an absurd on_disk_size
        handle.on_disk_size = u32::MAX;

        let file = File::open(&blob_file.0.path).unwrap();
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"a", &handle);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
    }
}
