// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    vlog::{
        blob_file::writer::{
            validate_header_crc, BLOB_HEADER_LEN_V4, BLOB_HEADER_MAGIC_V3, BLOB_HEADER_MAGIC_V4,
        },
        ValueHandle,
    },
    BlobFile, Checksum, CompressionType, UserValue,
};

/// Safety cap on blob value size (256 MiB).
///
/// Enforced on this reader and on the write path to prevent producing
/// or accepting blobs that are unreasonably large. Other internal
/// readers (e.g., scanner used by compaction/GC) may impose different
/// constraints.
///
/// NOTE: Intentionally duplicated in `vlog::blob_file::writer` and
/// `table::block` rather than shared, because blocks and blobs are
/// independent storage formats that may diverge in the future.
const MAX_DECOMPRESSION_SIZE: usize = 256 * 1024 * 1024;
use byteorder::{LittleEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{Cursor, Read},
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

    #[expect(
        clippy::too_many_lines,
        reason = "blob read/validation path is kept in one function so error handling and size checks stay co-located"
    )]
    pub fn get(&self, key: &'a [u8], vhandle: &'a ValueHandle) -> crate::Result<UserValue> {
        debug_assert_eq!(vhandle.blob_file_id, self.blob_file.id());

        // Enforce the same key-length constraint as the writer (u16::MAX)
        // so that a caller cannot inflate the computed read size.
        if key.len() > u16::MAX as usize {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Always read with V4 (max) header size so that version detection
        // is self-describing from the frame magic — no dependency on
        // metadata version which could be corrupted independently.
        // For V3 frames, the extra 4 bytes read are harmless: they come
        // from the next frame or metadata section (which always follows),
        // and raw_data is sliced to exact on_disk_val_len before use.
        let add_size = (BLOB_HEADER_LEN_V4 as u64) + (key.len() as u64);

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

        // Determine format from frame magic — self-describing, no metadata dependency.
        let frame_is_v4 = magic == BLOB_HEADER_MAGIC_V4;
        if !frame_is_v4 && magic != BLOB_HEADER_MAGIC_V3 {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        let expected_checksum = reader.read_u128::<LittleEndian>()?;

        let seqno = reader.read_u64::<LittleEndian>()?;
        let key_len = reader.read_u16::<LittleEndian>()?;

        let real_val_len = reader.read_u32::<LittleEndian>()? as usize;

        let on_disk_val_len = reader.read_u32::<LittleEndian>()?;

        // V4: read and validate header CRC before cross-checks.
        // Uses the on-disk CRC value (not recomputed) in data checksum
        // verification so that recomputing header_crc after tampering
        // header fields is still caught by the data checksum.
        let stored_header_crc = if frame_is_v4 {
            let crc = reader.read_u32::<LittleEndian>()?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "real_val_len originates as u32, round-tripped through usize; lossless on supported targets"
            )]
            validate_header_crc(seqno, key_len, real_val_len as u32, on_disk_val_len, crc)?;
            Some(crc)
        } else {
            // V3: seqno is unused (not covered by any checksum).
            let _ = seqno;
            None
        };

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

        // Actual header length determined from frame magic, not metadata.
        let header_len = if frame_is_v4 {
            BLOB_HEADER_LEN_V4
        } else {
            crate::vlog::blob_file::writer::BLOB_HEADER_LEN_V3
        };

        // Zero-copy view of the on-disk key bytes for checksum and cross-check.
        // The full blob record is already in `value`, so slicing avoids an extra
        // allocation vs UserKey::from_reader (upstream #277).
        let on_disk_key = value.slice(header_len..header_len + key_len as usize);

        // Ensure the stored key bytes exactly match the caller-provided key.
        // This protects against handles that point at a different key with the
        // same length (e.g., due to corruption or misuse).
        if on_disk_key != key {
            return Err(crate::Error::InvalidHeader("Blob"));
        }

        // Slice exactly on_disk_val_len bytes — important for V3 backward
        // compat where the read buffer is 4 bytes larger than the actual frame
        // (over-read from using V4 max header size).
        // No usize overflow: on_disk_val_len is u32, data_offset is ~42+key_len,
        // and total is bounded by MAX_DECOMPRESSION_SIZE (256 MiB) cap check above.
        let data_offset = header_len + key.len();
        let raw_data = value.slice(data_offset..data_offset + on_disk_val_len as usize);

        {
            // Checksum covers on-disk key + raw value data (upstream #277).
            // V4 additionally includes header_crc bytes so that recomputing
            // header_crc after tampering header fields is still detected.
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(&on_disk_key);
                hasher.update(&raw_data);
                if let Some(hcrc) = stored_header_crc {
                    hasher.update(&hcrc.to_le_bytes());
                }
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

                if decompressed.len() != real_val_len {
                    return Err(crate::Error::Decompress(self.blob_file.0.meta.compression));
                }

                UserValue::from(decompressed)
            }
        };

        debug_assert_eq!(real_val_len, value.len());

        Ok(value)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::vlog::blob_file::writer::BLOB_HEADER_LEN_V3;
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

    /// Tamper real_val_len to an absurd value: V4 header CRC catches the
    /// corruption before the size-cap check is even reached.
    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_reject_absurd_real_val_len() {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir().unwrap();
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)
            .unwrap()
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        let handle = writer.write_raw(b"k", 0, b"value", 5).unwrap();

        let blob_file = writer.finish().unwrap();
        let blob_file = blob_file.first().unwrap();

        // Patch real_val_len at handle.offset + magic(4) + checksum(16) + seqno(8) + key_len(2) = +30
        let mut raw = std::fs::read(&blob_file.0.path).unwrap();
        let real_val_len_offset = handle.offset as usize + 30;
        raw[real_val_len_offset..real_val_len_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&blob_file.0.path, &raw).unwrap();

        let file = File::open(&blob_file.0.path).unwrap();
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"k", &handle);
        assert!(
            matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
            "expected HeaderCrcMismatch, got: {result:?}",
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

    /// Tamper real_val_len in lz4 blob: V4 header CRC catches the
    /// corruption before decompression is attempted.
    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_lz4_corrupted_real_val_len_triggers_header_crc_mismatch() -> crate::Result<()> {
        use byteorder::WriteBytesExt;

        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        let handle = writer.write(b"a", 0, b"abcdef")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // RealValLen is at offset 30 from the blob start.
        let real_val_len_offset = handle.offset + 4 + 16 + 8 + 2;

        {
            use std::io::{Seek, Write};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&blob_file.0.path)?;
            file.seek(std::io::SeekFrom::Start(real_val_len_offset))?;
            file.write_u32::<LittleEndian>(b"abcdef".len() as u32 + 1)?;
            file.flush()?;
        }

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        match reader.get(b"a", &handle) {
            Err(crate::Error::HeaderCrcMismatch { .. }) => { /* header CRC catches it */ }
            Ok(_) => panic!("expected HeaderCrcMismatch, but got Ok"),
            Err(other) => panic!("expected HeaderCrcMismatch, got: {other:?}"),
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

    /// Tamper real_val_len in zstd blob: V4 header CRC catches the
    /// corruption before decompression is attempted.
    #[test]
    #[cfg(feature = "zstd")]
    fn blob_reader_zstd_corrupted_real_val_len_triggers_header_crc_mismatch() -> crate::Result<()> {
        use byteorder::WriteBytesExt;

        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Zstd(3));

        let handle = writer.write(b"a", 0, b"abcdef")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // RealValLen is at offset 30 from the blob start.
        let real_val_len_offset = handle.offset + 4 + 16 + 8 + 2;

        {
            use std::io::{Seek, Write};
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&blob_file.0.path)?;
            file.seek(std::io::SeekFrom::Start(real_val_len_offset))?;
            file.write_u32::<LittleEndian>(b"abcdef".len() as u32 + 1)?;
            file.flush()?;
        }

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        match reader.get(b"a", &handle) {
            Err(crate::Error::HeaderCrcMismatch { .. }) => { /* header CRC catches it */ }
            Ok(_) => panic!("expected HeaderCrcMismatch, but got Ok"),
            Err(other) => panic!("expected HeaderCrcMismatch, got: {other:?}"),
        }

        Ok(())
    }

    /// Tamper real_val_len to exceed size cap: V4 header CRC catches the
    /// corruption before the size-cap check is reached.
    #[test]
    fn blob_reader_rejects_oversized_real_val_len() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"a", 0, b"abcdef")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // Byte-patch real_val_len in the blob header
        let mut raw = std::fs::read(&blob_file.0.path)?;
        let real_val_len_offset = handle.offset as usize + 4 + 16 + 8 + 2;
        let oversize = (MAX_DECOMPRESSION_SIZE as u32) + 1;
        raw[real_val_len_offset..real_val_len_offset + 4].copy_from_slice(&oversize.to_le_bytes());
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"a", &handle);
        assert!(
            matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
            "expected HeaderCrcMismatch, got: {result:?}",
        );
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

    /// Tamper on-disk key bytes and verify two detection layers:
    /// 1. Original caller key → InvalidHeader from cross-check (fast path)
    /// 2. Tampered key as caller → ChecksumMismatch (checksum path, upstream #277)
    #[test]
    fn blob_reader_corrupted_on_disk_key_detected_by_cross_check_and_checksum() -> crate::Result<()>
    {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"abc", 0, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // Tamper on-disk key bytes.
        // V4 header layout: MAGIC(4) + Checksum(16) + SeqNo(8) + KeyLen(2) + RealValLen(4) + OnDiskValLen(4) + HeaderCrc(4) = 42
        // Key starts at offset 42 from blob start (BLOB_HEADER_LEN_V4).
        let key_offset = handle.offset as usize + BLOB_HEADER_LEN_V4;
        let mut raw = std::fs::read(&blob_file.0.path)?;
        raw[key_offset] ^= 0xFF; // flip bits in first key byte
        let corrupted_key = raw[key_offset..key_offset + 3].to_vec();
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        // Layer 1: original caller key vs tampered on-disk key → InvalidHeader
        let result = reader.get(b"abc", &handle);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader(Blob) from key cross-check, got: {result:?}",
        );

        // Layer 2: pass the tampered key as caller so cross-check passes,
        // but checksum (computed over tampered key + value) won't match the
        // stored checksum (computed over original key + value).
        let result = reader.get(&corrupted_key, &handle);
        assert!(
            matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
            "expected ChecksumMismatch for tampered on-disk key, got: {result:?}",
        );

        Ok(())
    }

    /// Verify that reading a blob with a caller key that differs from the
    /// stored key (same length, different bytes) is rejected.
    #[test]
    fn blob_reader_wrong_caller_key_same_length_returns_invalid_header() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"aaa", 0, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        // Correct key works
        assert_eq!(reader.get(b"aaa", &handle)?, b"value");

        // Wrong key with same length → InvalidHeader from cross-check
        let result = reader.get(b"bbb", &handle);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader(Blob) for wrong caller key, got: {result:?}",
        );

        Ok(())
    }

    /// Wrong caller key with different length is caught by the key_len
    /// cross-check (header field vs caller key length) before the on-disk
    /// key bytes are even read.
    #[test]
    fn blob_reader_wrong_caller_key_different_length_returns_invalid_header() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"abc", 0, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        // Shorter key
        let result = reader.get(b"ab", &handle);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader for shorter key, got: {result:?}",
        );

        // Longer key
        let result = reader.get(b"abcd", &handle);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader for longer key, got: {result:?}",
        );

        Ok(())
    }

    /// Tamper the value payload bytes (after the key) and verify the checksum
    /// catches the corruption. This validates the end-to-end checksum path
    /// for uncompressed blobs.
    #[test]
    fn blob_reader_corrupted_value_payload_triggers_checksum_mismatch() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"key", 0, b"payload_data")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // Value payload starts after header + key: offset + BLOB_HEADER_LEN_V4 + key_len
        let payload_offset = handle.offset as usize + BLOB_HEADER_LEN_V4 + b"key".len();
        let mut raw = std::fs::read(&blob_file.0.path)?;
        raw[payload_offset] ^= 0xFF; // flip bits in first value byte
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"key", &handle);
        assert!(
            matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
            "expected ChecksumMismatch for corrupted value, got: {result:?}",
        );

        Ok(())
    }

    /// Tamper on-disk key bytes in an lz4-compressed blob and verify the
    /// cross-check catches the corruption before decompression runs.
    #[test]
    #[cfg(feature = "lz4")]
    fn blob_reader_corrupted_on_disk_key_lz4_returns_invalid_header() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Lz4);

        let handle = writer.write(b"abc", 0, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        let key_offset = handle.offset as usize + BLOB_HEADER_LEN_V4;
        let mut raw = std::fs::read(&blob_file.0.path)?;
        raw[key_offset] ^= 0xFF;
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"abc", &handle);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader for corrupted lz4 key, got: {result:?}",
        );

        Ok(())
    }

    /// Tamper on-disk key bytes in a zstd-compressed blob and verify the
    /// cross-check catches the corruption before decompression runs.
    #[test]
    #[cfg(feature = "zstd")]
    fn blob_reader_corrupted_on_disk_key_zstd_returns_invalid_header() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX)
            .use_compression(CompressionType::Zstd(3));

        let handle = writer.write(b"abc", 0, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        let key_offset = handle.offset as usize + BLOB_HEADER_LEN_V4;
        let mut raw = std::fs::read(&blob_file.0.path)?;
        raw[key_offset] ^= 0xFF;
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"abc", &handle);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader for corrupted zstd key, got: {result:?}",
        );

        Ok(())
    }

    /// V4 header CRC detects seqno corruption — the primary motivating
    /// case for upstream #278. A corrupted seqno could cause MVCC
    /// time-travel returning wrong versions.
    #[test]
    fn blob_reader_v4_corrupted_seqno_detected_by_header_crc() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"key", 42, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // Tamper seqno: offset + magic(4) + checksum(16) = 20
        let seqno_offset = handle.offset as usize + 20;
        let mut raw = std::fs::read(&blob_file.0.path)?;
        // Change seqno from 42 to 99
        raw[seqno_offset..seqno_offset + 8].copy_from_slice(&99u64.to_le_bytes());
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"key", &handle);
        assert!(
            matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
            "expected HeaderCrcMismatch for corrupted seqno, got: {result:?}",
        );

        Ok(())
    }

    /// V4 header CRC field itself corrupted (header fields intact) is
    /// detected before the data checksum check.
    #[test]
    fn blob_reader_v4_corrupted_header_crc_field_detected() -> crate::Result<()> {
        let id_generator = SequenceNumberCounter::default();

        let folder = tempfile::tempdir()?;
        let mut writer = crate::vlog::BlobFileWriter::new(id_generator, folder.path(), 0, None)?
            .use_target_size(u64::MAX);

        let handle = writer.write(b"key", 0, b"value")?;

        let blob_file = writer.finish()?;
        let blob_file = blob_file.first().unwrap();

        // header_crc is at offset 38 (after magic+checksum+seqno+key_len+real_val_len+on_disk_val_len)
        let header_crc_offset = handle.offset as usize + 4 + 16 + 8 + 2 + 4 + 4;
        let mut raw = std::fs::read(&blob_file.0.path)?;
        raw[header_crc_offset] ^= 0xFF; // flip bits
        std::fs::write(&blob_file.0.path, &raw)?;

        let file = File::open(&blob_file.0.path)?;
        let reader = Reader::new(blob_file, &file);

        let result = reader.get(b"key", &handle);
        assert!(
            matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
            "expected HeaderCrcMismatch for corrupted header_crc field, got: {result:?}",
        );

        Ok(())
    }

    /// Verify V4 header layout: BLOB_HEADER_LEN_V4 = 42 bytes
    /// (magic:4 + checksum:16 + seqno:8 + key_len:2 + real_val_len:4 + on_disk_val_len:4 + header_crc:4).
    #[test]
    fn blob_header_len_v4_is_42() {
        assert_eq!(BLOB_HEADER_LEN_V4, 42);
        assert_eq!(BLOB_HEADER_LEN_V3, 38);
    }

    /// Write a V3 blob file manually and verify the reader handles it
    /// via the V3 backward compat path (no header_crc validation).
    #[test]
    fn blob_reader_v3_backward_compat_roundtrip() -> crate::Result<()> {
        use crate::file_accessor::FileAccessor;
        use crate::vlog::{blob_file::Inner as BlobFileInner, ValueHandle};
        use byteorder::WriteBytesExt;
        use std::io::Write;
        use std::sync::{atomic::AtomicBool, Arc};

        let folder = tempfile::tempdir()?;
        let blob_file_path = folder.path().join("0");

        let key = b"abc";
        let value = b"hello_v3";

        // V3 data checksum: xxh3_128(key + value) — no header_crc
        let checksum = {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            hasher.update(key);
            hasher.update(value);
            hasher.digest128()
        };

        // Write V3 blob file manually using sfa framing
        {
            let file = std::fs::File::create(&blob_file_path)?;
            let mut sfa_writer = sfa::Writer::from_writer(file);
            sfa_writer.start("data")?;

            // V3 frame: BLOB magic, no header_crc
            sfa_writer.write_all(b"BLOB")?;
            sfa_writer.write_u128::<byteorder::LittleEndian>(checksum)?;
            sfa_writer.write_u64::<byteorder::LittleEndian>(42)?; // seqno
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test key length fits in u16"
            )]
            sfa_writer.write_u16::<byteorder::LittleEndian>(key.len() as u16)?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test value length fits in u32"
            )]
            sfa_writer.write_u32::<byteorder::LittleEndian>(value.len() as u32)?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test value length fits in u32"
            )]
            sfa_writer.write_u32::<byteorder::LittleEndian>(value.len() as u32)?;
            sfa_writer.write_all(key)?;
            sfa_writer.write_all(value)?;

            // Write metadata
            sfa_writer.start("meta")?;
            let metadata = crate::vlog::blob_file::meta::Metadata {
                id: 0,
                version: 3,
                created_at: 0,
                item_count: 1,
                total_compressed_bytes: value.len() as u64,
                total_uncompressed_bytes: value.len() as u64,
                key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
                compression: CompressionType::None,
            };
            metadata.encode_into(&mut sfa_writer)?;
            let mut inner = sfa_writer.into_inner()?;
            inner.sync_all()?;
        }

        // Construct a BlobFile with V3 metadata for the reader
        let file = File::open(&blob_file_path)?;
        let file2 = File::open(&blob_file_path)?;
        let blob_file = crate::BlobFile(Arc::new(BlobFileInner {
            id: 0,
            tree_id: 0,
            path: blob_file_path,
            meta: crate::vlog::blob_file::meta::Metadata {
                id: 0,
                version: 3,
                created_at: 0,
                item_count: 1,
                total_compressed_bytes: value.len() as u64,
                total_uncompressed_bytes: value.len() as u64,
                key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
                compression: CompressionType::None,
            },
            is_deleted: AtomicBool::new(false),
            checksum: crate::Checksum::from_raw(0),
            file_accessor: FileAccessor::File(Arc::new(file2)),
        }));

        let reader = Reader::new(&blob_file, &file);

        // V3 frame offset: sfa "data" segment header comes first.
        // Find actual data start via sfa reader.
        let sfa_reader = sfa::Reader::new(&blob_file.0.path)?;
        let data_section = sfa_reader.toc().section(b"data").unwrap();
        let data_start = data_section.pos();

        let handle = ValueHandle {
            blob_file_id: 0,
            offset: data_start,
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test value length fits in u32"
            )]
            on_disk_size: value.len() as u32,
        };

        let result = reader.get(key, &handle)?;
        assert_eq!(result, value);

        Ok(())
    }
}
