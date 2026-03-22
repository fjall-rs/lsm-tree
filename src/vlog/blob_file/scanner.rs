// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

// Format constants live in writer (the format definition site).
// Extracting to a shared module is an upstream structural decision.
use super::writer::{validate_header_crc, BLOB_HEADER_MAGIC_V3, BLOB_HEADER_MAGIC_V4};
use crate::{
    vlog::{blob_file::meta::METADATA_HEADER_MAGIC, BlobFileId},
    Checksum, SeqNo, UserKey, UserValue,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read, Seek},
    path::Path,
};

/// Reads through a blob file in order
pub struct Scanner {
    pub(crate) blob_file_id: BlobFileId, // TODO: remove unused?
    inner: BufReader<File>,
    is_terminated: bool,
}

impl Scanner {
    /// Initializes a new blob file reader.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: AsRef<Path>>(path: P, blob_file_id: BlobFileId) -> crate::Result<Self> {
        let file_reader = BufReader::with_capacity(32_000, File::open(path)?);
        Ok(Self::with_reader(blob_file_id, file_reader))
    }

    /// Initializes a new blob file reader.
    #[must_use]
    pub fn with_reader(blob_file_id: BlobFileId, file_reader: BufReader<File>) -> Self {
        Self {
            blob_file_id,
            inner: file_reader,
            is_terminated: false,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ScanEntry {
    pub key: UserKey,
    pub seqno: SeqNo,
    pub value: UserValue,
    pub offset: u64,
    pub uncompressed_len: u32,
}

impl Iterator for Scanner {
    type Item = crate::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_terminated {
            return None;
        }

        let offset = fail_iter!(self.inner.stream_position());

        let frame_is_v4;

        {
            let mut buf = [0; BLOB_HEADER_MAGIC_V4.len()];
            fail_iter!(self.inner.read_exact(&mut buf));

            if buf == METADATA_HEADER_MAGIC {
                self.is_terminated = true;
                return None;
            }

            frame_is_v4 = buf == BLOB_HEADER_MAGIC_V4;
            if !frame_is_v4 && buf != BLOB_HEADER_MAGIC_V3 {
                return Some(Err(crate::Error::InvalidHeader("Blob")));
            }
        }

        let expected_checksum = fail_iter!(self.inner.read_u128::<LittleEndian>());
        let seqno = fail_iter!(self.inner.read_u64::<LittleEndian>());

        let key_len = fail_iter!(self.inner.read_u16::<LittleEndian>());

        let real_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        let on_disk_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        // V4: read and validate header CRC using shared validator.
        // On CRC failure, terminate the scanner so subsequent next() calls
        // don't read from a mid-frame stream position.
        let stored_header_crc = if frame_is_v4 {
            let crc = fail_iter!(self.inner.read_u32::<LittleEndian>());
            if let Err(e) = validate_header_crc(seqno, key_len, real_val_len, on_disk_val_len, crc)
            {
                self.is_terminated = true;
                return Some(Err(e));
            }
            Some(crc)
        } else {
            None
        };

        let key = fail_iter!(UserKey::from_reader(&mut self.inner, key_len as usize));

        let value = fail_iter!(UserValue::from_reader(
            &mut self.inner,
            on_disk_val_len as usize
        ));

        {
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(&key);
                hasher.update(&value);
                if let Some(hcrc) = stored_header_crc {
                    hasher.update(&hcrc.to_le_bytes());
                }
                hasher.digest128()
            };

            if expected_checksum != checksum {
                log::error!(
                    "Checksum mismatch for blob>{}@{offset}, got={checksum}, expected={expected_checksum}",
                    self.blob_file_id,
                );

                return Some(Err(crate::Error::ChecksumMismatch {
                    got: Checksum::from_raw(checksum),
                    expected: Checksum::from_raw(expected_checksum),
                }));
            }
        }

        Some(Ok(ScanEntry {
            key,
            seqno,
            value,
            offset,
            uncompressed_len: real_val_len,
        }))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{vlog::blob_file::writer::Writer as BlobFileWriter, Slice};
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn blob_scanner() -> crate::Result<()> {
        let dir = tempdir()?;
        let blob_file_path = dir.path().join("0");

        let keys = [b"a", b"b", b"c", b"d", b"e"];

        {
            let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0)?;

            for key in keys {
                writer.write(key, 0, &key.repeat(100))?;
            }

            writer.finish()?;
        }

        {
            let mut scanner = Scanner::new(&blob_file_path, 0)?;

            for key in keys {
                assert_eq!(
                    (Slice::from(key), Slice::from(key.repeat(100))),
                    scanner
                        .next()
                        .map(|result| result.map(|entry| { (entry.key, entry.value) }))
                        .unwrap()?,
                );
            }

            assert!(scanner.next().is_none());
        }

        Ok(())
    }

    /// Tamper seqno in first blob frame and verify scanner's V4 header
    /// CRC catches the corruption.
    #[test]
    fn blob_scanner_v4_corrupted_seqno_detected_by_header_crc() -> crate::Result<()> {
        use crate::vlog::blob_file::writer::BLOB_HEADER_MAGIC_V4;

        let dir = tempdir()?;
        let blob_file_path = dir.path().join("0");

        {
            let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0)?;
            writer.write(b"key", 42, &b"v".repeat(100))?;
            writer.finish()?;
        }

        // BlobFileWriter writes the first frame at file offset 0
        // (sfa has no inline section headers), so use deterministic offset.
        let mut raw = std::fs::read(&blob_file_path)?;
        let frame_start = 0usize;

        // Tamper seqno: header layout is [magic][checksum][seqno]...
        let seqno_offset = frame_start + BLOB_HEADER_MAGIC_V4.len() + std::mem::size_of::<u128>();
        let seqno_len = std::mem::size_of::<u64>();
        raw[seqno_offset..seqno_offset + seqno_len]
            .copy_from_slice(&99u64.to_le_bytes()[..seqno_len]);
        std::fs::write(&blob_file_path, &raw)?;

        let mut scanner = Scanner::new(&blob_file_path, 0)?;
        let result = scanner.next().unwrap();
        assert!(
            matches!(result, Err(crate::Error::HeaderCrcMismatch { .. })),
            "expected HeaderCrcMismatch for corrupted seqno, got: {result:?}",
        );

        Ok(())
    }

    /// Tamper value payload in blob frame and verify scanner's data
    /// checksum catches the corruption.
    #[test]
    fn blob_scanner_corrupted_value_detected_by_data_checksum() -> crate::Result<()> {
        use crate::vlog::blob_file::writer::BLOB_HEADER_LEN_V4;

        let dir = tempdir()?;
        let blob_file_path = dir.path().join("0");

        {
            let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0)?;
            writer.write(b"key", 0, &b"v".repeat(100))?;
            writer.finish()?;
        }

        // BlobFileWriter writes the first frame at file offset 0
        // (sfa has no inline section headers), so use deterministic offset.
        let mut raw = std::fs::read(&blob_file_path)?;
        let frame_start = 0usize;

        // Tamper value payload: frame_start + header + key
        let key = b"key";
        let value_offset = frame_start + BLOB_HEADER_LEN_V4 + key.len();
        raw[value_offset] ^= 0xFF;
        std::fs::write(&blob_file_path, &raw)?;

        let mut scanner = Scanner::new(&blob_file_path, 0)?;
        let result = scanner.next().unwrap();
        assert!(
            matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
            "expected ChecksumMismatch for corrupted value, got: {result:?}",
        );

        Ok(())
    }

    /// Write a V3 blob file (b"BLOB" magic, no header_crc) manually,
    /// then verify the scanner can read it with V3 backward compat path.
    #[test]
    fn blob_scanner_reads_v3_format() -> crate::Result<()> {
        use byteorder::{LittleEndian, WriteBytesExt};
        use std::io::Write;

        let dir = tempdir()?;
        let blob_file_path = dir.path().join("0");

        let key = b"abc";
        let value = b"hello_v3";

        // V3 data checksum: xxh3_128(key + value) — no header_crc
        let checksum = {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            hasher.update(key);
            hasher.update(value);
            hasher.digest128()
        };

        // Manually write V3 blob file using sfa framing
        {
            let file = std::fs::File::create(&blob_file_path)?;
            let mut sfa_writer = sfa::Writer::from_writer(file);
            sfa_writer.start("data")?;

            // V3 frame: BLOB magic, no header_crc
            sfa_writer.write_all(b"BLOB")?;
            sfa_writer.write_u128::<LittleEndian>(checksum)?;
            sfa_writer.write_u64::<LittleEndian>(42)?; // seqno
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test key length fits in u16"
            )]
            sfa_writer.write_u16::<LittleEndian>(key.len() as u16)?;
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test value length fits in u32"
            )]
            sfa_writer.write_u32::<LittleEndian>(value.len() as u32)?; // real_val_len
            #[expect(
                clippy::cast_possible_truncation,
                reason = "test value length fits in u32"
            )]
            sfa_writer.write_u32::<LittleEndian>(value.len() as u32)?; // on_disk_val_len
            sfa_writer.write_all(key)?;
            sfa_writer.write_all(value)?;

            // Write metadata section
            sfa_writer.start("meta")?;
            let metadata = crate::vlog::blob_file::meta::Metadata {
                id: 0,
                version: 3,
                created_at: 0,
                item_count: 1,
                total_compressed_bytes: value.len() as u64,
                total_uncompressed_bytes: value.len() as u64,
                key_range: crate::KeyRange::new((key[..].into(), key[..].into())),
                compression: crate::CompressionType::None,
            };
            metadata.encode_into(&mut sfa_writer)?;
            let mut inner = sfa_writer.into_inner()?;
            inner.sync_all()?;
        }

        // Scanner should read the V3 frame successfully
        let mut scanner = Scanner::new(&blob_file_path, 0)?;
        let entry = scanner.next().unwrap()?;
        assert_eq!(entry.key, Slice::from(&key[..]));
        assert_eq!(entry.value, Slice::from(&value[..]));
        assert_eq!(entry.seqno, 42);
        assert!(scanner.next().is_none());

        Ok(())
    }

    /// Scanner rejects frames with invalid magic (neither V3 nor V4).
    #[test]
    fn blob_scanner_rejects_invalid_magic() -> crate::Result<()> {
        let dir = tempdir()?;
        let blob_file_path = dir.path().join("0");

        {
            let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0)?;
            writer.write(b"key", 0, b"value")?;
            writer.finish()?;
        }

        // Corrupt magic bytes at offset 0 (start of first frame).
        let mut raw = std::fs::read(&blob_file_path)?;
        // First frame starts at offset 0 because sfa has no inline headers.
        raw[0..4].copy_from_slice(b"XXXX");
        std::fs::write(&blob_file_path, &raw)?;

        let mut scanner = Scanner::new(&blob_file_path, 0)?;
        let result = scanner.next().unwrap();
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("Blob"))),
            "expected InvalidHeader for bad magic, got: {result:?}",
        );

        Ok(())
    }
}
