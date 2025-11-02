// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::BLOB_HEADER_MAGIC;
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

#[derive(Debug)]
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

        {
            let mut buf = [0; BLOB_HEADER_MAGIC.len()];
            fail_iter!(self.inner.read_exact(&mut buf));

            if buf == METADATA_HEADER_MAGIC {
                self.is_terminated = true;
                return None;
            }

            if buf != BLOB_HEADER_MAGIC {
                return Some(Err(crate::Error::InvalidHeader("Blob")));
            }
        }

        let expected_checksum = fail_iter!(self.inner.read_u128::<LittleEndian>());
        let seqno = fail_iter!(self.inner.read_u64::<LittleEndian>());

        let key_len = fail_iter!(self.inner.read_u16::<LittleEndian>());

        let real_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

        let on_disk_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());

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
            let mut writer = BlobFileWriter::new(&blob_file_path, 0)?;

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
}
