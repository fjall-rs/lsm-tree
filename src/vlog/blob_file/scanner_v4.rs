// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::Decode,
    vlog::{
        blob_file::{header::BlobHeader, scanner::ScanEntry},
        BlobFileId,
    },
    Checksum, UserKey, UserValue,
};
use std::{
    fs::File,
    io::{BufReader, Seek},
    path::Path,
};

/// Reads through a blob file in order
pub struct Scanner {
    pub(crate) blob_file_id: BlobFileId, // TODO: remove unused?
    inner: BufReader<File>,
    blob_count: usize,
}

impl Scanner {
    /// Initializes a new blob file reader.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: AsRef<Path>>(
        path: P,
        blob_file_id: BlobFileId,
        blob_count: usize,
    ) -> crate::Result<Self> {
        let file_reader = BufReader::with_capacity(32_000, File::open(path)?);
        Ok(Self::with_reader(blob_file_id, file_reader, blob_count))
    }

    /// Initializes a new blob file reader.
    #[must_use]
    pub fn with_reader(
        blob_file_id: BlobFileId,
        file_reader: BufReader<File>,
        blob_count: usize,
    ) -> Self {
        Self {
            blob_file_id,
            inner: file_reader,
            blob_count,
        }
    }
}

impl Iterator for Scanner {
    type Item = crate::Result<ScanEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.blob_count == 0 {
            return None;
        }

        let offset = fail_iter!(self.inner.stream_position());

        let header = fail_iter!(BlobHeader::decode_from(&mut self.inner));

        let key = fail_iter!(UserKey::from_reader(
            &mut self.inner,
            header.key_len as usize
        ));

        let value = fail_iter!(UserValue::from_reader(
            &mut self.inner,
            header.on_disk_len as usize
        ));

        {
            let checksum = {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                hasher.update(&key);
                hasher.update(&value);
                hasher.digest128()
            };

            let expected_checksum = header.data_checksum;

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

        self.blob_count -= 1;

        Some(Ok(ScanEntry {
            key,
            seqno: header.seqno,
            value,
            offset,
            uncompressed_len: header.uncompressed_len,
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
            let mut scanner = Scanner::new(&blob_file_path, 0, keys.len())?;

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
