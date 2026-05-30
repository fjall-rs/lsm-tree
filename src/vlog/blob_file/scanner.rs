// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::BLOB_HEADER_MAGIC;
use crate::{
    direct_io::ChunkedReader,
    vlog::{blob_file::meta::METADATA_HEADER_MAGIC, BlobFileId},
    Checksum, SeqNo, UserKey, UserValue,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{io::Read, path::Path};

/// Reads through a blob file in order
pub struct Scanner {
    pub(crate) blob_file_id: BlobFileId, // TODO: remove unused?
    inner: ChunkedReader,
    is_terminated: bool,
    /// Logical byte offset within the file (manually tracked because `ChunkedReader`
    /// does not implement `Seek`). Used to record each scan entry's offset.
    pos: u64,
}

impl Scanner {
    /// Initializes a new blob file reader. When `use_direct_io` is `true` the
    /// underlying file is opened with platform direct I/O — what the compaction
    /// worker passes when [`crate::Config::use_direct_io_for_compaction_reads`]
    /// is on during blob relocation.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: AsRef<Path>>(
        path: P,
        blob_file_id: BlobFileId,
        use_direct_io: bool,
    ) -> crate::Result<Self> {
        let reader = ChunkedReader::open(path.as_ref(), use_direct_io)?;
        Ok(Self {
            blob_file_id,
            inner: reader,
            is_terminated: false,
            pos: 0,
        })
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

        // Track bytes consumed locally; commit to `self.pos` only after the entire
        // entry parses cleanly. Partial increments on a parse failure would leave
        // `self.pos` inconsistent with the kernel's file position and break the
        // recorded `offset` of any future entry (mostly defensive — callers do not
        // iterate past errors today).
        let offset = self.pos;
        let mut delta: u64 = 0;

        {
            let mut buf = [0; BLOB_HEADER_MAGIC.len()];
            fail_iter!(self.inner.read_exact(&mut buf));
            delta += BLOB_HEADER_MAGIC.len() as u64;

            if buf == METADATA_HEADER_MAGIC {
                self.is_terminated = true;
                self.pos += delta;
                return None;
            }

            if buf != BLOB_HEADER_MAGIC {
                self.pos += delta;
                return Some(Err(crate::Error::InvalidHeader("Blob")));
            }
        }

        let expected_checksum = fail_iter!(self.inner.read_u128::<LittleEndian>());
        delta += std::mem::size_of::<u128>() as u64;

        let seqno = fail_iter!(self.inner.read_u64::<LittleEndian>());
        delta += std::mem::size_of::<u64>() as u64;

        let key_len = fail_iter!(self.inner.read_u16::<LittleEndian>());
        delta += std::mem::size_of::<u16>() as u64;

        let real_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());
        delta += std::mem::size_of::<u32>() as u64;

        let on_disk_val_len = fail_iter!(self.inner.read_u32::<LittleEndian>());
        delta += std::mem::size_of::<u32>() as u64;

        let key = fail_iter!(UserKey::from_reader(&mut self.inner, key_len as usize));
        delta += u64::from(key_len);

        let value = fail_iter!(UserValue::from_reader(
            &mut self.inner,
            on_disk_val_len as usize
        ));
        delta += u64::from(on_disk_val_len);

        self.pos += delta;

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
            let mut writer = BlobFileWriter::new(&blob_file_path, 0, 0, false)?;

            for key in keys {
                writer.write(key, 0, &key.repeat(100))?;
            }

            writer.finish()?;
        }

        {
            let mut scanner = Scanner::new(&blob_file_path, 0, false)?;

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
