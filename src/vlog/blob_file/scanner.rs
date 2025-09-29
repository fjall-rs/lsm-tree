// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{meta::METADATA_HEADER_MAGIC, writer::BLOB_HEADER_MAGIC};
use crate::{coding::DecodeError, vlog::BlobFileId, Checksum, CompressionType, UserKey, UserValue};
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

/// Reads through a blob file in order
pub struct Scanner {
    pub(crate) blob_file_id: BlobFileId, // TODO: remove unused?
    inner: BufReader<File>,
    is_terminated: bool,
    compression: CompressionType,
}

impl Scanner {
    /// Initializes a new blob file reader.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn new<P: AsRef<Path>>(path: P, blob_file_id: BlobFileId) -> crate::Result<Self> {
        let file_reader = BufReader::new(File::open(path)?);
        Ok(Self::with_reader(blob_file_id, file_reader))
    }

    /// Initializes a new blob file reader.
    #[must_use]
    pub fn with_reader(blob_file_id: BlobFileId, file_reader: BufReader<File>) -> Self {
        Self {
            blob_file_id,
            inner: file_reader,
            is_terminated: false,
            compression: CompressionType::None,
        }
    }

    pub(crate) fn use_compression(mut self, compressoion: CompressionType) -> Self {
        self.compression = compressoion;
        self
    }

    // pub(crate) fn get_offset(&mut self) -> std::io::Result<u64> {
    //     self.inner.stream_position()
    // }

    // pub(crate) fn into_inner(self) -> BufReader<File> {
    //     self.inner
    // }
}

impl Iterator for Scanner {
    type Item = crate::Result<(UserKey, UserValue, Checksum)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_terminated {
            return None;
        }

        {
            let mut buf = [0; BLOB_HEADER_MAGIC.len()];
            fail_iter!(self.inner.read_exact(&mut buf));

            if buf == METADATA_HEADER_MAGIC {
                self.is_terminated = true;
                return None;
            }

            if buf != BLOB_HEADER_MAGIC {
                return Some(Err(crate::Error::Decode(DecodeError::InvalidHeader(
                    "Blob",
                ))));
            }
        }

        let checksum = fail_iter!(self.inner.read_u128::<BigEndian>());

        let key_len = fail_iter!(self.inner.read_u16::<BigEndian>());
        let real_val_len = fail_iter!(self.inner.read_u32::<BigEndian>());
        let on_disk_val_len = fail_iter!(self.inner.read_u32::<BigEndian>());

        let key = fail_iter!(UserKey::from_reader(&mut self.inner, key_len as usize));

        // TODO: finish compression
        #[warn(clippy::match_single_binding)]
        let val = match &self.compression {
            _ => {
                fail_iter!(UserValue::from_reader(
                    &mut self.inner,
                    on_disk_val_len as usize
                ))
            }
        };
        // Some(compressor) => {
        //     // TODO: https://github.com/PSeitz/lz4_flex/issues/166
        //     let mut val = vec![0; val_len as usize];
        //     fail_iter!(self.inner.read_exact(&mut val));
        //     UserValue::from(fail_iter!(compressor.decompress(&val)))
        // }
        // None => {

        // }

        Some(Ok((key, val, Checksum::from_raw(checksum))))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{vlog::blob_file::writer::Writer as BlobFileWriter, Slice};
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn blob_file_scanner() -> crate::Result<()> {
        let dir = tempdir()?;
        let blob_file_path = dir.path().join("0");

        let keys = [b"a", b"b", b"c", b"d", b"e"];

        {
            let mut writer = BlobFileWriter::new(&blob_file_path, 0)?;

            for key in keys {
                writer.write(key, &key.repeat(100))?;
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
                        .map(|result| result.map(|(k, v, _)| { (k, v) }))
                        .unwrap()?,
                );
            }

            assert!(scanner.next().is_none());
        }

        Ok(())
    }
}
