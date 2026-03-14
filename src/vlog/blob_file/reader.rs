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

/// Maximum allowed size for a blob *value payload* after decompression
/// (256 MiB). The actual on-disk read may include additional header and key
/// bytes on top of this payload limit.
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

        // NOTE: key is caller-provided (not read from disk), so key.len() is trusted.
        // The writer already enforces key.len() <= u16::MAX.
        let add_size = (BLOB_HEADER_LEN as u64) + (key.len() as u64);

        // Validate the full on-disk read size (header + key + value) against the limit.
        // Allow header+key overhead on top of the data cap.
        let max_total_read_size = (MAX_DECOMPRESSION_SIZE as u64).saturating_add(add_size);

        let total_read_size = match u64::from(vhandle.on_disk_size).checked_add(add_size) {
            Some(size) => size,
            None => {
                let attempted = u64::from(vhandle.on_disk_size).saturating_add(add_size);
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: attempted,
                    limit: max_total_read_size,
                });
            }
        };

        if total_read_size > max_total_read_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: total_read_size,
                limit: max_total_read_size,
            });
        }

        let Ok(total_read_usize) = usize::try_from(total_read_size) else {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: total_read_size,
                limit: usize::MAX as u64,
            });
        };

        let value = crate::file::read_exact(self.file, vhandle.offset, total_read_usize)?;

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

        let raw_data = value.slice((add_size as usize)..);

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
                if real_val_len > MAX_DECOMPRESSION_SIZE {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: real_val_len as u64,
                        limit: MAX_DECOMPRESSION_SIZE as u64,
                    });
                }

                #[warn(unsafe_code)]
                let mut builder = unsafe { UserValue::builder_unzeroed(real_val_len) };

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
}
