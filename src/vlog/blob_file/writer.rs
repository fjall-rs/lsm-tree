// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::meta::Metadata;
use crate::{
    checksum::ChecksummedWriter, time::unix_timestamp, vlog::BlobFileId, Checksum, CompressionType,
    KeyRange, SeqNo, TreeId, UserKey,
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

/// Safety cap on blob value size (256 MiB).
///
/// Enforced on the write path to prevent producing blobs that are
/// unreasonably large. The reader applies its own copy of this limit.
///
/// NOTE: Intentionally duplicated in `table::block` (as `u32`) and
/// `vlog::blob_file::reader` rather than shared, because blocks and
/// blobs are independent storage formats that may diverge in the future.
const MAX_DECOMPRESSION_SIZE: usize = 256 * 1024 * 1024;

/// Returns `Err(DecompressedSizeTooLarge)` if `len > MAX_DECOMPRESSION_SIZE`.
fn check_size_cap(len: usize) -> crate::Result<()> {
    if len > MAX_DECOMPRESSION_SIZE {
        return Err(crate::Error::DecompressedSizeTooLarge {
            declared: len as u64,
            limit: MAX_DECOMPRESSION_SIZE as u64,
        });
    }
    Ok(())
}

// Note: these constants are `pub` for crate-internal use but the parent
// `vlog` module is NOT exported from `lib.rs`, so they are not public API.

/// V3 blob frame magic (no header checksum).
pub const BLOB_HEADER_MAGIC_V3: &[u8] = b"BLOB";

/// V4 blob frame magic (includes header checksum).
pub const BLOB_HEADER_MAGIC_V4: &[u8] = b"BLO4";

/// V3 blob frame header length (38 bytes, no `header_crc`).
pub const BLOB_HEADER_LEN_V3: usize = BLOB_HEADER_MAGIC_V3.len()
    + std::mem::size_of::<u128>() // Checksum
    + std::mem::size_of::<u64>() // SeqNo
    + std::mem::size_of::<u16>() // Key length
    + std::mem::size_of::<u32>() // Real value length
    + std::mem::size_of::<u32>(); // On-disk value length

/// V4 blob frame header length (42 bytes, includes `header_crc`).
pub const BLOB_HEADER_LEN_V4: usize = BLOB_HEADER_LEN_V3 + std::mem::size_of::<u32>(); // Header CRC

/// Compute V4 header CRC from header fields.
/// Returns a 4-byte truncated xxh3 hash.
#[expect(
    clippy::cast_possible_truncation,
    reason = "intentionally truncated to 4-byte CRC"
)]
pub(super) fn compute_header_crc(
    seqno: u64,
    key_len: u16,
    real_val_len: u32,
    on_disk_val_len: u32,
) -> u32 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::default();
    hasher.update(&seqno.to_le_bytes());
    hasher.update(&key_len.to_le_bytes());
    hasher.update(&real_val_len.to_le_bytes());
    hasher.update(&on_disk_val_len.to_le_bytes());
    hasher.digest() as u32
}

/// Validate V4 header CRC: recompute from header fields and compare
/// against the stored value.
pub(super) fn validate_header_crc(
    seqno: u64,
    key_len: u16,
    real_val_len: u32,
    on_disk_val_len: u32,
    stored_crc: u32,
) -> crate::Result<()> {
    let recomputed_crc = compute_header_crc(seqno, key_len, real_val_len, on_disk_val_len);

    if stored_crc != recomputed_crc {
        return Err(crate::Error::HeaderCrcMismatch {
            recomputed: recomputed_crc,
            stored: stored_crc,
        });
    }

    Ok(())
}

/// Blob file writer
pub struct Writer {
    pub(crate) tree_id: TreeId,
    pub path: PathBuf,
    pub(crate) blob_file_id: BlobFileId,

    #[expect(clippy::struct_field_names)]
    writer: sfa::Writer<ChecksummedWriter<BufWriter<File>>>,

    offset: u64,

    pub(crate) item_count: u64,
    pub(crate) written_blob_bytes: u64,
    pub(crate) uncompressed_bytes: u64,

    pub(crate) first_key: Option<UserKey>,
    pub(crate) last_key: Option<UserKey>,

    pub(crate) compression: CompressionType,
}

impl Writer {
    /// Initializes a new blob file writer.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn new<P: AsRef<Path>>(
        path: P,
        blob_file_id: BlobFileId,
        tree_id: TreeId,
    ) -> crate::Result<Self> {
        let path = path.as_ref();

        let writer = BufWriter::new(File::create(path)?);
        let writer = ChecksummedWriter::new(writer);
        let mut writer = sfa::Writer::from_writer(writer);
        writer.start("data")?;

        Ok(Self {
            tree_id,
            path: path.into(),
            blob_file_id,

            writer,

            offset: 0,
            item_count: 0,
            written_blob_bytes: 0,
            uncompressed_bytes: 0,

            first_key: None,
            last_key: None,

            compression: CompressionType::None,
        })
    }

    pub fn use_compression(mut self, compressor: CompressionType) -> Self {
        self.compression = compressor;
        self
    }

    /// Returns the current offset in the file.
    #[must_use]
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the blob file ID.
    #[must_use]
    pub(crate) fn blob_file_id(&self) -> BlobFileId {
        self.blob_file_id
    }

    pub(crate) fn write_raw(
        &mut self,
        key: &[u8],
        seqno: SeqNo,
        value: &[u8],
        uncompressed_len: u32,
    ) -> crate::Result<u32> {
        assert!(!key.is_empty());
        assert!(u16::try_from(key.len()).is_ok());
        assert!(u32::try_from(value.len()).is_ok());

        check_size_cap(uncompressed_len as usize)?;
        check_size_cap(value.len())?;

        // Perform compression before mutating writer state, so an error
        // leaves the writer consistent. Post-compression output is also
        // checked against the cap (reuses DecompressedSizeTooLarge since
        // the cap applies uniformly to all blob data regardless of
        // compression state).
        let value = match &self.compression {
            CompressionType::None => std::borrow::Cow::Borrowed(value),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                let compressed = lz4_flex::compress(value);
                check_size_cap(compressed.len())?;
                std::borrow::Cow::Owned(compressed)
            }

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(level) => {
                let compressed =
                    zstd::bulk::compress(value, *level).map_err(std::io::Error::other)?;
                check_size_cap(compressed.len())?;
                std::borrow::Cow::Owned(compressed)
            }
        };

        // Ensure the compressed value length fits in u32 before we write it
        // to disk as a 32-bit length. This prevents truncation if compression
        // expands the payload (possible for incompressible data near u32 boundary).
        let compressed_len_u32 = u32::try_from(value.len())
            .map_err(|_| std::io::Error::other("compressed value length exceeds u32::MAX"))?;

        if self.first_key.is_none() {
            self.first_key = Some(key.into());
        }
        self.last_key = Some(key.into());

        self.uncompressed_bytes += u64::from(uncompressed_len);

        // NOTE:
        // V4 BLOB HEADER LAYOUT
        //
        // [MAGIC_BYTES; 4B]    - b"BLO4"
        // [Checksum; 16B]      - xxh3_128(key + value + header_crc_le)
        // [Seqno; 8B]
        // [key len; 2B]
        // [real val len; 4B]
        // [on-disk val len; 4B]
        // [header_crc; 4B]     - truncated xxh3(seqno + key_len + real_val_len + on_disk_val_len)
        // [...key; ?]
        // [...val; ?]

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        let header_crc = compute_header_crc(
            seqno,
            key.len() as u16,
            uncompressed_len,
            compressed_len_u32,
        );

        // Data checksum includes header_crc bytes so that changes to header
        // fields without correspondingly updating the data checksum will be
        // detected as an inconsistency between header and data.
        let checksum = {
            let mut hasher = xxhash_rust::xxh3::Xxh3::default();
            hasher.update(key);
            hasher.update(&value);
            hasher.update(&header_crc.to_le_bytes());
            hasher.digest128()
        };

        // Write header
        self.writer.write_all(BLOB_HEADER_MAGIC_V4)?;

        // Write data checksum
        self.writer.write_u128::<LittleEndian>(checksum)?;

        // Write seqno
        self.writer.write_u64::<LittleEndian>(seqno)?;

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        self.writer.write_u16::<LittleEndian>(key.len() as u16)?;

        // Write uncompressed value length
        self.writer.write_u32::<LittleEndian>(uncompressed_len)?;

        // Write compressed (on-disk) value length
        self.writer.write_u32::<LittleEndian>(compressed_len_u32)?;

        // Write header CRC
        self.writer.write_u32::<LittleEndian>(header_crc)?;

        self.writer.write_all(key)?;
        self.writer.write_all(&value)?;

        // Update offset
        self.offset += BLOB_HEADER_LEN_V4 as u64;
        self.offset += key.len() as u64;
        self.offset += value.len() as u64;

        // Update metadata
        self.written_blob_bytes += value.len() as u64;
        self.item_count += 1;

        // TODO: if we store the offset before writing, we can return a vhandle here
        // instead of needing to call offset() and blob_file_id() before write()

        Ok(compressed_len_u32)
    }

    /// Writes an item into the file.
    ///
    /// Items need to be written in key order.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// Will return `Err(Error::DecompressedSizeTooLarge { .. })` if the value exceeds the 256 MiB limit.
    ///
    /// # Panics
    ///
    /// Panics if the key length is empty or greater than 2^16, or the value length is greater than 2^32.
    pub fn write(&mut self, key: &[u8], seqno: SeqNo, value: &[u8]) -> crate::Result<u32> {
        #[expect(clippy::cast_possible_truncation, reason = "values are u32 max")]
        self.write_raw(key, seqno, value, value.len() as u32)
    }

    pub(crate) fn finish(mut self) -> crate::Result<(Metadata, Checksum)> {
        self.writer.start("meta")?;

        // Write metadata
        let metadata = Metadata {
            id: self.blob_file_id,
            version: 4,
            created_at: unix_timestamp().as_nanos(),
            item_count: self.item_count,
            total_compressed_bytes: self.written_blob_bytes,
            total_uncompressed_bytes: self.uncompressed_bytes,
            #[expect(clippy::expect_used, reason = "should have written at least 1 item")]
            key_range: KeyRange::new((
                self.first_key
                    .clone()
                    .expect("should have written at least 1 item"),
                self.last_key
                    .clone()
                    .expect("should have written at least 1 item"),
            )),
            compression: self.compression,
        };
        metadata.encode_into(&mut self.writer)?;

        let mut checksum = self.writer.into_inner()?;
        checksum.inner_mut().get_mut().sync_all()?;
        let checksum = checksum.checksum();

        Ok((metadata, checksum))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_write_rejects_oversized_value() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("test.blob");
        let mut writer = Writer::new(&path, 0, 0)?;

        let oversize = MAX_DECOMPRESSION_SIZE as u32 + 1;
        let result = writer.write_raw(b"key", 0, b"small-on-disk", oversize);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
        Ok(())
    }

    #[test]
    fn blob_write_accepts_max_size_value() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("test.blob");
        let mut writer = Writer::new(&path, 0, 0)?;

        let at_limit = MAX_DECOMPRESSION_SIZE as u32;
        let result = writer.write_raw(b"key", 0, b"small-on-disk", at_limit);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        Ok(())
    }

    #[test]
    fn blob_write_rejects_oversized_value_none_compression() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("test.blob");
        let mut writer = Writer::new(&path, 0, 0)?;

        let oversize_value = vec![0u8; MAX_DECOMPRESSION_SIZE + 1];
        let result = writer.write_raw(b"key", 0, &oversize_value, MAX_DECOMPRESSION_SIZE as u32);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn blob_write_lz4_accepts_small_value() -> crate::Result<()> {
        let folder = tempfile::tempdir()?;
        let path = folder.path().join("test.blob");
        let mut writer = Writer::new(&path, 0, 0)?.use_compression(CompressionType::Lz4);

        // Exercise the LZ4 compression arm with a value that passes
        // the pre-compression check and compresses successfully.
        let value = b"hello world lz4 test data";
        #[expect(clippy::cast_possible_truncation, reason = "test value is 25 bytes")]
        let result = writer.write_raw(b"key", 0, value, value.len() as u32);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        Ok(())
    }

    #[test]
    fn check_size_cap_rejects_over_limit() {
        let result = super::check_size_cap(MAX_DECOMPRESSION_SIZE + 1);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
    }

    #[test]
    fn check_size_cap_accepts_at_limit() {
        assert!(super::check_size_cap(MAX_DECOMPRESSION_SIZE).is_ok());
        assert!(super::check_size_cap(0).is_ok());
    }
}
