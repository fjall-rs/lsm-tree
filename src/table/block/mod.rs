// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod binary_index;
pub mod decoder;
mod encoder;
pub mod hash_index;
mod header;
mod offset;
mod trailer;
mod r#type;

pub(crate) use decoder::{Decodable, Decoder, ParsedItem};
pub(crate) use encoder::{Encodable, Encoder};
pub use header::Header;
pub use offset::BlockOffset;
pub use r#type::BlockType;
pub(crate) use trailer::{Trailer, TRAILER_START_MARKER};

use crate::{
    coding::{Decode, Encode},
    encryption::EncryptionProvider,
    fs::FsFile,
    table::BlockHandle,
    Checksum, CompressionType, Slice,
};

/// Safety cap on block payload size (256 MiB).
///
/// Enforced on both write and read paths to prevent producing or accepting
/// blocks that are unreasonably large. Intentionally stricter than the
/// on-disk format limit (`u32::MAX`) to guard against decompression bombs
/// and OOM from crafted/malicious SST files.
///
/// NOTE: Intentionally duplicated in `vlog::blob_file` (writer as `usize`,
/// reader as `usize`) rather than shared, because blocks and blobs are
/// independent storage formats that may diverge in the future.
const MAX_DECOMPRESSION_SIZE: u32 = 256 * 1024 * 1024;

/// A block on disk
///
/// Consists of a fixed-size header and some bytes (the data/payload).
#[derive(Clone)]
pub struct Block {
    pub header: Header,
    pub data: Slice,
}

impl Block {
    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Encodes a block into a writer.
    ///
    /// Pipeline: raw data → compress → encrypt → checksum → write.
    /// When `encryption` is `None`, the encrypt step is skipped.
    pub fn write_into<W: std::io::Write>(
        mut writer: &mut W,
        data: &[u8],
        block_type: BlockType,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
    ) -> crate::Result<Header> {
        if data.len() > MAX_DECOMPRESSION_SIZE as usize {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: data.len() as u64,
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        let mut header = Header {
            block_type,
            checksum: Checksum::from_raw(0), // <-- NOTE: Is set later on
            data_length: 0,                  // <-- NOTE: Is set later on

            #[expect(clippy::cast_possible_truncation, reason = "blocks are limited to u32")]
            uncompressed_length: data.len() as u32,
        };

        // `compressed_buf` keeps the compressed data alive so `payload` can borrow it.
        // NOTE: Uses Option<Vec<u8>> (not Cow) to match upstream's lz4 pattern and
        // minimize merge conflict surface. Only declared when a compression feature
        // is enabled; the match arms always initialize it before use.
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        let compressed_buf: Option<Vec<u8>>;

        let payload: &[u8] = match compression {
            CompressionType::None => data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                compressed_buf = Some(lz4_flex::compress(data));

                #[expect(clippy::expect_used, reason = "compressed_buf was just assigned")]
                compressed_buf.as_ref().expect("just assigned")
            }

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(level) => {
                compressed_buf = Some(
                    zstd::bulk::compress(data, level)
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?,
                );

                #[expect(clippy::expect_used, reason = "compressed_buf was just assigned")]
                compressed_buf.as_ref().expect("just assigned")
            }
        };

        // Encrypt the compressed payload if an encryption provider is configured.
        // The encrypted bytes replace the compressed bytes on disk; checksums
        // cover the encrypted form so corruption is detected before decryption.
        let encrypted_buf = encryption.map(|enc| enc.encrypt(payload)).transpose()?;
        let payload: &[u8] = encrypted_buf.as_deref().unwrap_or(payload);

        // Validate the final on-disk payload against the same size limit
        // enforced on the read path (MAX_DECOMPRESSION_SIZE + encryption overhead).
        // Check in u64 first to produce the correct DecompressedSizeTooLarge error,
        // then narrow to u32 for the header field.
        //
        // NOTE: max_overhead() is used only for the LIMIT — the actual ciphertext
        // length is checked against it regardless. A buggy provider that expands
        // beyond max_overhead() will be caught by this check (payload > limit).
        // Cap at u32::MAX to guarantee the subsequent as-u32 cast is safe.
        let max_payload = (u64::from(MAX_DECOMPRESSION_SIZE)
            + encryption.map_or(0u64, |enc| u64::from(enc.max_overhead())))
        .min(u64::from(u32::MAX));

        if payload.len() as u64 > max_payload {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: payload.len() as u64,
                limit: max_payload,
            });
        }

        // Safe: payload.len() <= max_payload <= MAX_DECOMPRESSION_SIZE + overhead,
        // which is well within u32 range.
        #[expect(clippy::cast_possible_truncation, reason = "bounded by check above")]
        let payload_len = payload.len() as u32;

        header.data_length = payload_len;
        header.checksum = Checksum::from_raw(crate::hash::hash128(payload));

        header.encode_into(&mut writer)?;
        writer.write_all(payload)?;

        log::trace!(
            "Writing block with size {}B (on-disk: {}B) (excluding header of {}B)",
            header.uncompressed_length,
            header.data_length,
            Header::serialized_len(),
        );

        Ok(header)
    }

    /// Reads a block from a reader.
    ///
    /// Pipeline: read → verify checksum → decrypt → decompress.
    /// When `encryption` is `None`, the decrypt step is skipped.
    ///
    /// Encryption state is determined by the caller (via [`Config`]),
    /// not recorded in the on-disk block header. With an authenticated
    /// encryption provider (such as AES-256-GCM), using the wrong key
    /// or provider will typically surface as a read/validation error
    /// (checksum, length, or decompression failure) rather than
    /// silently producing valid-looking plaintext.
    pub fn from_reader<R: std::io::Read>(
        reader: &mut R,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
    ) -> crate::Result<Self> {
        let header = Header::decode_from(reader)?;

        // Validate both size fields before any I/O or hashing to fail fast
        // on malformed headers. The on-disk data_length may include encryption
        // overhead (nonce + auth tag), so allow the provider's declared margin.
        // Use u64 arithmetic to avoid any possibility of u32 overflow
        // (consistent with from_file).
        let enc_overhead = encryption.map_or(0u64, |e| u64::from(e.max_overhead()));
        let max_data_length = u64::from(MAX_DECOMPRESSION_SIZE) + enc_overhead;

        if u64::from(header.data_length) > max_data_length {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.data_length),
                limit: max_data_length,
            });
        }

        if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.uncompressed_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        let raw_data = Slice::from_reader(reader, header.data_length as usize)?;

        let checksum = Checksum::from_raw(crate::hash::hash128(&raw_data));

        checksum.check(header.checksum).inspect_err(|_| {
            log::error!(
                "Checksum mismatch for <bufreader>, got={}, expected={}",
                checksum,
                header.checksum,
            );
        })?;

        // Decrypt the on-disk bytes before decompression.
        let decrypted = encryption.map(|enc| enc.decrypt(&raw_data)).transpose()?;
        let compressed_data: &[u8] = decrypted.as_deref().unwrap_or(&raw_data);

        let data = match compression {
            CompressionType::None => {
                #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
                let actual_len = compressed_data.len() as u32;

                if header.uncompressed_length != actual_len {
                    return Err(crate::Error::InvalidHeader("Block"));
                }

                if let Some(plain) = decrypted {
                    Slice::from(plain)
                } else {
                    raw_data
                }
            }

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                let mut buf = vec![0u8; header.uncompressed_length as usize];

                let bytes_written = lz4_flex::decompress_into(compressed_data, &mut buf)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != header.uncompressed_length as usize {
                    return Err(crate::Error::Decompress(compression));
                }

                Slice::from(buf)
            }

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(_) => {
                let decompressed =
                    zstd::bulk::decompress(compressed_data, header.uncompressed_length as usize)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                if decompressed.len() != header.uncompressed_length as usize {
                    return Err(crate::Error::Decompress(compression));
                }

                Slice::from(decompressed)
            }
        };

        Ok(Self { header, data })
    }

    /// Reads a block from a file.
    ///
    /// Pipeline: read → verify checksum → decrypt → decompress.
    /// When `encryption` is `None`, the decrypt step is skipped.
    pub fn from_file(
        file: &impl FsFile,
        handle: BlockHandle,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
    ) -> crate::Result<Self> {
        // handle.size() includes Header::serialized_len(), so allow that overhead.
        // Encrypted blocks add provider-specific overhead to the on-disk size.
        let enc_overhead = encryption.map_or(0u64, |e| u64::from(e.max_overhead()));
        let max_on_disk_size =
            u64::from(MAX_DECOMPRESSION_SIZE) + Header::serialized_len() as u64 + enc_overhead;

        if u64::from(handle.size()) > max_on_disk_size {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(handle.size()),
                limit: max_on_disk_size,
            });
        }

        let buf = crate::file::read_exact(file, *handle.offset(), handle.size() as usize)?;

        let header = Header::decode_from(&mut &buf[..])?;

        let actual_data_len = buf.len().saturating_sub(Header::serialized_len());

        if header.data_length as usize != actual_data_len {
            return Err(crate::Error::InvalidHeader("Block"));
        }

        if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.uncompressed_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        #[expect(clippy::indexing_slicing)]
        let checksum = Checksum::from_raw(crate::hash::hash128(&buf[Header::serialized_len()..]));

        checksum.check(header.checksum).inspect_err(|_| {
            log::error!(
                "Checksum mismatch for block {handle:?}, got={}, expected={}",
                checksum,
                header.checksum,
            );
        })?;

        // Decrypt the on-disk bytes before decompression.
        #[expect(
            clippy::indexing_slicing,
            reason = "header was decoded from buf, so it has at least Header::serialized_len() bytes"
        )]
        let decrypted = encryption
            .map(|enc| enc.decrypt(&buf[Header::serialized_len()..]))
            .transpose()?;

        let buf = match compression {
            CompressionType::None => {
                if let Some(plain) = decrypted {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = plain.len() as u32;

                    if header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    Slice::from(plain)
                } else {
                    let value = buf.slice(Header::serialized_len()..);

                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = value.len() as u32;

                    if header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    value
                }
            }

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                let compressed_data: &[u8] = if let Some(ref plain) = decrypted {
                    plain
                } else {
                    #[expect(clippy::indexing_slicing)]
                    &buf[Header::serialized_len()..]
                };

                let mut decompressed = vec![0u8; header.uncompressed_length as usize];

                let bytes_written = lz4_flex::decompress_into(compressed_data, &mut decompressed)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != header.uncompressed_length as usize {
                    return Err(crate::Error::Decompress(compression));
                }

                Slice::from(decompressed)
            }

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(_) => {
                let compressed_data: &[u8] = if let Some(ref plain) = decrypted {
                    plain
                } else {
                    #[expect(clippy::indexing_slicing)]
                    &buf[Header::serialized_len()..]
                };

                let decompressed =
                    zstd::bulk::decompress(compressed_data, header.uncompressed_length as usize)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                if decompressed.len() != header.uncompressed_length as usize {
                    return Err(crate::Error::Decompress(compression));
                }

                Slice::from(decompressed)
            }
        };

        Ok(Self { header, data: buf })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn block_from_file_roundtrip_uncompressed() -> crate::Result<()> {
        use std::io::Write;

        let data = b"abcdefabcdefabcdef";
        let mut buf = vec![];
        let header =
            Block::write_into(&mut buf, data, BlockType::Data, CompressionType::None, None)?;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(
            BlockOffset(0),
            header.data_length + Header::serialized_len() as u32,
        );
        let block = Block::from_file(&file, handle, CompressionType::None, None)?;
        assert_eq!(data, &*block.data);

        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_roundtrip_lz4() -> crate::Result<()> {
        use std::io::Write;

        let data = b"abcdefabcdefabcdef";
        let mut buf = vec![];
        let header =
            Block::write_into(&mut buf, data, BlockType::Data, CompressionType::Lz4, None)?;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(
            BlockOffset(0),
            header.data_length + Header::serialized_len() as u32,
        );
        let block = Block::from_file(&file, handle, CompressionType::Lz4, None)?;
        assert_eq!(data, &*block.data);

        Ok(())
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn block_from_file_roundtrip_zstd() -> crate::Result<()> {
        use std::io::Write;

        let data = b"abcdefabcdefabcdef";
        let mut buf = vec![];
        let header = Block::write_into(
            &mut buf,
            data,
            BlockType::Data,
            CompressionType::Zstd(3),
            None,
        )?;

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("block");
        let mut file = std::fs::File::create(&path)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        drop(file);

        let file = std::fs::File::open(&path)?;
        let handle = crate::table::BlockHandle::new(
            BlockOffset(0),
            header.data_length + Header::serialized_len() as u32,
        );
        let block = Block::from_file(&file, handle, CompressionType::Zstd(3), None)?;
        assert_eq!(data, &*block.data);

        Ok(())
    }

    #[test]
    fn block_roundtrip_uncompressed() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            BlockType::Data,
            CompressionType::None,
            None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::None, None)?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }
    #[test]
    #[cfg(feature = "lz4")]
    fn block_roundtrip_lz4() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            BlockType::Data,
            CompressionType::Lz4,
            None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::Lz4, None)?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_reject_absurd_uncompressed_length() {
        use crate::coding::Encode;

        // Write a valid lz4-compressed block first so we get the right header format
        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            BlockType::Data,
            CompressionType::Lz4,
            None,
        )
        .unwrap();

        // Tamper the header: set uncompressed_length to u32::MAX.
        // The block checksum only covers the compressed payload bytes; it does not include
        // header fields. The header itself has its own checksum, which we recompute below
        // by re-encoding the modified header, so the tampered block remains internally
        // consistent while exercising the DecompressedSizeTooLarge path.
        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = u32::MAX;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(&mut r, CompressionType::Lz4, None);

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_zero_uncompressed_length_with_data_fails_decompress() {
        use crate::coding::Encode;

        // Zero uncompressed_length is allowed (valid for empty blocks), but when
        // the compressed payload is non-empty, lz4 decompression will fail because
        // the output buffer is zero-sized.
        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            BlockType::Data,
            CompressionType::Lz4,
            None,
        )
        .unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = 0;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(&mut r, CompressionType::Lz4, None);

        assert!(
            matches!(&result, Err(crate::Error::Decompress(_))),
            "expected Decompress error, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn lz4_corrupted_uncompressed_length_triggers_decompress_error() {
        use crate::coding::Encode;
        use std::io::Cursor;

        let payload: &[u8] = b"hello world";

        // Compress with lz4 using the block format
        let compressed = lz4_flex::compress(payload);

        // Build a header with corrupted uncompressed_length (1 byte too large)
        let data_length = compressed.len() as u32;
        let uncompressed_length_correct = payload.len() as u32;
        let uncompressed_length_corrupted = uncompressed_length_correct + 1;

        let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

        let header = Header {
            data_length,
            uncompressed_length: uncompressed_length_corrupted,
            checksum,
            block_type: BlockType::Data,
        };

        let mut buf = header.encode_into_vec();
        buf.extend_from_slice(&compressed);

        let mut cursor = Cursor::new(buf);
        let result = Block::from_reader(&mut cursor, CompressionType::Lz4, None);

        match result {
            Err(crate::Error::Decompress(CompressionType::Lz4)) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
            Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_reject_absurd_uncompressed_length() {
        use crate::coding::Encode;
        use std::io::Write;

        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            BlockType::Data,
            CompressionType::Lz4,
            None,
        )
        .unwrap();

        // Tamper: set uncompressed_length to u32::MAX.
        // The block checksum only covers the compressed payload bytes; it does not include
        // header fields. The header itself has its own checksum, which we recompute below
        // by re-encoding the modified header, so the tampered block remains internally
        // consistent while exercising the DecompressedSizeTooLarge path.
        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = u32::MAX;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&tampered).unwrap();
        tmp.flush().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let handle = crate::table::BlockHandle::new(BlockOffset(0), tampered.len() as u32);
        let result = Block::from_file(&file, handle, CompressionType::Lz4, None);

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_zero_uncompressed_length_with_data_fails_decompress() {
        use crate::coding::Encode;
        use std::io::Write;

        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            BlockType::Data,
            CompressionType::Lz4,
            None,
        )
        .unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = 0;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(&tampered).unwrap();
        tmp.flush().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let handle = crate::table::BlockHandle::new(BlockOffset(0), tampered.len() as u32);
        let result = Block::from_file(&file, handle, CompressionType::Lz4, None);

        assert!(
            matches!(&result, Err(crate::Error::Decompress(_))),
            "expected Decompress error, got: {:?}",
            result.err(),
        );
    }

    #[test]
    fn block_from_reader_reject_absurd_data_length() {
        use crate::coding::Encode;

        let mut buf = vec![];
        Block::write_into(
            &mut buf,
            b"hello",
            BlockType::Data,
            CompressionType::None,
            None,
        )
        .unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let payload: Vec<u8> = reader.to_vec();

        // Set data_length past the limit (no encryption → overhead is 0)
        header.data_length = MAX_DECOMPRESSION_SIZE + 1;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(&mut r, CompressionType::None, None);

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    fn block_from_file_reject_oversized_handle() {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"dummy").unwrap();
        tmp.flush().unwrap();
        let file = std::fs::File::open(tmp.path()).unwrap();

        let handle = crate::table::BlockHandle::new(BlockOffset(0), u32::MAX);
        let result = Block::from_file(&file, handle, CompressionType::None, None);

        assert!(
            matches!(&result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn zstd_corrupted_uncompressed_length_triggers_decompress_error() {
        use crate::coding::Encode;
        use std::io::Cursor;

        let payload: &[u8] = b"hello world";

        let compressed = zstd::bulk::compress(payload, 3).expect("zstd compress failed");

        let data_length = compressed.len() as u32;
        let uncompressed_length_corrupted = payload.len() as u32 + 1;

        let checksum = Checksum::from_raw(crate::hash::hash128(&compressed));

        let header = Header {
            data_length,
            uncompressed_length: uncompressed_length_corrupted,
            checksum,
            block_type: BlockType::Data,
        };

        let mut buf = header.encode_into_vec();
        buf.extend_from_slice(&compressed);

        let mut cursor = Cursor::new(buf);
        let result = Block::from_reader(&mut cursor, CompressionType::Zstd(3), None);

        match result {
            Err(crate::Error::Decompress(CompressionType::Zstd(_))) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
            Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn block_roundtrip_zstd() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            BlockType::Data,
            CompressionType::Zstd(3),
            None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::Zstd(3), None)?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }

    #[test]
    fn block_write_rejects_oversized_payload() {
        let oversized = vec![0u8; MAX_DECOMPRESSION_SIZE as usize + 1];
        let mut sink = std::io::sink();
        let result = Block::write_into(
            &mut sink,
            &oversized,
            BlockType::Data,
            CompressionType::None,
            None,
        );
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {result:?}",
        );
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn block_roundtrip_zstd_large_data() -> crate::Result<()> {
        let data = vec![0xABu8; 64 * 1024]; // 64KB
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            &data,
            BlockType::Data,
            CompressionType::Zstd(3),
            None,
        )?;

        // Verify compression actually reduced size
        assert!(
            writer.len() < data.len(),
            "zstd should compress repeated data"
        );

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::Zstd(3), None)?;
            assert_eq!(&*block.data, &data[..]);
        }

        Ok(())
    }
}
