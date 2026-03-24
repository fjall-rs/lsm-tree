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
    ///
    /// When `compression` is [`CompressionType::ZstdDict`], `zstd_dict` must
    /// contain the raw dictionary bytes matching the `dict_id` in the
    /// compression type. For all other compression types, `zstd_dict` is
    /// ignored.
    pub fn write_into<W: std::io::Write>(
        mut writer: &mut W,
        data: &[u8],
        block_type: BlockType,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        #[cfg(feature = "zstd")] zstd_dict: Option<&crate::compression::ZstdDictionary>,
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

        // Compression step — produces an owned Vec when a compressor is active.
        #[cfg(any(feature = "lz4", feature = "zstd"))]
        let mut compressed_buf: Option<Vec<u8>> = None;

        match compression {
            CompressionType::None => {}

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                compressed_buf = Some(lz4_flex::compress(data));
            }

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(level) => {
                compressed_buf = Some(
                    zstd::bulk::compress(data, level)
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?,
                );
            }

            #[cfg(feature = "zstd")]
            CompressionType::ZstdDict { level, dict_id } => {
                let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got: None,
                })?;
                if dict.id() != dict_id {
                    return Err(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: Some(dict.id()),
                    });
                }

                compressed_buf = Some({
                    let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dict.raw())
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
                    compressor
                        .compress(data)
                        .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?
                });
            }
        }

        // Encryption step — reuses the owned compression buffer via encrypt_vec
        // when available, eliminating one allocation on the compress+encrypt path.
        let encrypted_buf: Option<Vec<u8>>;

        #[cfg(any(feature = "lz4", feature = "zstd"))]
        {
            encrypted_buf = if let Some(enc) = encryption {
                Some(match compressed_buf.take() {
                    Some(owned) => enc.encrypt_vec(owned)?,
                    None => enc.encrypt(data)?,
                })
            } else {
                None
            };
        }

        #[cfg(not(any(feature = "lz4", feature = "zstd")))]
        {
            encrypted_buf = encryption.map(|enc| enc.encrypt(data)).transpose()?;
        }

        // Determine the final on-disk payload reference.
        let payload: &[u8] = if let Some(ref enc) = encrypted_buf {
            enc
        } else {
            #[cfg(any(feature = "lz4", feature = "zstd"))]
            {
                compressed_buf.as_deref().unwrap_or(data)
            }
            #[cfg(not(any(feature = "lz4", feature = "zstd")))]
            {
                data
            }
        };

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
    // The encrypted and unencrypted branches duplicate the checksum
    // verification and compression match because their input types
    // differ: encrypted reads into Vec<u8> (for decrypt_vec in-place
    // reuse), while unencrypted reads into Slice (zero-copy on the
    // None-compression path). Unifying them would require either a
    // Cow/enum wrapper or sacrificing the zero-copy optimization.
    #[expect(
        clippy::too_many_lines,
        reason = "encrypt/no-encrypt branches duplicate compression match — see comment above"
    )]
    pub fn from_reader<R: std::io::Read>(
        reader: &mut R,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        #[cfg(feature = "zstd")] zstd_dict: Option<&crate::compression::ZstdDictionary>,
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

        // When encryption is active, read into a Vec so decrypt_vec can
        // reuse the buffer in-place (one allocation instead of two).
        // When no encryption, read into a Slice which may use optimized
        // reference-counted storage.
        let data = if let Some(enc) = encryption {
            let mut raw_vec = vec![0u8; header.data_length as usize];
            reader.read_exact(&mut raw_vec)?;

            let checksum = Checksum::from_raw(crate::hash::hash128(&raw_vec));
            checksum.check(header.checksum).inspect_err(|_| {
                log::error!(
                    "Checksum mismatch for <bufreader>, got={}, expected={}",
                    checksum,
                    header.checksum,
                );
            })?;

            // Decrypt in-place, reusing the read buffer.
            let decrypted = enc.decrypt_vec(raw_vec)?;

            match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = decrypted.len() as u32;

                    if header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    Slice::from(decrypted)
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let mut buf = vec![0u8; header.uncompressed_length as usize];

                    let bytes_written = lz4_flex::decompress_into(&decrypted, &mut buf)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(buf)
                }

                #[cfg(feature = "zstd")]
                CompressionType::Zstd(_) => {
                    let decompressed =
                        zstd::bulk::decompress(&decrypted, header.uncompressed_length as usize)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(feature = "zstd")]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict.raw())
                        .map_err(|_| crate::Error::Decompress(compression))?;
                    let decompressed = decompressor
                        .decompress(&decrypted, header.uncompressed_length as usize)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            }
        } else {
            let raw_data = Slice::from_reader(reader, header.data_length as usize)?;

            let checksum = Checksum::from_raw(crate::hash::hash128(&raw_data));
            checksum.check(header.checksum).inspect_err(|_| {
                log::error!(
                    "Checksum mismatch for <bufreader>, got={}, expected={}",
                    checksum,
                    header.checksum,
                );
            })?;

            match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = raw_data.len() as u32;

                    if header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    raw_data
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let mut buf = vec![0u8; header.uncompressed_length as usize];

                    let bytes_written = lz4_flex::decompress_into(&raw_data, &mut buf)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(buf)
                }

                #[cfg(feature = "zstd")]
                CompressionType::Zstd(_) => {
                    let decompressed =
                        zstd::bulk::decompress(&raw_data, header.uncompressed_length as usize)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(feature = "zstd")]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict.raw())
                        .map_err(|_| crate::Error::Decompress(compression))?;
                    let decompressed = decompressor
                        .decompress(&raw_data, header.uncompressed_length as usize)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            }
        };

        Ok(Self { header, data })
    }

    /// Reads a block from a file.
    ///
    /// Pipeline: read → verify checksum → decrypt → decompress.
    /// When `encryption` is `None`, the decrypt step is skipped.
    // Same duplication rationale as from_reader — see comment there.
    #[expect(
        clippy::too_many_lines,
        reason = "encrypt/no-encrypt branches duplicate compression match — see from_reader"
    )]
    pub fn from_file(
        file: &dyn FsFile,
        handle: BlockHandle,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        #[cfg(feature = "zstd")] zstd_dict: Option<&crate::compression::ZstdDictionary>,
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

        // When encryption is active, read the whole block into an owned
        // Vec (single I/O, single allocation), parse the header, then strip
        // the header prefix so decrypt_vec operates on the payload in-place.
        // No intermediate Slice, no overlap of encrypted + decrypted buffers.
        // When no encryption, read into a Slice (zero-copy on the
        // None-compression path).
        let (header, data) = if let Some(enc) = encryption {
            let header_len = Header::serialized_len();
            let block_size = handle.size() as usize;

            if block_size < header_len {
                return Err(crate::Error::InvalidHeader("Block"));
            }

            // Zero-init is redundant (read_at overwrites all bytes) but avoids
            // unsafe. The cost is negligible vs I/O + decryption. Unsafe
            // uninitialized allocation (like Slice::builder_unzeroed) could be
            // used here if profiling shows this as a bottleneck.
            let mut buf = vec![0u8; block_size];
            let n = file.read_at(&mut buf, *handle.offset())?;
            if n != block_size {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "block read_at: expected {block_size} bytes, got {n} at offset {}",
                        *handle.offset(),
                    ),
                )));
            }

            #[expect(
                clippy::indexing_slicing,
                reason = "buf.len() == block_size == handle.size() ≥ Header::serialized_len()"
            )]
            let parsed_header = Header::decode_from(&mut &buf[..header_len])?;

            let actual_data_len = block_size.saturating_sub(header_len);
            if parsed_header.data_length as usize != actual_data_len {
                return Err(crate::Error::InvalidHeader("Block"));
            }

            if parsed_header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: u64::from(parsed_header.uncompressed_length),
                    limit: u64::from(MAX_DECOMPRESSION_SIZE),
                });
            }

            // Checksum covers the on-disk payload (after header).
            #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
            let checksum = Checksum::from_raw(crate::hash::hash128(&buf[header_len..]));
            checksum.check(parsed_header.checksum).inspect_err(|_| {
                log::error!(
                    "Checksum mismatch for block {handle:?}, got={}, expected={}",
                    checksum,
                    parsed_header.checksum,
                );
            })?;

            // Strip header prefix so buf contains only the payload.
            buf.copy_within(header_len.., 0);
            buf.truncate(actual_data_len);

            let decrypted = enc.decrypt_vec(buf)?;

            let data = match compression {
                CompressionType::None => {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = decrypted.len() as u32;

                    if parsed_header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    Slice::from(decrypted)
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    let mut decompressed = vec![0u8; parsed_header.uncompressed_length as usize];

                    let bytes_written = lz4_flex::decompress_into(&decrypted, &mut decompressed)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(feature = "zstd")]
                CompressionType::Zstd(_) => {
                    let decompressed = zstd::bulk::decompress(
                        &decrypted,
                        parsed_header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(feature = "zstd")]
                CompressionType::ZstdDict { dict_id, .. } => {
                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict.raw())
                        .map_err(|_| crate::Error::Decompress(compression))?;
                    let decompressed = decompressor
                        .decompress(&decrypted, parsed_header.uncompressed_length as usize)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            };

            (parsed_header, data)
        } else {
            // Single I/O read — header + payload in one Slice.
            let buf = crate::file::read_exact(file, *handle.offset(), handle.size() as usize)?;

            let parsed_header = Header::decode_from(&mut &buf[..])?;

            let actual_data_len = buf.len().saturating_sub(Header::serialized_len());
            if parsed_header.data_length as usize != actual_data_len {
                return Err(crate::Error::InvalidHeader("Block"));
            }

            if parsed_header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
                return Err(crate::Error::DecompressedSizeTooLarge {
                    declared: u64::from(parsed_header.uncompressed_length),
                    limit: u64::from(MAX_DECOMPRESSION_SIZE),
                });
            }

            #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
            let checksum =
                Checksum::from_raw(crate::hash::hash128(&buf[Header::serialized_len()..]));

            checksum.check(parsed_header.checksum).inspect_err(|_| {
                log::error!(
                    "Checksum mismatch for block {handle:?}, got={}, expected={}",
                    checksum,
                    parsed_header.checksum,
                );
            })?;

            let data = match compression {
                CompressionType::None => {
                    let value = buf.slice(Header::serialized_len()..);

                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "values are u32 length max"
                    )]
                    let actual_len = value.len() as u32;

                    if parsed_header.uncompressed_length != actual_len {
                        return Err(crate::Error::InvalidHeader("Block"));
                    }

                    value
                }

                #[cfg(feature = "lz4")]
                CompressionType::Lz4 => {
                    #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                    let compressed_data = &buf[Header::serialized_len()..];

                    let mut decompressed = vec![0u8; parsed_header.uncompressed_length as usize];

                    let bytes_written =
                        lz4_flex::decompress_into(compressed_data, &mut decompressed)
                            .map_err(|_| crate::Error::Decompress(compression))?;

                    if bytes_written != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(feature = "zstd")]
                CompressionType::Zstd(_) => {
                    #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                    let compressed_data = &buf[Header::serialized_len()..];

                    let decompressed = zstd::bulk::decompress(
                        compressed_data,
                        parsed_header.uncompressed_length as usize,
                    )
                    .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }

                #[cfg(feature = "zstd")]
                CompressionType::ZstdDict { dict_id, .. } => {
                    #[expect(clippy::indexing_slicing, reason = "header was decoded from buf")]
                    let compressed_data = &buf[Header::serialized_len()..];

                    let dict = zstd_dict.ok_or(crate::Error::ZstdDictMismatch {
                        expected: dict_id,
                        got: None,
                    })?;
                    if dict.id() != dict_id {
                        return Err(crate::Error::ZstdDictMismatch {
                            expected: dict_id,
                            got: Some(dict.id()),
                        });
                    }

                    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict.raw())
                        .map_err(|_| crate::Error::Decompress(compression))?;
                    let decompressed = decompressor
                        .decompress(compressed_data, parsed_header.uncompressed_length as usize)
                        .map_err(|_| crate::Error::Decompress(compression))?;

                    if decompressed.len() != parsed_header.uncompressed_length as usize {
                        return Err(crate::Error::Decompress(compression));
                    }

                    Slice::from(decompressed)
                }
            };

            (parsed_header, data)
        };

        Ok(Self { header, data })
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn block_from_file_roundtrip_uncompressed() -> crate::Result<()> {
        use std::io::Write;

        let data = b"abcdefabcdefabcdef";
        let mut buf = vec![];
        let header = Block::write_into(
            &mut buf,
            data,
            BlockType::Data,
            CompressionType::None,
            None,
            #[cfg(feature = "zstd")]
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
        let block = Block::from_file(
            &file,
            handle,
            CompressionType::None,
            None,
            #[cfg(feature = "zstd")]
            None,
        )?;
        assert_eq!(data, &*block.data);

        Ok(())
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_file_roundtrip_lz4() -> crate::Result<()> {
        use std::io::Write;

        let data = b"abcdefabcdefabcdef";
        let mut buf = vec![];
        let header = Block::write_into(
            &mut buf,
            data,
            BlockType::Data,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
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
        let block = Block::from_file(
            &file,
            handle,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
            None,
        )?;
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
        let block = Block::from_file(&file, handle, CompressionType::Zstd(3), None, None)?;
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
            #[cfg(feature = "zstd")]
            None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::None,
                None,
                #[cfg(feature = "zstd")]
                None,
            )?;
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
            #[cfg(feature = "zstd")]
            None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::Lz4,
                None,
                #[cfg(feature = "zstd")]
                None,
            )?;
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
            #[cfg(feature = "zstd")]
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
        let result = Block::from_reader(
            &mut r,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
            #[cfg(feature = "zstd")]
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
        let result = Block::from_reader(
            &mut r,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
        let result = Block::from_reader(
            &mut cursor,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
            #[cfg(feature = "zstd")]
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
        let result = Block::from_file(
            &file,
            handle,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
            #[cfg(feature = "zstd")]
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
        let result = Block::from_file(
            &file,
            handle,
            CompressionType::Lz4,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
            #[cfg(feature = "zstd")]
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
        let result = Block::from_reader(
            &mut r,
            CompressionType::None,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
        let result = Block::from_file(
            &file,
            handle,
            CompressionType::None,
            None,
            #[cfg(feature = "zstd")]
            None,
        );

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
        let result = Block::from_reader(&mut cursor, CompressionType::Zstd(3), None, None);

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
            None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::Zstd(3), None, None)?;
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
            #[cfg(feature = "zstd")]
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
            None,
        )?;

        // Verify compression actually reduced size
        assert!(
            writer.len() < data.len(),
            "zstd should compress repeated data"
        );

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::Zstd(3), None, None)?;
            assert_eq!(&*block.data, &data[..]);
        }

        Ok(())
    }

    // --- Encrypted block roundtrip tests ---
    // These exercise the encrypt_vec/decrypt_vec code paths in write_into,
    // from_reader, and from_file that are untouched by the non-encrypted tests.
    //
    // NOTE: The tempfile + write + reopen + handle pattern is duplicated across
    // from_file tests (both encrypted and non-encrypted). Tracked in #127.

    #[cfg(feature = "encryption")]
    mod encrypted {
        use crate::table::block::*;

        fn test_provider() -> crate::encryption::Aes256GcmProvider {
            crate::encryption::Aes256GcmProvider::new(&[0x42; 32])
        }

        #[test]
        fn block_roundtrip_encrypted_uncompressed() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"plaintext block data for encryption test";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "lz4")]
        fn block_roundtrip_encrypted_lz4() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                BlockType::Data,
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "zstd")]
        fn block_roundtrip_encrypted_zstd() -> crate::Result<()> {
            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                BlockType::Data,
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_uncompressed() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let data = b"plaintext block data for from_file encryption test";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
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
            let block = Block::from_file(
                &file,
                handle,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "lz4")]
        fn block_from_file_encrypted_lz4() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                BlockType::Data,
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(feature = "zstd")]
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
            let block = Block::from_file(
                &file,
                handle,
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "zstd")]
        fn block_from_file_encrypted_zstd() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let data = b"abcdefabcdefabcdef";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                BlockType::Data,
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(feature = "zstd")]
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
            let block = Block::from_file(
                &file,
                handle,
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_wrong_key_fails() -> crate::Result<()> {
            use std::io::Write;

            let enc_write = test_provider();
            let enc_read = crate::encryption::Aes256GcmProvider::new(&[0x99; 32]);
            let data = b"encrypted block data";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc_write),
                #[cfg(feature = "zstd")]
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
            let result = Block::from_file(
                &file,
                handle,
                CompressionType::None,
                Some(&enc_read),
                #[cfg(feature = "zstd")]
                None,
            );
            assert!(
                matches!(result, Err(crate::Error::Decrypt(_))),
                "expected Decrypt error for wrong key, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_reader_encrypted_wrong_key_fails() -> crate::Result<()> {
            let enc_write = test_provider();
            let enc_read = crate::encryption::Aes256GcmProvider::new(&[0x99; 32]);
            let data = b"encrypted block data";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc_write),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let result = Block::from_reader(
                &mut reader,
                CompressionType::None,
                Some(&enc_read),
                #[cfg(feature = "zstd")]
                None,
            );
            assert!(
                matches!(result, Err(crate::Error::Decrypt(_))),
                "expected Decrypt error for wrong key, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_checksum_tamper_detected() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let data = b"data for tamper test";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            // Tamper a byte in the encrypted payload (after header)
            let mid = Header::serialized_len() + 1;
            if mid < buf.len() {
                #[expect(clippy::indexing_slicing, reason = "mid < buf.len() checked above")]
                {
                    buf[mid] ^= 0xFF;
                }
            }

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
            let result = Block::from_file(
                &file,
                handle,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            );
            assert!(
                matches!(result, Err(crate::Error::ChecksumMismatch { .. })),
                "expected ChecksumMismatch for tampered data, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_undersized_handle_rejected() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let dir = tempfile::tempdir()?;
            let path = dir.path().join("block");
            let mut file = std::fs::File::create(&path)?;
            file.write_all(b"tiny")?;
            file.sync_all()?;
            drop(file);

            let file = std::fs::File::open(&path)?;
            // Handle size smaller than Header::serialized_len()
            let handle = crate::table::BlockHandle::new(BlockOffset(0), 2);
            let result = Block::from_file(
                &file,
                handle,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            );

            assert!(
                matches!(result, Err(crate::Error::InvalidHeader(_))),
                "expected InvalidHeader for undersized handle, got: {:?}",
                result.err(),
            );
            Ok(())
        }

        #[test]
        fn block_from_file_encrypted_uncompressed_large_payload() -> crate::Result<()> {
            use std::io::Write;

            let enc = test_provider();
            let data = vec![0xBB_u8; 32 * 1024]; // 32 KiB
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                &data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
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
            let block = Block::from_file(
                &file,
                handle,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        fn block_roundtrip_encrypted_uncompressed_large() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xCC_u8; 32 * 1024]; // 32 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                BlockType::Data,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::None,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        #[cfg(feature = "lz4")]
        fn block_roundtrip_encrypted_lz4_large() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xDD_u8; 32 * 1024]; // 32 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                BlockType::Data,
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::Lz4,
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        #[cfg(feature = "zstd")]
        fn block_roundtrip_encrypted_zstd_large() -> crate::Result<()> {
            let enc = test_provider();
            let data = vec![0xEE_u8; 32 * 1024]; // 32 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                BlockType::Data,
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(
                &mut reader,
                CompressionType::Zstd(3),
                Some(&enc),
                #[cfg(feature = "zstd")]
                None,
            )?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd_dict {
        use super::*;
        use crate::compression::ZstdDictionary;
        use test_log::test;

        fn test_dict() -> ZstdDictionary {
            let mut samples = Vec::new();
            for i in 0u32..500 {
                samples.extend_from_slice(format!("key-{i:05}val-{i:05}").as_bytes());
            }
            ZstdDictionary::new(&samples)
        }

        fn test_compression(dict: &ZstdDictionary) -> CompressionType {
            CompressionType::ZstdDict {
                level: 3,
                dict_id: dict.id(),
            }
        }

        #[test]
        fn block_roundtrip_zstd_dict_reader() -> crate::Result<()> {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = b"abcdefabcdefabcdef";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                BlockType::Data,
                compression,
                None,
                Some(&dict),
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, compression, None, Some(&dict))?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_roundtrip_zstd_dict_file() -> crate::Result<()> {
            use std::io::Write;

            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = b"abcdefabcdefabcdef";
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                data,
                BlockType::Data,
                compression,
                None,
                Some(&dict),
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
            let block = Block::from_file(&file, handle, compression, None, Some(&dict))?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        fn block_roundtrip_zstd_dict_large_data() -> crate::Result<()> {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = vec![0xAB_u8; 64 * 1024]; // 64 KiB
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                &data,
                BlockType::Data,
                compression,
                None,
                Some(&dict),
            )?;

            assert!(
                writer.len() < data.len(),
                "dict compression should reduce size"
            );

            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, compression, None, Some(&dict))?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }

        #[test]
        fn block_zstd_dict_missing_returns_error() -> crate::Result<()> {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let mut sink = vec![];

            // Write with dict
            Block::write_into(
                &mut sink,
                b"hello",
                BlockType::Data,
                compression,
                None,
                Some(&dict),
            )?;

            // Read without dict → ZstdDictMismatch
            let mut reader = &sink[..];
            let result = Block::from_reader(&mut reader, compression, None, None);
            assert!(
                matches!(
                    result,
                    Err(crate::Error::ZstdDictMismatch { got: None, .. })
                ),
                "expected ZstdDictMismatch with got=None",
            );
            Ok(())
        }

        #[test]
        fn block_zstd_dict_wrong_dict_returns_error() -> crate::Result<()> {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let wrong_dict = ZstdDictionary::new(b"completely different dictionary bytes");
            let mut sink = vec![];

            // Write expects dict.id(), but we'll try reading with wrong_dict
            Block::write_into(
                &mut sink,
                b"hello",
                BlockType::Data,
                compression,
                None,
                Some(&dict),
            )?;

            let mut reader = &sink[..];
            let result = Block::from_reader(&mut reader, compression, None, Some(&wrong_dict));
            assert!(
                matches!(
                    result,
                    Err(crate::Error::ZstdDictMismatch { got: Some(_), .. })
                ),
                "expected ZstdDictMismatch with got=Some",
            );
            Ok(())
        }

        #[test]
        fn block_write_zstd_dict_missing_returns_error() {
            let dict = test_dict();
            let compression = test_compression(&dict);
            let mut sink = std::io::sink();

            // Write without providing dict → ZstdDictMismatch
            let result = Block::write_into(
                &mut sink,
                b"hello",
                BlockType::Data,
                compression,
                None,
                None, // no dict
            );
            assert!(
                matches!(
                    result,
                    Err(crate::Error::ZstdDictMismatch { got: None, .. })
                ),
                "expected ZstdDictMismatch, got: {result:?}",
            );
        }

        #[test]
        #[cfg(feature = "encryption")]
        fn block_roundtrip_zstd_dict_encrypted_reader() -> crate::Result<()> {
            let enc = crate::Aes256GcmProvider::new(&[0x42; 32]);
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = b"encrypted-dict-compressed-data-for-test";
            let mut writer = vec![];

            Block::write_into(
                &mut writer,
                data,
                BlockType::Data,
                compression,
                Some(&enc),
                Some(&dict),
            )?;

            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, compression, Some(&enc), Some(&dict))?;
            assert_eq!(data, &*block.data);
            Ok(())
        }

        #[test]
        #[cfg(feature = "encryption")]
        fn block_roundtrip_zstd_dict_encrypted_file() -> crate::Result<()> {
            use std::io::Write;

            let enc = crate::Aes256GcmProvider::new(&[0x42; 32]);
            let dict = test_dict();
            let compression = test_compression(&dict);
            let data = vec![0xCC_u8; 16 * 1024]; // 16 KiB
            let mut buf = vec![];
            let header = Block::write_into(
                &mut buf,
                &data,
                BlockType::Data,
                compression,
                Some(&enc),
                Some(&dict),
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
            let block = Block::from_file(&file, handle, compression, Some(&enc), Some(&dict))?;
            assert_eq!(&*block.data, &data[..]);
            Ok(())
        }
    }
}
