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
    table::BlockHandle,
    Checksum, CompressionType, Slice,
};
use std::fs::File;

/// Safety cap on block payload size (256 MiB).
///
/// Enforced on both write and read paths to prevent producing or
/// accepting blocks that are unreasonably large.
///
/// NOTE: Intentionally duplicated in `vlog::blob_file` (as `usize`)
/// rather than shared, because blocks and blobs are independent storage
/// formats that may diverge in the future.
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
    pub fn write_into<W: std::io::Write>(
        mut writer: &mut W,
        data: &[u8],
        block_type: BlockType,
        compression: CompressionType,
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

        // `compressed_buf` keeps the compressed data alive for the LZ4 branch
        let mut compressed_buf: Option<Vec<u8>> = None;

        let payload: &[u8] = match compression {
            CompressionType::None => data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                let buf = lz4_flex::compress(data);

                if buf.len() > MAX_DECOMPRESSION_SIZE as usize {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: buf.len() as u64,
                        limit: u64::from(MAX_DECOMPRESSION_SIZE),
                    });
                }

                compressed_buf = Some(buf);

                // SAFETY: just set above
                #[expect(clippy::expect_used, reason = "compressed_buf was just assigned")]
                compressed_buf.as_ref().expect("just assigned")
            }
        };

        #[expect(clippy::cast_possible_truncation, reason = "blocks are limited to u32")]
        {
            header.data_length = payload.len() as u32;
            header.checksum = Checksum::from_raw(crate::hash::hash128(payload));
        }

        header.encode_into(&mut writer)?;
        writer.write_all(payload)?;

        log::trace!(
            "Writing block with size {}B (compressed: {}B) (excluding header of {}B)",
            header.uncompressed_length,
            header.data_length,
            Header::serialized_len(),
        );

        Ok(header)
    }

    /// Reads a block from a reader.
    pub fn from_reader<R: std::io::Read>(
        reader: &mut R,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        let header = Header::decode_from(reader)?;

        if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.uncompressed_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        if header.data_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.data_length),
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

        let data = match compression {
            CompressionType::None => raw_data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                #[warn(unsafe_code)]
                let mut builder =
                    unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                lz4_flex::decompress_into(&raw_data, &mut builder)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                builder.freeze().into()
            }
        };

        debug_assert_eq!(header.uncompressed_length, {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
            {
                data.len() as u32
            }
        });

        Ok(Self { header, data })
    }

    /// Reads a block from a file.
    pub fn from_file(
        file: &File,
        handle: BlockHandle,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        // Cap the read size: header + payload must not exceed the safety limit
        let max_on_disk = Header::serialized_len() as u64 + u64::from(MAX_DECOMPRESSION_SIZE);

        if u64::from(handle.size()) > max_on_disk {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(handle.size()),
                limit: max_on_disk,
            });
        }

        let buf = crate::file::read_exact(file, *handle.offset(), handle.size() as usize)?;

        let header = Header::decode_from(&mut &buf[..])?;

        #[expect(clippy::indexing_slicing)]
        let checksum = Checksum::from_raw(crate::hash::hash128(&buf[Header::serialized_len()..]));

        checksum.check(header.checksum).inspect_err(|_| {
            log::error!(
                "Checksum mismatch for block {handle:?}, got={}, expected={}",
                checksum,
                header.checksum,
            );
        })?;

        if header.data_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.data_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(header.uncompressed_length),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
            });
        }

        let buf = match compression {
            CompressionType::None => {
                let value = buf.slice(Header::serialized_len()..);

                #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
                {
                    debug_assert_eq!(header.uncompressed_length, value.len() as u32);
                }

                value
            }

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                // NOTE: We know that a header always exists and data is never empty
                // So the slice is fine
                #[expect(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                #[warn(unsafe_code)]
                let mut builder =
                    unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                lz4_flex::decompress_into(raw_data, &mut builder)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                builder.freeze().into()
            }
        };

        Ok(Self { header, data: buf })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    // TODO: Block::from_file roundtrips

    #[test]
    fn block_roundtrip_uncompressed() -> crate::Result<()> {
        let mut writer = vec![];

        Block::write_into(
            &mut writer,
            b"abcdefabcdefabcdef",
            BlockType::Data,
            CompressionType::None,
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::None)?;
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
        )?;

        {
            let mut reader = &writer[..];
            let block = Block::from_reader(&mut reader, CompressionType::Lz4)?;
            assert_eq!(b"abcdefabcdefabcdef", &*block.data);
        }

        Ok(())
    }

    #[test]
    #[ignore = "allocates ~256 MiB; run with `cargo test -- --ignored`"]
    fn block_write_rejects_oversized_payload() {
        let data = vec![0u8; MAX_DECOMPRESSION_SIZE as usize + 1];
        let mut sink = std::io::sink();
        let result = Block::write_into(&mut sink, &data, BlockType::Data, CompressionType::None);
        assert!(
            matches!(
                result,
                Err(crate::Error::DecompressedSizeTooLarge { declared, limit })
                    if declared == (MAX_DECOMPRESSION_SIZE as u64 + 1)
                        && limit == u64::from(MAX_DECOMPRESSION_SIZE)
            ),
            "expected DecompressedSizeTooLarge with matching declared/limit, got: {result:?}",
        );
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn block_from_reader_rejects_absurd_uncompressed_length() {
        use crate::coding::Encode;

        // Write a valid lz4 block, then tamper the header
        let mut buf = vec![];
        Block::write_into(&mut buf, b"hello", BlockType::Data, CompressionType::Lz4).unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        // Set uncompressed_length to exceed the cap
        header.uncompressed_length = MAX_DECOMPRESSION_SIZE + 1;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(&mut r, CompressionType::Lz4);
        assert!(
            matches!(result, Err(crate::Error::DecompressedSizeTooLarge { .. })),
            "expected DecompressedSizeTooLarge, got: {:?}",
            result.err(),
        );
    }
}
