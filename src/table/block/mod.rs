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

/// Maximum allowed decompressed block size (256 MiB).
///
/// Prevents OOM from crafted SST files that declare an absurdly large
/// `uncompressed_length` in the block header.
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
        let mut header = Header {
            block_type,
            checksum: Checksum::from_raw(0), // <-- NOTE: Is set later on
            data_length: 0,                  // <-- NOTE: Is set later on

            #[expect(clippy::cast_possible_truncation, reason = "blocks are limited to u32")]
            uncompressed_length: data.len() as u32,
        };

        let data = match compression {
            CompressionType::None => data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => &lz4_flex::compress(data),
        };

        #[expect(clippy::cast_possible_truncation, reason = "blocks are limited to u32")]
        {
            header.data_length = data.len() as u32;
            header.checksum = Checksum::from_raw(crate::hash::hash128(data));
        }

        header.encode_into(&mut writer)?;
        writer.write_all(data)?;

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
                if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: u64::from(header.uncompressed_length),
                        limit: u64::from(MAX_DECOMPRESSION_SIZE),
                    });
                }

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
        if handle.size() > MAX_DECOMPRESSION_SIZE {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: u64::from(handle.size()),
                limit: u64::from(MAX_DECOMPRESSION_SIZE),
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
                if header.uncompressed_length > MAX_DECOMPRESSION_SIZE {
                    return Err(crate::Error::DecompressedSizeTooLarge {
                        declared: u64::from(header.uncompressed_length),
                        limit: u64::from(MAX_DECOMPRESSION_SIZE),
                    });
                }

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
    #[cfg(feature = "lz4")]
    fn block_reject_absurd_uncompressed_length() {
        use crate::coding::Encode;

        // Write a valid lz4-compressed block first so we get the right header format
        let mut buf = vec![];
        Block::write_into(&mut buf, b"hello", BlockType::Data, CompressionType::Lz4).unwrap();

        // Tamper the header: set uncompressed_length to u32::MAX.
        // The block checksum only covers the compressed payload bytes, so changing
        // this header field does not require updating any checksum; we just re-encode the header.
        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = u32::MAX;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(&mut r, CompressionType::Lz4);

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
        Block::write_into(&mut buf, b"hello", BlockType::Data, CompressionType::Lz4).unwrap();

        let mut reader = &buf[..];
        let mut header = Header::decode_from(&mut reader).unwrap();
        let compressed_payload: Vec<u8> = reader.to_vec();

        header.uncompressed_length = 0;
        let mut tampered = header.encode_into_vec();
        tampered.extend_from_slice(&compressed_payload);

        let mut r = &tampered[..];
        let result = Block::from_reader(&mut r, CompressionType::Lz4);

        assert!(
            matches!(&result, Err(crate::Error::Decompress(_))),
            "expected Decompress error, got: {:?}",
            result.err(),
        );
    }
}
