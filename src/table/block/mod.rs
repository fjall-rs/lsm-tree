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
                // NOTE: size cap validation for uncompressed_length is in PR #7
                // (feat/#258-security-validate-uncompressedlength-before-decomp)
                let mut buf = vec![0u8; header.uncompressed_length as usize];

                let bytes_written = lz4_flex::decompress_into(&raw_data, &mut buf)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != header.uncompressed_length as usize {
                    return Err(crate::Error::Decompress(compression));
                }

                Slice::from(buf)
            }
        };

        Ok(Self { header, data })
    }

    /// Reads a block from a file.
    pub fn from_file(
        file: &File,
        handle: BlockHandle,
        compression: CompressionType,
    ) -> crate::Result<Self> {
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
                // NOTE: We know that a header always exists and data is never empty
                // So the slice is fine
                #[expect(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                // NOTE: size cap validation for uncompressed_length is in PR #7
                let mut decompressed = vec![0u8; header.uncompressed_length as usize];

                let bytes_written = lz4_flex::decompress_into(raw_data, &mut decompressed)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                // Runtime validation: corrupted data may decompress to fewer bytes
                if bytes_written != header.uncompressed_length as usize {
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
        let result = Block::from_reader(&mut cursor, CompressionType::Lz4);

        match result {
            Err(crate::Error::Decompress(CompressionType::Lz4)) => { /* expected */ }
            Ok(_) => panic!("expected Error::Decompress, but got Ok(Block)"),
            Err(other) => panic!("expected Error::Decompress, got different error: {other:?}"),
        }
    }
}
