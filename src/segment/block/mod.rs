// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod binary_index;
mod checksum;
pub mod decoder;
mod encoder;
pub mod hash_index;
mod header;
mod offset;
mod trailer;

pub use checksum::Checksum;
pub(crate) use decoder::{Decodable, Decoder, ParsedItem};
pub(crate) use encoder::{Encodable, Encoder};
pub use header::{BlockType, Header};
pub use offset::BlockOffset;
pub(crate) use trailer::{Trailer, TRAILER_START_MARKER};

use crate::{
    coding::{Decode, Encode},
    segment::BlockHandle,
    CompressionType, Slice,
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
            checksum: Checksum::from_raw(crate::hash::hash128(data)),
            data_length: 0, // <-- NOTE: Is set later on
            uncompressed_length: data.len() as u32,
            previous_block_offset: BlockOffset(0), // <-- TODO:
        };

        let data = match compression {
            CompressionType::None => data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => &lz4_flex::compress(data),
        };
        header.data_length = data.len() as u32;

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

        let data = match compression {
            CompressionType::None => raw_data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                let mut builder =
                    unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                lz4_flex::decompress_into(&raw_data, &mut builder)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                builder.freeze().into()
            }
        };

        debug_assert_eq!(header.uncompressed_length, {
            #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
            {
                data.len() as u32
            }
        });

        let checksum = Checksum::from_raw(crate::hash::hash128(&data));
        if checksum != header.checksum {
            log::warn!(
                "Checksum mismatch for <bufreader>, got={}, expected={}",
                *checksum,
                *header.checksum,
            );
            return Err(crate::Error::InvalidChecksum((checksum, header.checksum)));
        }

        Ok(Self { header, data })
    }

    /// Reads a block from a file.
    pub fn from_file(
        file: &File,
        handle: BlockHandle,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        #[warn(unsafe_code)]
        let mut builder = unsafe { Slice::builder_unzeroed(handle.size() as usize) };

        {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;

                let bytes_read = file.read_at(&mut builder, *handle.offset())?;

                assert_eq!(
                    bytes_read,
                    handle.size() as usize,
                    "not enough bytes read: file has length {}",
                    file.metadata()?.len(),
                );
            }

            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;

                let bytes_read = file.seek_read(&mut builder, *handle.offset())?;

                assert_eq!(
                    bytes_read,
                    handle.size() as usize,
                    "not enough bytes read: file has length {}",
                    file.metadata()?.len(),
                );
            }

            #[cfg(not(any(unix, windows)))]
            {
                compile_error!("unsupported OS");
                unimplemented!();
            }
        }

        let buf = builder.freeze();
        let header = Header::decode_from(&mut &buf[..])?;

        let buf = match compression {
            CompressionType::None => buf.slice(Header::serialized_len()..),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                // NOTE: We know that a header always exists and data is never empty
                // So the slice is fine
                #[allow(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                #[warn(unsafe_code)]
                let mut builder =
                    unsafe { Slice::builder_unzeroed(header.uncompressed_length as usize) };

                lz4_flex::decompress_into(raw_data, &mut builder)
                    .map_err(|_| crate::Error::Decompress(compression))?;

                builder.freeze()
            }
        };

        #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
        {
            debug_assert_eq!(header.uncompressed_length, buf.len() as u32);
        }

        let checksum = Checksum::from_raw(crate::hash::hash128(&buf));
        if checksum != header.checksum {
            log::warn!(
                "Checksum mismatch for block {handle:?}, got={}, expected={}",
                *checksum,
                *header.checksum,
            );

            return Err(crate::Error::InvalidChecksum((checksum, header.checksum)));
        }

        Ok(Self {
            header,
            data: Slice::from(buf),
        })
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
}
