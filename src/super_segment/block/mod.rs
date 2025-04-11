// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub(crate) mod binary_index;
mod checksum;
mod encoder;
pub(crate) mod hash_index;
mod header;
mod offset;
mod trailer;

pub use checksum::Checksum;
pub(crate) use encoder::{Encodable, Encoder};
pub use header::Header;
pub use offset::BlockOffset;
pub(crate) use trailer::{Trailer, TRAILER_START_MARKER};

use crate::{
    coding::{Decode, Encode},
    CompressionType, Slice,
};
use std::fs::File;
use xxhash_rust::xxh3::xxh3_64;

/// A block on disk.
///
/// Consists of a header and some bytes (the data/payload).
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

    pub fn to_writer<W: std::io::Write>(
        mut writer: &mut W,
        data: &[u8],
        compression: CompressionType,
    ) -> crate::Result<Header> {
        let checksum = xxh3_64(data);

        let mut header = Header {
            checksum: Checksum::from_raw(checksum),
            data_length: 0, // <-- NOTE: Is set later on
            uncompressed_length: data.len() as u32,
            previous_block_offset: BlockOffset(0), // <-- TODO:
        };

        let data = match compression {
            CompressionType::None => data,
            CompressionType::Lz4 => &lz4_flex::compress(data),
            CompressionType::Miniz(level) => &miniz_oxide::deflate::compress_to_vec(data, level),
        };
        header.data_length = data.len() as u32;

        debug_assert!(header.data_length > 0);

        header.encode_into(&mut writer)?;
        writer.write_all(data)?;

        log::trace!(
            "Writing block with size {}B (compressed: {}B)",
            header.uncompressed_length,
            header.data_length,
        );

        Ok(header)
    }

    pub fn from_file(
        file: &File,
        offset: BlockOffset,
        size: u32,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        // TODO: use a Slice::get_mut instead... needs value-log update
        let mut buf = byteview::ByteView::with_size(size as usize);

        {
            let mut mutator = buf.get_mut().expect("should be the owner");

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;

                file.read_at(&mut mutator, *offset)?;
            }

            #[cfg(windows)]
            {
                todo!()
            }

            #[cfg(not(any(unix, windows)))]
            {
                compile_error!("unsupported OS");
                unimplemented!();
            }
        }

        let header = Header::decode_from(&mut &buf[..])?;

        let data = match compression {
            CompressionType::None => buf.slice(Header::serialized_len()..),
            CompressionType::Lz4 => {
                // NOTE: We know that a header always exists and data is never empty
                // So the slice is fine
                #[allow(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                let mut data = byteview::ByteView::with_size(header.uncompressed_length as usize);
                {
                    // NOTE: We know that we are the owner
                    #[allow(clippy::expect_used)]
                    let mut mutator = data.get_mut().expect("should be the owner");

                    lz4_flex::decompress_into(raw_data, &mut mutator)
                        .map_err(|_| crate::Error::Decompress(compression))?;
                }
                data
            }
            CompressionType::Miniz(_) => {
                // NOTE: We know that a header always exists and data is never empty
                // So the slice is fine
                #[allow(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                miniz_oxide::inflate::decompress_to_vec(raw_data)
                    .map_err(|_| crate::Error::Decompress(compression))?
                    .into()
            }
        };

        debug_assert_eq!(header.uncompressed_length, {
            #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
            {
                data.len() as u32
            }
        });

        Ok(Self {
            header,
            data: Slice::from(data),
        })
    }
}
