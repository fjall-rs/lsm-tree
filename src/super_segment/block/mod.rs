// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod encoder;
mod header;
mod trailer;

pub(crate) use encoder::{Encodable, Encoder};
pub use header::Header;
pub(crate) use trailer::{Trailer, TRAILER_START_MARKER};

use crate::{coding::Decode, segment::block::offset::BlockOffset, CompressionType, Slice};
use std::fs::File;

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

    pub fn from_file(
        file: &File,
        offset: BlockOffset,
        size: usize,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        // TODO: use a Slice::get_mut instead... needs value-log update
        let mut buf = byteview::ByteView::with_size(size);

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

        let header = Header::decode_from(&mut &*buf)?;

        debug_assert_eq!(header.uncompressed_length, {
            #[allow(clippy::expect_used, clippy::cast_possible_truncation)]
            {
                buf.get(Header::serialized_len()..)
                    .expect("should be in bounds")
                    .len() as u32
            }
        });

        let data = match compression {
            CompressionType::None => buf.slice(Header::serialized_len()..),
            CompressionType::Lz4 => {
                // NOTE: We that a header always exists and data is never empty
                // So the slice is fine
                #[allow(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                let mut data = byteview::ByteView::with_size(header.uncompressed_length as usize);
                {
                    let mut mutator = data.get_mut().expect("should be the owner");
                    lz4_flex::decompress_into(raw_data, &mut mutator)
                        .map_err(|_| crate::Error::Decompress(compression))?;
                }
                data
            }
            CompressionType::Miniz(_) => {
                // NOTE: We that a header always exists and data is never empty
                // So the slice is fine
                #[allow(clippy::indexing_slicing)]
                let raw_data = &buf[Header::serialized_len()..];

                miniz_oxide::inflate::decompress_to_vec(raw_data)
                    .map_err(|_| crate::Error::Decompress(compression))?
                    .into()
            }
        };

        Ok(Self {
            header,
            data: Slice::from(data),
        })
    }
}
