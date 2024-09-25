// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::CompressionType;
use value_log::Compressor;

#[derive(Copy, Clone, Debug)]
pub struct MyCompressor(pub(crate) CompressionType);

impl Default for MyCompressor {
    fn default() -> Self {
        Self(CompressionType::None)
    }
}

impl Compressor for MyCompressor {
    fn compress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        Ok(match self.0 {
            CompressionType::None => bytes.into(),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => lz4_flex::compress_prepend_size(bytes),

            #[cfg(feature = "miniz")]
            CompressionType::Miniz(lvl) => miniz_oxide::deflate::compress_to_vec(bytes, lvl),

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(level) => zstd::bulk::compress(bytes, level)?,
        })
    }

    fn decompress(&self, bytes: &[u8]) -> value_log::Result<Vec<u8>> {
        match self.0 {
            CompressionType::None => Ok(bytes.into()),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => {
                lz4_flex::decompress_size_prepended(bytes).map_err(|_| value_log::Error::Decompress)
            }

            #[cfg(feature = "miniz")]
            CompressionType::Miniz(_) => miniz_oxide::inflate::decompress_to_vec(bytes)
                .map_err(|_| value_log::Error::Decompress),

            #[cfg(feature = "zstd")]
            CompressionType::Zstd(_) => zstd::bulk::decompress(
                bytes,
                // TODO: assuming 4GB output size max
                u32::MAX as usize,
            )
            .map_err(|_| value_log::Error::Decompress),
        }
    }
}
