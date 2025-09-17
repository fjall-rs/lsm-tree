// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::vlog::Compressor;
use crate::CompressionType;
use std::io::{Read, Write};
#[cfg(feature = "zlib")]
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression as ZCompression};

#[derive(Copy, Clone, Debug)]
pub struct MyCompressor(pub(crate) CompressionType);

impl Default for MyCompressor {
    fn default() -> Self {
        Self(CompressionType::None)
    }
}

impl Compressor for MyCompressor {
    fn compress(&self, bytes: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(match self.0 {
            CompressionType::None => bytes.into(),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => lz4_flex::compress_prepend_size(bytes),

            #[cfg(feature = "zlib")]
            CompressionType::Zlib(level) => {
                let lvl = level.min(9) as u32;
                let mut e = ZlibEncoder::new(Vec::new(), ZCompression::new(lvl));
                e.write_all(bytes)?;
                e.finish()?
            }
        })
    }

    fn decompress(&self, bytes: &[u8]) -> crate::Result<Vec<u8>> {
        match self.0 {
            CompressionType::None => Ok(bytes.into()),

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(bytes)
                .map_err(|_| crate::Error::Decompress(self.0)),

            #[cfg(feature = "zlib")]
            CompressionType::Zlib(_level) => {
                let mut d = ZlibDecoder::new(bytes);
                let mut out = Vec::new();
                d.read_to_end(&mut out)
                    .map_err(|_| crate::Error::Decompress(self.0))?;
                Ok(out)
            }
        }
    }
}
