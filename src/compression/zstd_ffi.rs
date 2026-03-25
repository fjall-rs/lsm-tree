// Copyright (c) 2025-present, Structured World Foundation
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! C FFI zstd backend via the `zstd` crate (libzstd bindings).
//!
//! This backend provides full compression levels 1–22 and dictionary
//! support. It requires a C compiler and cmake at build time.

use super::CompressionProvider;

/// C FFI zstd backend.
pub struct ZstdFfiProvider;

impl CompressionProvider for ZstdFfiProvider {
    fn compress(data: &[u8], level: i32) -> crate::Result<Vec<u8>> {
        zstd::bulk::compress(data, level).map_err(|e| crate::Error::Io(std::io::Error::other(e)))
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        zstd::bulk::decompress(data, capacity)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))
    }

    fn compress_with_dict(data: &[u8], level: i32, dict_raw: &[u8]) -> crate::Result<Vec<u8>> {
        let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dict_raw)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        compressor
            .compress(data)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))
    }

    fn decompress_with_dict(
        data: &[u8],
        dict_raw: &[u8],
        capacity: usize,
    ) -> crate::Result<Vec<u8>> {
        let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict_raw)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        decompressor
            .decompress(data, capacity)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))
    }
}
