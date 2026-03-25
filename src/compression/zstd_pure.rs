// Copyright (c) 2025-present, Structured World Foundation
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! Pure Rust zstd backend via the `structured-zstd` crate.
//!
//! This backend requires no C compiler or system libraries — it compiles
//! with `cargo build` alone.
//!
//! # Limitations
//!
//! - Compression uses the `Fastest` level regardless of the requested
//!   level (higher levels are not yet implemented in structured-zstd).
//! - Dictionary compression is not yet supported (returns an error).
//! - Dictionary decompression is supported.
//! - Decompression throughput is ~2–3.5x slower than the C reference.

use super::CompressionProvider;
use std::io::Read;

/// Read at most `capacity` bytes from `reader` into a pre-allocated buffer,
/// then probe for excess data. Returns the filled portion of the buffer.
///
/// The limit is enforced _during_ decode — the Vec never grows beyond
/// `capacity`, preventing unbounded allocation from crafted frames.
fn bounded_read(reader: &mut impl Read, capacity: usize) -> crate::Result<Vec<u8>> {
    let mut output = vec![0u8; capacity];
    let mut filled = 0;

    loop {
        let dest = output
            .get_mut(filled..)
            .ok_or(crate::Error::DecompressedSizeTooLarge {
                declared: filled as u64,
                limit: capacity as u64,
            })?;
        match reader.read(dest) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) => return Err(crate::Error::Io(e)),
        }
    }

    // Probe for excess data: if the reader still has bytes after filling
    // the buffer, the frame exceeds capacity.
    let mut probe = [0u8; 1];
    match reader.read(&mut probe) {
        Ok(0) => {}
        Ok(_) => {
            return Err(crate::Error::DecompressedSizeTooLarge {
                declared: (filled + 1) as u64,
                limit: capacity as u64,
            });
        }
        Err(e) => return Err(crate::Error::Io(e)),
    }

    output.truncate(filled);
    Ok(output)
}

/// Pure Rust zstd backend.
pub struct ZstdPureProvider;

impl CompressionProvider for ZstdPureProvider {
    fn compress(data: &[u8], _level: i32) -> crate::Result<Vec<u8>> {
        // structured-zstd currently only supports Fastest level;
        // higher levels are accepted but silently map to Fastest.
        let compressed = structured_zstd::encoding::compress_to_vec(
            std::io::Cursor::new(data),
            structured_zstd::encoding::CompressionLevel::Fastest,
        );
        Ok(compressed)
    }

    fn decompress(data: &[u8], capacity: usize) -> crate::Result<Vec<u8>> {
        let mut decoder = structured_zstd::decoding::StreamingDecoder::new(data)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        bounded_read(&mut decoder, capacity)
    }

    fn compress_with_dict(_data: &[u8], _level: i32, _dict_raw: &[u8]) -> crate::Result<Vec<u8>> {
        Err(crate::Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "zstd dictionary compression is not yet supported by the pure Rust backend \
             (structured-zstd); use the `zstd` feature for dictionary compression",
        )))
    }

    fn decompress_with_dict(
        data: &[u8],
        dict_raw: &[u8],
        capacity: usize,
    ) -> crate::Result<Vec<u8>> {
        // NOTE: Dictionary is re-parsed from raw bytes on every call.
        // The C FFI backend has the same per-call overhead (Decompressor::with_dictionary
        // also re-initializes). Caching would require adding precompiled dictionary
        // state to the CompressionProvider trait, which is a Phase 2 optimization.
        let dict = structured_zstd::decoding::Dictionary::decode_dict(dict_raw)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        // FrameDecoder supports dictionaries (unlike StreamingDecoder).
        let mut decoder = structured_zstd::decoding::FrameDecoder::new();
        decoder
            .add_dict(dict)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;
        decoder
            .init(data)
            .map_err(|e| crate::Error::Io(std::io::Error::other(e)))?;

        bounded_read(&mut decoder, capacity)
    }
}
