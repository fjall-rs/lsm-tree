// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{Checksum, CompressionType};

/// Represents errors that can occur in the LSM-tree
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error
    Io(std::io::Error),

    /// Decompression failed
    Decompress(CompressionType),

    /// Invalid or unparsable data format version
    InvalidVersion(u8),

    /// Some required files could not be recovered from disk
    Unrecoverable,

    /// Checksum mismatch
    ChecksumMismatch {
        /// Checksum of loaded block
        got: Checksum,

        /// Checksum that was saved in block header
        expected: Checksum,
    },

    /// Blob frame header CRC mismatch (V4 format).
    /// Distinct from `ChecksumMismatch` which covers data payload checksums.
    HeaderCrcMismatch {
        /// CRC recomputed from header fields
        recomputed: u32,

        /// CRC stored in the blob frame header
        stored: u32,
    },

    /// Invalid enum tag
    InvalidTag((&'static str, u8)),

    /// Invalid block trailer
    InvalidTrailer,

    /// Invalid block header
    InvalidHeader(&'static str),

    /// Data size (decompressed, on-disk, or requested) is invalid or exceeds a safety limit
    DecompressedSizeTooLarge {
        /// Size associated with the data being processed. This may come from
        /// on-disk/in-memory metadata (e.g., header, block/value handle) or be
        /// derived from caller input (e.g., a requested key or value length),
        /// and may be zero, invalid, or over the configured limit.
        declared: u64,

        /// Maximum allowed size for the data or request being processed
        limit: u64,
    },

    /// UTF-8 error
    Utf8(std::str::Utf8Error),

    /// Range tombstone block decode failure.
    RangeTombstoneDecode {
        /// Which field or validation failed (e.g. `start_len`, `start`, `seqno`, `interval`)
        field: &'static str,

        /// Byte offset within the block to the start of the field whose decoding failed
        /// (captured before reading bytes for that field).
        offset: u64,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LsmTreeError: {self:?}")
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<sfa::Error> for Error {
    fn from(value: sfa::Error) -> Self {
        match value {
            sfa::Error::Io(e) => Self::from(e),
            sfa::Error::ChecksumMismatch { got, expected } => {
                log::error!("Archive ToC checksum mismatch");
                Self::ChecksumMismatch {
                    got: got.into(),
                    expected: expected.into(),
                }
            }
            sfa::Error::InvalidHeader => {
                log::error!("Invalid archive header");
                Self::Unrecoverable
            }
            sfa::Error::InvalidVersion => {
                log::error!("Invalid archive version");
                Self::Unrecoverable
            }
            sfa::Error::UnsupportedChecksumType => {
                log::error!("Invalid archive checksum type");
                Self::Unrecoverable
            }
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Tree result
pub type Result<T> = std::result::Result<T, Error>;
