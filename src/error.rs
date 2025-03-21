// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{version::Version, Checksum, CompressionType, DecodeError, EncodeError};

/// Represents errors that can occur in the LSM-tree
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// I/O error
    Io(std::io::Error),

    /// Serialization failed
    Encode(EncodeError),

    /// Deserialization failed
    Decode(DecodeError),

    /// Decompression failed
    Decompress(CompressionType),

    /// Invalid or unparsable data format version
    InvalidVersion(Version),

    /// Some required segments could not be recovered from disk
    Unrecoverable,

    /// Invalid checksum value (got, expected)
    InvalidChecksum((Checksum, Checksum)),

    /// Value log errors
    ValueLog(value_log::Error),
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
            Self::Encode(e) => Some(e),
            Self::Decode(e) => Some(e),
            Self::ValueLog(e) => Some(e),
            Self::Decompress(_)
            | Self::InvalidVersion(_)
            | Self::Unrecoverable
            | Self::InvalidChecksum(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<EncodeError> for Error {
    fn from(value: EncodeError) -> Self {
        Self::Encode(value)
    }
}

impl From<DecodeError> for Error {
    fn from(value: DecodeError) -> Self {
        Self::Decode(value)
    }
}

impl From<value_log::Error> for Error {
    fn from(value: value_log::Error) -> Self {
        Self::ValueLog(value)
    }
}

/// Tree result
pub type Result<T> = std::result::Result<T, Error>;
