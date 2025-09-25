// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::io::{Read, Write};

/// Error during serialization
#[derive(Debug)]
pub enum EncodeError {
    /// I/O error
    Io(std::io::Error),
    InvalidCompressionLevel((crate::CompressionType, i32)),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "EncodeError(Io: {})", e),
            Self::InvalidCompressionLevel((compression_type, level)) => {
                write!(f, "EncodeError(InvalidCompressionLevel: {compression_type:?} level {level})")
            }
        }
    }
}

impl From<std::io::Error> for EncodeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::InvalidCompressionLevel(_) => None,
        }
    }
}

/// Error during deserialization
#[derive(Debug)]
pub enum DecodeError {
    /// I/O error
    Io(std::io::Error),

    /// Unsupported/outdated disk version
    InvalidVersion,

    /// Invalid enum tag
    InvalidTag((&'static str, u8)),

    /// Invalid block trailer
    InvalidTrailer,

    /// Invalid block header
    InvalidHeader(&'static str),

    /// UTF-8 error
    Utf8(std::str::Utf8Error),

    InvalidCompressionLevel((crate::CompressionType, i32)),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DecodeError({})",
            match self {
                Self::Io(e) => e.to_string(),
                Self::InvalidCompressionLevel((compression_type, level)) => format!("InvalidCompressionLevel: {compression_type:?} level {level}"),
                e => format!("{e:?}"),
            }
        )
    }
}

impl From<std::str::Utf8Error> for DecodeError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<std::io::Error> for DecodeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// Trait to serialize stuff
pub trait Encode {
    /// Serializes into writer.
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError>;

    /// Serializes into vector.
    #[allow(unused)]
    fn encode_into_vec(&self) -> Vec<u8> {
        let mut v = vec![];
        self.encode_into(&mut v).expect("cannot fail");
        v
    }
}

/// Trait to deserialize stuff
pub trait Decode {
    /// Deserializes from reader.
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError>
    where
        Self: Sized;
}
