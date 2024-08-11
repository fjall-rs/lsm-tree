// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::io::{Read, Write};

/// Error during serialization
#[derive(Debug)]
pub enum SerializeError {
    /// I/O error
    Io(std::io::Error),
}

impl std::fmt::Display for SerializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SerializeError({})",
            match self {
                Self::Io(e) => e.to_string(),
            }
        )
    }
}

impl From<std::io::Error> for SerializeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Error during deserialization
#[derive(Debug)]
pub enum DeserializeError {
    /// I/O error
    Io(std::io::Error),

    Utf8(std::str::Utf8Error),

    /// Invalid enum tag
    InvalidTag((&'static str, u8)),

    InvalidTrailer,

    /// Invalid block header
    InvalidHeader(&'static str),
}

impl std::fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DeserializeError({})",
            match self {
                Self::Io(e) => e.to_string(),
                e => format!("{e:?}"),
            }
        )
    }
}

impl From<std::io::Error> for DeserializeError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<std::str::Utf8Error> for DeserializeError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

/// Trait to serialize stuff
pub trait Serializable {
    /// Serialize to bytes
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError>;
}

/// Trait to deserialize stuff
pub trait Deserializable {
    /// Deserialize from bytes
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError>
    where
        Self: Sized;
}
