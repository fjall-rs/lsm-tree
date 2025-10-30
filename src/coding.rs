// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Checksum;
use std::io::{Read, Write};

/// Error during serialization
#[derive(Debug)]
pub enum EncodeError {
    /// I/O error
    Io(std::io::Error),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EncodeError({})",
            match self {
                Self::Io(e) => e.to_string(),
            }
        )
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
        }
    }
}

/// Trait to serialize stuff
pub trait Encode {
    /// Serializes into writer.
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError>;

    /// Serializes into vector.
    fn encode_into_vec(&self) -> Vec<u8> {
        let mut v = vec![];
        self.encode_into(&mut v).expect("cannot fail");
        v
    }
}

/// Trait to deserialize stuff
pub trait Decode {
    /// Deserializes from reader.
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error>
    where
        Self: Sized;
}
