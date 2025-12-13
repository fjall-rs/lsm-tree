// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::io::{Read, Write};

/// Trait to serialize stuff
pub trait Encode {
    /// Serializes into writer.
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error>;

    /// Serializes into vector.
    fn encode_into_vec(&self) -> Vec<u8> {
        let mut v = vec![];
        #[expect(
            clippy::expect_used,
            reason = "encoding into a vec is not expected to fail"
        )]
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
