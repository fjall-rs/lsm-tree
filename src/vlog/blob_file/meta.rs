// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    KeyRange,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub const METADATA_HEADER_MAGIC: &[u8] = &[b'V', b'L', b'O', b'G', b'S', b'M', b'D', 1];

#[derive(Debug)]
pub struct Metadata {
    /// Number of KV-pairs in the blob file
    pub item_count: u64,

    /// compressed size in bytes (on disk) (without the fixed size trailer)
    pub compressed_bytes: u64,

    /// true size in bytes (if no compression were used)
    pub total_uncompressed_bytes: u64,

    /// Key range
    pub key_range: KeyRange,
}

impl Encode for Metadata {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        // Write header
        writer.write_all(METADATA_HEADER_MAGIC)?;

        writer.write_u64::<BigEndian>(self.item_count)?;
        writer.write_u64::<BigEndian>(self.compressed_bytes)?;
        writer.write_u64::<BigEndian>(self.total_uncompressed_bytes)?;

        self.key_range.encode_into(writer)?;

        Ok(())
    }
}

impl Decode for Metadata {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        // Check header
        let mut magic = [0u8; METADATA_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != METADATA_HEADER_MAGIC {
            return Err(DecodeError::InvalidHeader("BlobFileMeta"));
        }

        let item_count = reader.read_u64::<BigEndian>()?;
        let compressed_bytes = reader.read_u64::<BigEndian>()?;
        let total_uncompressed_bytes = reader.read_u64::<BigEndian>()?;

        let key_range = KeyRange::decode_from(reader)?;

        Ok(Self {
            item_count,
            compressed_bytes,
            total_uncompressed_bytes,
            key_range,
        })
    }
}
