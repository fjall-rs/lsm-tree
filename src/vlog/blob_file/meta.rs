// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, Encode},
    table::{Block, DataBlock},
    CompressionType, InternalValue, KeyRange, SeqNo, Slice,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Read, Write};

macro_rules! read_u64 {
    ($block:expr, $name:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX)
            .unwrap_or_else(|| panic!("meta property {:?} should exist", $name));

        let mut bytes = &bytes.value[..];
        bytes.read_u64::<LittleEndian>()?
    }};
}

macro_rules! read_u128 {
    ($block:expr, $name:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX)
            .unwrap_or_else(|| panic!("meta property {:?} should exist", $name));

        let mut bytes = &bytes.value[..];
        bytes.read_u128::<LittleEndian>()?
    }};
}

pub const METADATA_HEADER_MAGIC: &[u8] = b"META";

#[derive(Debug, PartialEq, Eq)]
pub struct Metadata {
    pub created_at: u128,

    /// Number of KV-pairs in the blob file
    pub item_count: u64,

    /// compressed size in bytes (on disk) (without metadata or trailer)
    pub total_compressed_bytes: u64,

    /// true size in bytes (if no compression were used)
    pub total_uncompressed_bytes: u64,

    /// Key range
    pub key_range: KeyRange,

    /// Compression type used for all blobs in this file
    pub compression: CompressionType,
}

impl Metadata {
    pub fn encode_into<W: Write>(&self, writer: &mut W) -> crate::Result<()> {
        fn meta(key: &str, value: &[u8]) -> InternalValue {
            InternalValue::from_components(key, value, 0, crate::ValueType::Value)
        }

        // Write header
        writer.write_all(METADATA_HEADER_MAGIC)?;

        #[rustfmt::skip]
        let meta_items = [
            meta("checksum_type", b"xxh3"),
            meta("compression", &self.compression.encode_into_vec()),
            meta("created_at", &self.created_at.to_le_bytes()),
            meta("file_size", &self.total_compressed_bytes.to_le_bytes()),
            meta("item_count", &self.item_count.to_le_bytes()),
            meta("key#max", self.key_range.max()),
            meta("key#min", self.key_range.min()),
            meta("uncompressed_size", &self.total_uncompressed_bytes.to_le_bytes()),
        ];

        // NOTE: Just to make sure the items are definitely sorted
        #[cfg(debug_assertions)]
        {
            let is_sorted = meta_items.iter().is_sorted_by_key(|kv| &kv.key);
            assert!(is_sorted, "meta items not sorted correctly");
        }

        // TODO: no binary index
        let buf = DataBlock::encode_into_vec(&meta_items, 1, 0.0)?;

        Block::write_into(
            writer,
            &buf,
            crate::table::block::BlockType::Meta,
            CompressionType::None,
        )?;

        Ok(())
    }

    pub fn from_slice(slice: &Slice) -> crate::Result<Self> {
        let reader = &mut &slice[..];

        // Check header
        let mut magic = [0u8; METADATA_HEADER_MAGIC.len()];
        reader.read_exact(&mut magic)?;

        if magic != METADATA_HEADER_MAGIC {
            return Err(crate::Error::Decode(crate::DecodeError::InvalidHeader(
                "BlobFileMeta",
            )));
        }

        // TODO: Block::from_slice
        let block = Block::from_reader(reader, CompressionType::None)?;
        let block = DataBlock::new(block);

        let created_at = read_u128!(block, b"created_at");
        let item_count = read_u64!(block, b"item_count");
        let file_size = read_u64!(block, b"file_size");
        let total_uncompressed_bytes = read_u64!(block, b"uncompressed_size");

        let compression = {
            let bytes = block
                .point_read(b"compression", SeqNo::MAX)
                .expect("size should exist");

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        let key_range = KeyRange::new((
            block
                .point_read(b"key#min", SeqNo::MAX)
                .expect("key min should exist")
                .value,
            block
                .point_read(b"key#max", SeqNo::MAX)
                .expect("key max should exist")
                .value,
        ));

        Ok(Self {
            created_at,
            compression,
            item_count,
            total_compressed_bytes: file_size,
            total_uncompressed_bytes,
            key_range,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn test_blob_file_meta_roundtrip() {
        let meta = Metadata {
            created_at: 1234567890,
            compression: CompressionType::None,
            item_count: 100,
            total_compressed_bytes: 1024,
            total_uncompressed_bytes: 2048,
            key_range: KeyRange::new((b"a".into(), b"z".into())),
        };

        let mut buf = Vec::new();
        meta.encode_into(&mut buf).unwrap();
        let buf = Slice::from(buf);

        let meta2 = Metadata::from_slice(&buf).unwrap();
        assert_eq!(meta, meta2);
    }
}
