// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    checksum::ChecksumType,
    coding::{Decode, Encode},
    comparator::default_comparator,
    table::{Block, DataBlock},
    vlog::BlobFileId,
    CompressionType, InternalValue, KeyRange, SeqNo, Slice,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Read, Write};

macro_rules! read_u64 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)
            .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;

        let mut bytes = &bytes.value[..];
        bytes.read_u64::<LittleEndian>()?
    }};
}

macro_rules! read_u128 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)
            .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;

        let mut bytes = &bytes.value[..];
        bytes.read_u128::<LittleEndian>()?
    }};
}

pub const METADATA_HEADER_MAGIC: &[u8] = b"META";

// Note: `pub` for crate-internal use; parent `vlog` module is NOT
// exported from `lib.rs`, so this struct is not public API.
#[derive(Debug, PartialEq, Eq)]
pub struct Metadata {
    pub id: BlobFileId,

    /// Blob file format version (3 = V3, 4 = V4 with header CRC).
    pub version: u8,

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
            meta("blob_file_version", &[self.version]),
            meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
            meta("compression", &self.compression.encode_into_vec()),
            meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
            meta("created_at", &self.created_at.to_le_bytes()),
            meta("file_size", &self.total_compressed_bytes.to_le_bytes()),
            meta("id", &self.id.to_le_bytes()),
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
            return Err(crate::Error::InvalidHeader("BlobFileMeta"));
        }

        // TODO: Block::from_slice
        let block = Block::from_reader(reader, CompressionType::None)?;
        let block = DataBlock::new(block);

        // Metadata keys are always lexicographic, so use the default comparator.
        let cmp = default_comparator();

        let version = {
            let bytes = block
                .point_read(b"blob_file_version", SeqNo::MAX, &cmp)
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;
            *bytes
                .value
                .first()
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?
        };

        // Reject unknown versions early to catch corrupted or
        // future-incompatible metadata before downstream code
        // misinterprets header fields.
        match version {
            3 | 4 => {}
            _ => return Err(crate::Error::InvalidHeader("BlobFileMeta")),
        }

        let id = read_u64!(block, b"id", &cmp);
        let created_at = read_u128!(block, b"created_at", &cmp);
        let item_count = read_u64!(block, b"item_count", &cmp);
        let file_size = read_u64!(block, b"file_size", &cmp);
        let total_uncompressed_bytes = read_u64!(block, b"uncompressed_size", &cmp);

        let compression = {
            let bytes = block
                .point_read(b"compression", SeqNo::MAX, &cmp)
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?;

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        let key_range = KeyRange::new((
            block
                .point_read(b"key#min", SeqNo::MAX, &cmp)
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?
                .value,
            block
                .point_read(b"key#max", SeqNo::MAX, &cmp)
                .ok_or(crate::Error::InvalidHeader("BlobFileMeta"))?
                .value,
        ));

        Ok(Self {
            id,
            version,
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
    fn test_blob_file_meta_truncated_returns_err() {
        // Truncated metadata (just the magic header) must return Err, not panic
        let buf = Slice::from(METADATA_HEADER_MAGIC.to_vec());
        assert!(Metadata::from_slice(&buf).is_err());
    }

    /// Build a metadata block that is structurally valid but omits a required
    /// property (`compression`).  `from_slice` must return `Err`, not panic.
    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_blob_file_meta_missing_field_returns_err() {
        use crate::table::block::BlockType;
        use std::io::Write;

        fn meta(key: &str, value: &[u8]) -> InternalValue {
            InternalValue::from_components(key, value, 0, crate::ValueType::Value)
        }

        // Include all required fields EXCEPT `compression`
        #[rustfmt::skip]
        let meta_items = [
            meta("blob_file_version", &[4u8]),
            meta("checksum_type", &[u8::from(ChecksumType::Xxh3)]),
            // "compression" intentionally omitted
            meta("crate_version", env!("CARGO_PKG_VERSION").as_bytes()),
            meta("created_at", &1_234_567_890u128.to_le_bytes()),
            meta("file_size", &1024u64.to_le_bytes()),
            meta("id", &0u64.to_le_bytes()),
            meta("item_count", &100u64.to_le_bytes()),
            meta("key#max", b"z"),
            meta("key#min", b"a"),
            meta("uncompressed_size", &2048u64.to_le_bytes()),
        ];

        let encoded = DataBlock::encode_into_vec(&meta_items, 1, 0.0).unwrap();

        let mut buf = Vec::new();
        buf.write_all(METADATA_HEADER_MAGIC).unwrap();
        Block::write_into(&mut buf, &encoded, BlockType::Meta, CompressionType::None).unwrap();

        let buf = Slice::from(buf);
        let result = Metadata::from_slice(&buf);
        assert!(
            matches!(result, Err(crate::Error::InvalidHeader("BlobFileMeta"))),
            "expected Err(InvalidHeader(\"BlobFileMeta\")), got {result:?}",
        );
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn test_blob_file_meta_roundtrip() {
        let meta = Metadata {
            id: 0,
            version: 4,
            created_at: 1_234_567_890,
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
