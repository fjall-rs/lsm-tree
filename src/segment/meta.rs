// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, DataBlock};
use crate::{
    coding::Decode, segment::block::BlockType, CompressionType, KeyRange, SegmentId, SeqNo,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{fs::File, ops::Deref};

/// Nanosecond timestamp.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Timestamp(u128);

impl Deref for Timestamp {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Timestamp> for u128 {
    fn from(val: Timestamp) -> Self {
        val.0
    }
}

impl From<u128> for Timestamp {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
pub struct ParsedMeta {
    pub id: SegmentId,
    pub created_at: Timestamp,
    pub data_block_count: u64,
    pub index_block_count: u64,
    pub key_range: KeyRange,
    pub seqnos: (SeqNo, SeqNo),
    pub file_size: u64,
    pub item_count: u64,
    pub tombstone_count: u64,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,
}

macro_rules! read_u64 {
    ($block:expr, $name:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX)
            .unwrap_or_else(|| panic!("meta property {:?} should exist", $name));

        let mut bytes = &bytes.value[..];
        bytes.read_u64::<LittleEndian>()?
    }};
}

impl ParsedMeta {
    #[allow(clippy::expect_used, clippy::too_many_lines)]
    pub fn load_with_handle(file: &File, handle: &BlockHandle) -> crate::Result<Self> {
        let block = Block::from_file(file, *handle, CompressionType::None)?;

        if block.header.block_type != BlockType::Meta {
            return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            ))));
        }

        let block = DataBlock::new(block);

        #[allow(clippy::indexing_slicing)]
        {
            let table_version = block
                .point_read(b"v#table_version", SeqNo::MAX)
                .expect("Segment ID should exist")
                .value;

            assert_eq!(1, table_version.len(), "invalid table version byte array");

            assert_eq!(
                [3u8],
                &*table_version,
                "unspported table version {}",
                table_version[0],
            );
        }

        {
            let hash_type = block
                .point_read(b"#filter_hash_type", SeqNo::MAX)
                .expect("Segment ID should exist")
                .value;

            assert_eq!(
                b"xxh3",
                &*hash_type,
                "invalid hash type: {:?}",
                std::str::from_utf8(&hash_type),
            );
        }

        {
            let hash_type = block
                .point_read(b"#checksum_type", SeqNo::MAX)
                .expect("Segment ID should exist")
                .value;

            assert_eq!(
                b"xxh3",
                &*hash_type,
                "invalid checksum type: {:?}",
                std::str::from_utf8(&hash_type),
            );
        }

        let id = read_u64!(block, b"#id");
        let item_count = read_u64!(block, b"#item_count");
        let tombstone_count = read_u64!(block, b"#tombstone_count");
        let data_block_count = read_u64!(block, b"#data_block_count");
        let index_block_count = read_u64!(block, b"#index_block_count");
        let file_size = read_u64!(block, b"#size"); // TODO: 3.0.0 rename file_size

        let created_at = {
            let bytes = block
                .point_read(b"#created_at", SeqNo::MAX)
                .expect("Segment created_at should exist");

            let mut bytes = &bytes.value[..];
            bytes.read_u128::<LittleEndian>()?.into()
        };

        let key_range = KeyRange::new((
            block
                .point_read(b"#key#min", SeqNo::MAX)
                .expect("key min should exist")
                .value,
            block
                .point_read(b"#key#max", SeqNo::MAX)
                .expect("key max should exist")
                .value,
        ));

        let seqnos = {
            let min = {
                let bytes = block
                    .point_read(b"#seqno#min", SeqNo::MAX)
                    .expect("seqno min should exist")
                    .value;
                let mut bytes = &bytes[..];
                bytes.read_u64::<LittleEndian>()?
            };

            let max = {
                let bytes = block
                    .point_read(b"#seqno#max", SeqNo::MAX)
                    .expect("seqno max should exist")
                    .value;
                let mut bytes = &bytes[..];
                bytes.read_u64::<LittleEndian>()?
            };

            (min, max)
        };

        let data_block_compression = {
            let bytes = block
                .point_read(b"#compression#data", SeqNo::MAX)
                .expect("size should exist");

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        let index_block_compression = {
            let bytes = block
                .point_read(b"#compression#index", SeqNo::MAX)
                .expect("size should exist");

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        Ok(Self {
            id,
            created_at,
            data_block_count,
            index_block_count,
            key_range,
            seqnos,
            file_size,
            item_count,
            tombstone_count,
            data_block_compression,
            index_block_compression,
        })
    }
}
