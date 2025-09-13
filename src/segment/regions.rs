// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle};
use crate::{
    coding::{Decode, Encode},
    segment::{block::BlockType, DataBlock},
    CompressionType, InternalValue, SeqNo, UserValue,
};
use std::fs::File;

/// The regions block stores offsets to the different segment disk file "regions"
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ParsedRegions {
    pub tli: BlockHandle,
    pub index: Option<BlockHandle>,
    pub filter: Option<BlockHandle>,
    pub metadata: BlockHandle,
}

impl ParsedRegions {
    pub fn load_with_handle(file: &File, handle: &BlockHandle) -> crate::Result<Self> {
        let block = Block::from_file(file, *handle, CompressionType::None)?;

        if block.header.block_type != BlockType::Regions {
            return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            ))));
        }

        let block = DataBlock::new(block);

        let tli = {
            // NOTE: Top-level index block is always written
            #[allow(clippy::expect_used)]
            let bytes = block
                .point_read(b"tli", SeqNo::MAX)
                .expect("TLI handle should exist");

            let mut bytes = &bytes.value[..];
            BlockHandle::decode_from(&mut bytes)
        }?;

        let metadata = {
            // NOTE: Metadata block is always written
            #[allow(clippy::expect_used)]
            let bytes = block
                .point_read(b"meta", SeqNo::MAX)
                .expect("Metadata handle should exist");

            let mut bytes = &bytes.value[..];
            BlockHandle::decode_from(&mut bytes)
        }?;

        let index = {
            match block.point_read(b"index", SeqNo::MAX) {
                Some(bytes) if !bytes.value.is_empty() => {
                    let mut bytes = &bytes.value[..];
                    Some(BlockHandle::decode_from(&mut bytes))
                }
                _ => None,
            }
        }
        .transpose()?;

        let filter = {
            match block.point_read(b"filter", SeqNo::MAX) {
                Some(bytes) if !bytes.value.is_empty() => {
                    let mut bytes = &bytes.value[..];
                    Some(BlockHandle::decode_from(&mut bytes))
                }
                _ => None,
            }
        }
        .transpose()?;

        Ok(Self {
            tli,
            index,
            filter,
            metadata,
        })
    }

    pub fn encode_into_vec(&self) -> crate::Result<Vec<u8>> {
        fn region(key: &str, value: impl Into<UserValue>) -> InternalValue {
            InternalValue::from_components(key, value, 0, crate::ValueType::Value)
        }

        let items = [
            region(
                "filter",
                match self.filter {
                    Some(handle) => handle.encode_into_vec(),
                    None => vec![],
                },
            ),
            region(
                "index",
                match self.index {
                    Some(handle) => handle.encode_into_vec(),
                    None => vec![],
                },
            ),
            region("meta", self.metadata.encode_into_vec()),
            region("tli", self.tli.encode_into_vec()),
        ];

        #[cfg(debug_assertions)]
        {
            let mut sorted_copy = items.clone();
            sorted_copy.sort();

            // Just to make sure the items are definitely sorted
            assert_eq!(items, sorted_copy, "region items not sorted correctly");
        }

        // TODO: no binary index
        DataBlock::encode_into_vec(&items, 1, 0.0)
    }
}
