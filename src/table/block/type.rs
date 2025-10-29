// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::DecodeError;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BlockType {
    Data,
    Index,
    Filter,
    Meta,
    Regions,
}

impl From<BlockType> for u8 {
    fn from(val: BlockType) -> Self {
        match val {
            BlockType::Data => 0,
            BlockType::Index => 1,
            BlockType::Filter => 2,
            BlockType::Meta => 3,
            BlockType::Regions => 4,
        }
    }
}

impl TryFrom<u8> for BlockType {
    type Error = DecodeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Index),
            2 => Ok(Self::Filter),
            3 => Ok(Self::Meta),
            4 => Ok(Self::Regions),
            _ => Err(DecodeError::InvalidTag(("BlockType", value))),
        }
    }
}
