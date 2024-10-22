// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    segment::{block::ItemSize, value_block::BlockOffset},
    value::UserKey,
    Slice,
};
use std::io::{Read, Write};
use varint_rs::{VarintReader, VarintWriter};

/// Points to a block on file
#[derive(Clone, Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct KeyedBlockHandle {
    /// Key of last item in block
    pub end_key: UserKey,

    /// Position of block in file
    pub offset: BlockOffset,
}

impl KeyedBlockHandle {
    #[must_use]
    pub fn new<K: Into<Slice>>(end_key: K, offset: BlockOffset) -> Self {
        Self {
            end_key: end_key.into(),
            offset,
        }
    }
}

impl ItemSize for KeyedBlockHandle {
    fn size(&self) -> usize {
        std::mem::size_of::<BlockOffset>() + self.end_key.len()
    }
}

impl PartialEq for KeyedBlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}
impl Eq for KeyedBlockHandle {}

impl std::hash::Hash for KeyedBlockHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(*self.offset);
    }
}

impl PartialOrd for KeyedBlockHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyedBlockHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.end_key, self.offset).cmp(&(&other.end_key, other.offset))
    }
}

impl Encode for KeyedBlockHandle {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_u64_varint(*self.offset)?;

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16_varint(self.end_key.len() as u16)?;
        writer.write_all(&self.end_key)?;

        Ok(())
    }
}

impl Decode for KeyedBlockHandle {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError>
    where
        Self: Sized,
    {
        let offset = reader.read_u64_varint()?;

        let key_len = reader.read_u16_varint()?;
        let mut key = vec![0; key_len.into()];
        reader.read_exact(&mut key)?;

        Ok(Self {
            offset: BlockOffset(offset),
            end_key: Slice::from(key),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_block_size() {
        let items = [
            KeyedBlockHandle::new("abcd", BlockOffset(5)),
            KeyedBlockHandle::new("efghij", BlockOffset(10)),
        ];
        assert_eq!(26, items.size());
    }
}
