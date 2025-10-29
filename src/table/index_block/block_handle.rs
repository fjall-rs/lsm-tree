// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::UserKey;
use crate::{
    coding::{Decode, DecodeError, Encode, EncodeError},
    table::{
        block::{BlockOffset, Decodable, Encodable, TRAILER_START_MARKER},
        index_block::IndexBlockParsedItem,
        util::SliceIndexes,
    },
    unwrap,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Seek};
use varint_rs::{VarintReader, VarintWriter};

/// Points to a block on file
#[derive(Copy, Clone, Debug, Default, Eq)]
pub struct BlockHandle {
    /// Position of block in file
    offset: BlockOffset,

    /// Size of block in bytes
    size: u32,
} // TODO: 3.0.0 ^---- maybe u64

impl BlockHandle {
    #[must_use]
    pub fn new(offset: BlockOffset, size: u32) -> Self {
        Self { offset, size }
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        self.size
    }

    #[must_use]
    pub fn offset(&self) -> BlockOffset {
        self.offset
    }
}

impl PartialEq for BlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl Ord for BlockHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl PartialOrd for BlockHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Encode for BlockHandle {
    fn encode_into<W: std::io::Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        writer.write_u64_varint(*self.offset)?;
        writer.write_u32_varint(self.size)?;
        Ok(())
    }
}

impl Decode for BlockHandle {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, DecodeError>
    where
        Self: Sized,
    {
        let offset = reader.read_u64_varint()?;
        let size = reader.read_u32_varint()?;

        Ok(Self {
            offset: BlockOffset(offset),
            size,
        })
    }
}

/// Points to a block on file
#[derive(Clone, Debug, Eq)]
pub struct KeyedBlockHandle {
    /// Key of last item in block
    end_key: UserKey,

    inner: BlockHandle,
}

impl AsRef<BlockHandle> for KeyedBlockHandle {
    fn as_ref(&self) -> &BlockHandle {
        &self.inner
    }
}

impl KeyedBlockHandle {
    #[must_use]
    pub fn into_inner(self) -> BlockHandle {
        self.inner
    }

    #[must_use]
    pub fn new(end_key: UserKey, offset: BlockOffset, size: u32) -> Self {
        Self {
            end_key,
            inner: BlockHandle::new(offset, size),
        }
    }

    pub fn shift(&mut self, delta: BlockOffset) {
        self.inner.offset += delta;
    }

    #[must_use]
    pub fn size(&self) -> u32 {
        self.inner.size()
    }

    #[must_use]
    pub fn offset(&self) -> BlockOffset {
        self.inner.offset()
    }

    #[must_use]
    pub fn end_key(&self) -> &UserKey {
        &self.end_key
    }

    #[must_use]
    pub fn into_end_key(self) -> UserKey {
        self.end_key
    }
}

impl PartialEq for KeyedBlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.offset() == other.offset()
    }
}

impl Ord for KeyedBlockHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset().cmp(&other.offset())
    }
}

impl PartialOrd for KeyedBlockHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Encodable<BlockOffset> for KeyedBlockHandle {
    fn encode_full_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut BlockOffset,
    ) -> crate::Result<()> {
        // We encode restart markers as:
        // [marker=0] [offset] [size] [key len] [end key]
        // 1          2        3      4         5

        writer.write_u8(0)?; // 1

        // TODO: maybe move these behind the key
        self.inner.encode_into(writer)?; // 2, 3

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(self.end_key.len() as u16)?; // 4
        writer.write_all(&self.end_key)?; // 5

        *state = BlockOffset(*self.offset() + u64::from(self.size()));

        Ok(())
    }

    fn encode_truncated_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut BlockOffset,
        shared_len: usize,
    ) -> crate::Result<()> {
        // We encode truncated handles as:
        // [marker=0] [size] [shared prefix len] [rest key len] [rest key]
        // 1          2      3                   4              5

        writer.write_u8(0)?; // 1

        writer.write_u32_varint(self.size())?; // 2

        // TODO: maybe we can skip this varint altogether if prefix truncation = false
        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(shared_len as u16)?; // 3

        let rest_len = self.end_key.len() - shared_len;

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 long max")]
        writer.write_u16_varint(rest_len as u16)?; // 4

        let truncated_user_key = self.end_key.get(shared_len..).expect("should be in bounds");
        writer.write_all(truncated_user_key)?; // 5

        *state += u64::from(self.size());

        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.end_key
    }
}

impl Decodable<IndexBlockParsedItem> for KeyedBlockHandle {
    fn parse_full(reader: &mut Cursor<&[u8]>, offset: usize) -> Option<IndexBlockParsedItem> {
        let marker = unwrap!(reader.read_u8());

        if marker == TRAILER_START_MARKER {
            return None;
        }

        let file_offset = unwrap!(reader.read_u64_varint());
        let size = unwrap!(reader.read_u32_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        let key_start = offset + reader.position() as usize;

        unwrap!(reader.seek_relative(key_len as i64));

        Some(IndexBlockParsedItem {
            prefix: None,
            end_key: SliceIndexes(key_start, key_start + key_len),
            offset: BlockOffset(file_offset),
            size,
        })
    }

    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
    ) -> Option<&'a [u8]> {
        let marker = unwrap!(reader.read_u8());

        if marker == TRAILER_START_MARKER {
            return None;
        }

        let _file_offset = unwrap!(reader.read_u64_varint());
        let _size = unwrap!(reader.read_u32_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        let key_start = offset + reader.position() as usize;

        unwrap!(reader.seek_relative(key_len as i64));

        data.get(key_start..(key_start + key_len))
    }

    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
    ) -> Option<IndexBlockParsedItem> {
        unimplemented!()
    }
}
