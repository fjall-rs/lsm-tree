// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::super_segment::block::{BlockOffset, Encodable};
use value_log::UserKey;
use varint_rs::VarintWriter;

/// Points to a block on file
#[derive(Clone, Debug, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct NewKeyedBlockHandle {
    /// Key of last item in block
    pub end_key: UserKey,

    /// Position of block in file
    pub offset: BlockOffset,

    /// Size of block in bytes
    pub size: u32,
}

impl Ord for NewKeyedBlockHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

impl PartialOrd for NewKeyedBlockHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.offset.cmp(&other.offset))
    }
}

impl PartialEq for NewKeyedBlockHandle {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl Encodable<BlockOffset> for NewKeyedBlockHandle {
    fn encode_full_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut BlockOffset,
    ) -> crate::Result<()> {
        // We encode restart markers as:
        // [offset] [size] [key len] [end key]
        // 1        2      3         4

        writer.write_u64_varint(*self.offset)?; // 1
        writer.write_u32_varint(self.size)?; // 2
        writer.write_u16_varint(self.end_key.len() as u16)?; // 3
        writer.write_all(&self.end_key)?; // 4

        *state = BlockOffset(*self.offset + u64::from(self.size));

        Ok(())
    }

    fn encode_truncated_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        state: &mut BlockOffset,
        shared_len: usize,
    ) -> crate::Result<()> {
        // We encode truncated handles as:
        // [size] [shared prefix len] [rest key len] [rest key]

        writer.write_u32_varint(self.size)?;

        // TODO: maybe we can skip this varint altogether if prefix truncation = false
        writer.write_u16_varint(shared_len as u16)?;

        // NOTE: We can safely cast to u16, because keys are u16 long max
        #[allow(clippy::cast_possible_truncation)]
        let rest_len = self.end_key.len() - shared_len;

        writer.write_u16_varint(rest_len as u16)?;

        let truncated_user_key = self.end_key.get(shared_len..).expect("should be in bounds");
        writer.write_all(truncated_user_key)?;

        *state += u64::from(self.size);

        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.end_key
    }
}
