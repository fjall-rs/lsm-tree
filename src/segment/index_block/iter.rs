// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    forward_reader::{ForwardReader, ParsedItem},
    IndexBlock,
};
use std::io::Cursor;

#[derive(Debug)]
struct HiScanner {
    offset: usize,
    ptr_idx: usize,
    stack: Vec<usize>, // TODO: SmallVec?
    base_key_offset: Option<usize>,
}

/// Double-ended iterator over index blocks
pub struct Iter<'a> {
    block: &'a IndexBlock,
    restart_interval: usize,

    lo_scanner: ForwardReader<'a>,
    hi_scanner: HiScanner,
}

impl<'a> Iter<'a> {
    #[must_use]
    pub fn new(block: &'a IndexBlock) -> Self {
        let restart_interval = block.restart_interval.into();
        let binary_index_len = block.binary_index_len as usize;

        Self {
            block,

            restart_interval,

            lo_scanner: ForwardReader::new(block),

            /* lo_scanner: LoScanner::default(), */
            hi_scanner: HiScanner {
                offset: 0,
                ptr_idx: binary_index_len,
                stack: Vec::new(),
                base_key_offset: None,
            },
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.lo_scanner = self.lo_scanner.with_offset(offset);
        self
    }

    fn parse_restart_item(
        block: &IndexBlock,
        offset: &mut usize,
        base_key_offset: &mut Option<usize>,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = IndexBlock::parse_restart_item(&mut reader, *offset)?;

        *offset += reader.position() as usize;
        *base_key_offset = Some(item.end_key.0);

        Some(item)
    }

    fn parse_truncated_item(
        block: &IndexBlock,
        offset: &mut usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = IndexBlock::parse_truncated_item(&mut reader, *offset, base_key_offset)?;

        *offset += reader.position() as usize;

        Some(item)
    }

    fn consume_stack_top(&mut self) -> Option<ParsedItem> {
        if let Some(offset) = self.hi_scanner.stack.pop() {
            if self.lo_scanner.offset() > 0 && offset < self.lo_scanner.offset() {
                return None;
            }

            self.hi_scanner.offset = offset;

            let is_restart = self.hi_scanner.stack.is_empty();

            if is_restart {
                Self::parse_restart_item(
                    self.block,
                    &mut self.hi_scanner.offset,
                    &mut self.hi_scanner.base_key_offset,
                )
            } else {
                Self::parse_truncated_item(
                    self.block,
                    &mut self.hi_scanner.offset,
                    self.hi_scanner.base_key_offset.expect("should exist"),
                )
            }
        } else {
            None
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = ParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset() >= self.hi_scanner.offset
        {
            return None;
        }

        /* let is_restart = self.lo_scanner.remaining_in_interval == 0;

        let item = if is_restart {
            self.lo_scanner.remaining_in_interval = self.restart_interval;

            Self::parse_restart_item(
                self.block,
                &mut self.lo_scanner.offset,
                &mut self.lo_scanner.base_key_offset,
            )
        } else {
            Self::parse_truncated_item(
                self.block,
                &mut self.lo_scanner.offset,
                self.lo_scanner.base_key_offset.expect("should exist"),
            )
        };

        self.lo_scanner.remaining_in_interval -= 1; */

        let item = self.lo_scanner.next();

        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset() >= self.hi_scanner.offset
        {
            return None;
        }

        item
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(top) = self.consume_stack_top() {
            return Some(top);
        }

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_scanner.ptr_idx == usize::MAX {
            return None;
        }

        self.hi_scanner.ptr_idx = self.hi_scanner.ptr_idx.wrapping_sub(1);

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_scanner.ptr_idx == usize::MAX {
            return None;
        }

        let binary_index = self.block.get_binary_index_reader();

        {
            self.hi_scanner.offset = binary_index.get(self.hi_scanner.ptr_idx);
            let offset = self.hi_scanner.offset;

            if Self::parse_restart_item(
                self.block,
                &mut self.hi_scanner.offset,
                &mut self.hi_scanner.base_key_offset,
            )
            .is_some()
            {
                self.hi_scanner.stack.push(offset);
            }
        }

        for _ in 1..self.restart_interval {
            let offset = self.hi_scanner.offset;

            if Self::parse_truncated_item(
                self.block,
                &mut self.hi_scanner.offset,
                self.hi_scanner.base_key_offset.expect("should exist"),
            )
            .is_some()
            {
                self.hi_scanner.stack.push(offset);
            }
        }

        if self.hi_scanner.stack.is_empty() {
            return None;
        }

        self.consume_stack_top()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        segment::{block::Header, Block, BlockOffset, KeyedBlockHandle},
        Checksum,
    };
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_iter_simple() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());
        assert_eq!(index_block.iter().count(), items.len());
        assert_eq!(index_block.iter().rev().count(), items.len());

        {
            let mut iter = index_block.iter();

            assert_eq!(b"b", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"def", &**iter.next().unwrap().end_key());
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
        }

        {
            let mut iter = index_block.iter().rev();

            assert_eq!(b"def", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"b", &**iter.next().unwrap().end_key());
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
        }

        {
            let mut iter = index_block.iter();

            assert_eq!(b"b", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"def", &**iter.next_back().unwrap().end_key());
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
        }

        {
            let mut iter = index_block.iter().rev();

            assert_eq!(b"def", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"b", &**iter.next_back().unwrap().end_key());
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_iter_exhaust() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());
        assert_eq!(index_block.iter().count(), items.len());
        assert_eq!(index_block.iter().rev().count(), items.len());

        {
            let mut iter = index_block.iter();

            assert_eq!(b"b", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"def", &**iter.next().unwrap().end_key());
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
            assert!(iter.next_back().is_none());
        }

        {
            let mut iter = index_block.iter().rev();

            assert_eq!(b"def", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"b", &**iter.next().unwrap().end_key());
            assert!(iter.next_back().is_none());
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }

        {
            let mut iter = index_block.iter();

            assert_eq!(b"b", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"def", &**iter.next_back().unwrap().end_key());
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
            assert!(iter.next_back().is_none());
        }

        {
            let mut iter = index_block.iter().rev();

            assert_eq!(b"def", &**iter.next().unwrap().end_key());
            assert_eq!(b"bcdef", &**iter.next().unwrap().end_key());
            assert_eq!(b"b", &**iter.next_back().unwrap().end_key());
            assert!(iter.next_back().is_none());
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
        }

        Ok(())
    }
}
