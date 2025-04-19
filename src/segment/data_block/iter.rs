// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::DataBlock;
use crate::{key::InternalKey, InternalValue, SeqNo, Slice};
use std::io::Cursor;

#[derive(Default, Debug)]
struct LoScanner {
    offset: usize,
    remaining_in_interval: usize,
    base_key_offset: Option<usize>,
}

#[derive(Debug)]
struct HiScanner {
    offset: usize,
    ptr_idx: usize,
    stack: Vec<usize>,
    base_key_offset: Option<usize>,
}

/// Double-ended iterator over data blocks
pub struct Iter<'a> {
    block: &'a DataBlock,
    restart_interval: usize,

    lo_scanner: LoScanner,
    hi_scanner: HiScanner,
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct ParsedSlice(pub usize, pub usize);

#[derive(Debug)]
pub struct ParsedItem {
    pub value_type: u8,
    pub seqno: SeqNo,
    pub prefix: Option<ParsedSlice>,
    pub key: ParsedSlice,
    pub value: Option<ParsedSlice>,
}

impl ParsedItem {
    pub fn materialize(&self, bytes: &Slice) -> InternalValue {
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.key.0..self.key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.key.0..self.key.1)
        };
        let key = InternalKey::new(
            key,
            self.seqno,
            self.value_type.try_into().expect("should work"),
        );

        let value = self
            .value
            .as_ref()
            .map_or_else(Slice::empty, |v| bytes.slice(v.0..v.1));

        InternalValue { key, value }
    }
}

impl<'a> Iter<'a> {
    pub fn new(block: &'a DataBlock) -> Self {
        let restart_interval = block.restart_interval.into();
        let binary_index_len = block.binary_index_len as usize;

        Self {
            block,

            restart_interval,

            lo_scanner: LoScanner::default(),

            hi_scanner: HiScanner {
                offset: 0,
                ptr_idx: binary_index_len,
                stack: Vec::new(),
                base_key_offset: None,
            },
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.lo_scanner.offset = offset;
        self
    }

    fn parse_restart_item(
        block: &DataBlock,
        offset: &mut usize,
        base_key_offset: &mut Option<usize>,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = DataBlock::parse_restart_item(&mut reader, *offset)?;

        *offset += reader.position() as usize;
        *base_key_offset = Some(item.key.0);

        Some(item)
    }

    fn parse_truncated_item(
        block: &DataBlock,
        offset: &mut usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = DataBlock::parse_truncated_item(&mut reader, *offset, base_key_offset)?;

        *offset += reader.position() as usize;

        Some(item)
    }

    fn consume_stack_top(&mut self) -> Option<ParsedItem> {
        if let Some(offset) = self.hi_scanner.stack.pop() {
            if self.lo_scanner.offset > 0 && offset < self.lo_scanner.offset {
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
            && self.lo_scanner.offset >= self.hi_scanner.offset
        {
            return None;
        }

        let is_restart = self.lo_scanner.remaining_in_interval == 0;

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

        self.lo_scanner.remaining_in_interval -= 1;

        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset >= self.hi_scanner.offset
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
