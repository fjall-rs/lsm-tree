use super::DataBlock;
use crate::{key::InternalKey, InternalValue, SeqNo, Slice};
use std::io::Cursor;

/// Double-ended iterator over data blocks
pub struct Iter<'a> {
    block: &'a DataBlock,

    cursor: usize,
    remaining_in_interval: usize,
    restart_interval: usize,

    lo_watermark: usize,

    // base_key: Option<&'a [u8]>,
    base_key_offset: Option<usize>,

    hi_ptr_idx: usize,
    hi_stack: Vec<usize>,
    // TODO: refactor into two members: LoScanner and HiScanner
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

        let value = if let Some(value) = &self.value {
            bytes.slice(value.0..value.1)
        } else {
            Slice::empty()
        };

        InternalValue { key, value }
    }
}

impl<'a> Iter<'a> {
    pub fn new(block: &'a DataBlock) -> Self {
        let restart_interval = block.restart_interval.into();
        let binary_index_len = block.binary_index_len as usize;

        Self {
            block,

            cursor: 0,
            remaining_in_interval: 0,
            restart_interval,

            lo_watermark: 0,

            // base_key: None, //  TODO: remove
            base_key_offset: None,

            hi_ptr_idx: binary_index_len,
            hi_stack: Vec::new(),
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.lo_watermark = offset;
        self
    }

    // TODO: refactor together with deserialize and point_read
    // skip should return the basic info, and rename to deserialize
    // rename deserialize to materialize by using the return type of deserialize
    /*   fn skip_restart_item(&mut self) -> crate::Result<bool> {
        let bytes = &self.block.inner.data;

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(self.cursor..) });

        let parsed = DataBlock::parse_restart_head(&mut reader)?;

        if parsed.value_type == TRAILER_START_MARKER {
            return Ok(false);
        }

        let value_type: ValueType = parsed
            .value_type
            .try_into()
            .map_err(|()| DecodeError::InvalidTag(("ValueType", parsed.value_type)))?;

        let key_start = self.cursor + parsed.key_start;
        let key_end = key_start + parsed.key_len;
        let key = bytes.slice(key_start..key_end);

        let val_len: usize = if value_type == ValueType::Value {
            reader.read_u32_varint()? as usize
        } else {
            0
        };
        reader.seek_relative(val_len as i64)?;

        self.cursor += reader.position() as usize;
        self.base_key = Some(key);

        Ok(true)
    } */

    // TODO: refactor together with deserialize and point_read
    // skip should return the basic info, and rename to deserialize
    // rename deserialize to materialize by using the return type of deserialize
    /*  fn skip_truncated_item(&mut self) -> crate::Result<bool> {
        let bytes = &self.block.inner.data;

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(self.cursor..) });

        let value_type = reader.read_u8()?;

        if value_type == TRAILER_START_MARKER {
            return Ok(false);
        }

        let value_type: ValueType = value_type
            .try_into()
            .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))?;

        let _seqno = reader.read_u64_varint()?;

        let _shared_prefix_len: usize = reader.read_u16_varint()?.into();
        let rest_key_len: usize = reader.read_u16_varint()?.into();

        reader.seek_relative(rest_key_len as i64)?;

        let val_len: usize = if value_type == ValueType::Value {
            reader.read_u32_varint()? as usize
        } else {
            0
        };
        reader.seek_relative(val_len as i64)?;

        self.cursor += reader.position() as usize;

        Ok(true)
    } */

    fn parse_restart_item(&mut self, offset: usize) -> Option<ParsedItem> {
        let bytes = &self.block.inner.data;

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(offset..) });

        let Some(item) = DataBlock::parse_restart_item(&mut reader, offset) else {
            return None;
        };

        self.cursor += reader.position() as usize;
        self.base_key_offset = Some(item.key.0);

        Some(item)
    }

    fn parse_truncated_item(&mut self, offset: usize) -> Option<ParsedItem> {
        let bytes = &self.block.inner.data;

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(offset..) });

        let Some(item) = DataBlock::parse_truncated_item(
            &mut reader,
            offset,
            self.base_key_offset.expect("should exist"),
        ) else {
            return None;
        };

        self.cursor += reader.position() as usize;

        Some(item)
    }

    /* fn consume_stack_top(&mut self) -> crate::Result<Option<InternalValue>> {
        if let Some(offset) = self.hi_stack.pop() {
            if self.lo_watermark > 0 && offset <= self.lo_watermark {
                return Ok(None);
            }

            self.cursor = offset;

            // TODO: pop from stack, check if offset < self.cursor, then also make sure to terminate forwards iteration
            // TODO: probably need a lo_cursor

            let is_restart = self.hi_stack.is_empty();

            if is_restart {
                self.deserialize_restart_item()
            } else {
                self.deserialize_truncated_item()
            }
        } else {
            Ok(None)
        }
    } */
}

impl Iterator for Iter<'_> {
    type Item = ParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        let is_restart = self.remaining_in_interval == 0;

        self.cursor = self.lo_watermark;

        let item = if is_restart {
            self.remaining_in_interval = self.restart_interval;
            self.parse_restart_item(self.lo_watermark)
        } else {
            self.parse_truncated_item(self.lo_watermark)
        };

        self.lo_watermark = self.cursor;
        self.remaining_in_interval -= 1;

        item
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
        /* if let Some(top) = fail_iter!(self.consume_stack_top()) {
            return Some(Ok(top));
        }

        self.hi_ptr_idx = self.hi_ptr_idx.wrapping_sub(1);

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_ptr_idx == usize::MAX {
            return None;
        }

        let binary_index = self.block.get_binary_index_reader();

        {
            let offset = binary_index.get(self.hi_ptr_idx);
            self.cursor = offset;

            if fail_iter!(self.skip_restart_item()) {
                self.hi_stack.push(offset);
            }
        }

        for _ in 1..self.restart_interval {
            let cursor = self.cursor;

            if fail_iter!(self.skip_truncated_item()) {
                self.hi_stack.push(cursor);
            }
        }

        if self.hi_stack.is_empty() {
            return None;
        }

        self.consume_stack_top().transpose() */
    }
}
