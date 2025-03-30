use super::{encoder::TRAILER_SIZE, DataBlock};
use crate::{
    coding::DecodeError, super_segment::data_block::encoder::TERMINATOR_MARKER, InternalValue,
    Slice, ValueType,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{Cursor, Seek};
use varint_rs::VarintReader;

/// Double-ended iterator over data blocks
pub struct Iter {
    block: DataBlock,

    cursor: usize,
    idx: usize,
    restart_interval: usize,

    base_key: Option<Slice>,
}

impl Iter {
    pub fn new(block: DataBlock) -> Self {
        let bytes = &block.inner.data;
        let mut reader = &bytes[bytes.len() - TRAILER_SIZE..];

        let _item_count = reader.read_u32::<LittleEndian>().expect("should read") as usize;
        let restart_interval = reader.read_u8().expect("should read") as usize;

        Self {
            block,
            cursor: 0,
            idx: 0,
            restart_interval,

            base_key: None,
        }
    }
}

impl Iterator for Iter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let is_restart = (self.idx % self.restart_interval) == 0;

        let bytes = &self.block.inner.data;
        let mut cursor = Cursor::new(&bytes[self.cursor..]);

        if is_restart {
            let parsed = fail_iter!(DataBlock::parse_restart_item(&mut cursor));

            if parsed.value_type == TERMINATOR_MARKER {
                return None;
            }

            let value_type: ValueType = fail_iter!(parsed
                .value_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("ValueType", parsed.value_type))));

            let seqno = parsed.seqno;

            let key_start = self.cursor + parsed.key_start;
            let key_end = key_start + parsed.key_len;
            let key = bytes.slice(key_start..key_end);

            let val_len: usize = if value_type == ValueType::Value {
                fail_iter!(cursor.read_u32_varint()) as usize
            } else {
                0
            };
            let val_offset = self.cursor + cursor.position() as usize;
            fail_iter!(cursor.seek_relative(val_len as i64));

            self.cursor += cursor.position() as usize;
            self.idx += 1;
            self.base_key = Some(key.clone());

            Some(Ok(if value_type == ValueType::Value {
                let value = bytes.slice(val_offset..(val_offset + val_len));
                InternalValue::from_components(key, value, seqno, value_type)
            } else {
                InternalValue::from_components(key, b"", seqno, value_type)
            }))
        } else {
            let value_type = fail_iter!(cursor.read_u8());

            if value_type == TERMINATOR_MARKER {
                return None;
            }

            let value_type: ValueType = fail_iter!(value_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type))));

            let seqno = fail_iter!(cursor.read_u64_varint());

            let shared_prefix_len: usize = fail_iter!(cursor.read_u16_varint()).into();
            let rest_key_len: usize = fail_iter!(cursor.read_u16_varint()).into();

            let key_offset = self.cursor + cursor.position() as usize;

            // SAFETY: We always start with a restart item, so the base key is always set to Some(_)
            #[warn(unsafe_code)]
            let base_key = unsafe { self.base_key.as_ref().unwrap_unchecked() };

            let prefix_part = &base_key[..shared_prefix_len];
            let rest_key = &bytes[key_offset..(key_offset + rest_key_len)];
            fail_iter!(cursor.seek_relative(rest_key_len as i64));

            let val_len: usize = if value_type == ValueType::Value {
                fail_iter!(cursor.read_u32_varint()) as usize
            } else {
                0
            };
            let val_offset = self.cursor + cursor.position() as usize;
            fail_iter!(cursor.seek_relative(val_len as i64));

            let key = if shared_prefix_len == 0 {
                bytes.slice(key_offset..(key_offset + rest_key_len))
            } else {
                // Stitch key
                Slice::fused(prefix_part, rest_key)
            };

            self.cursor += cursor.position() as usize;
            self.idx += 1;

            Some(Ok(if value_type == ValueType::Value {
                let value = bytes.slice(val_offset..(val_offset + val_len));
                InternalValue::from_components(key, value, seqno, value_type)
            } else {
                InternalValue::new_tombstone(key, seqno)
            }))
        }
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
