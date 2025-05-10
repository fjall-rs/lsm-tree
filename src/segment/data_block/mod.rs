// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod forward_reader;
mod iter;

pub use iter::Iter;

use super::block::{
    binary_index::Reader as BinaryIndexReader, hash_index::Reader as HashIndexReader, Block,
    Encodable, Encoder, Trailer, TRAILER_START_MARKER,
};
use crate::clipping_iter::ClippingIter;
use crate::{InternalValue, SeqNo, ValueType};
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use forward_reader::ForwardReader;
use iter::{ParsedItem, ParsedSlice};
use std::io::Seek;
use std::ops::RangeBounds;
use std::{cmp::Reverse, io::Cursor};
use varint_rs::{VarintReader, VarintWriter};

impl Encodable<()> for InternalValue {
    fn encode_full_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        _state: &mut (),
    ) -> crate::Result<()> {
        // We encode restart markers as:
        // [value type] [seqno] [user key len] [user key] [value len] [value]
        // 1            2       3              4          5?           6?

        writer.write_u8(u8::from(self.key.value_type))?; // 1
        writer.write_u64_varint(self.key.seqno)?; // 2

        // NOTE: Truncation is okay and actually needed
        #[allow(clippy::cast_possible_truncation)]
        writer.write_u16_varint(self.key.user_key.len() as u16)?; // 3
        writer.write_all(&self.key.user_key)?; // 4

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            // NOTE: We know values are limited to 32-bit length
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32_varint(self.value.len() as u32)?; // 5
            writer.write_all(&self.value)?; // 6
        }

        Ok(())
    }

    fn encode_truncated_into<W: std::io::Write>(
        &self,
        writer: &mut W,
        _state: &mut (),
        shared_len: usize,
    ) -> crate::Result<()> {
        // We encode truncated values as:
        // [value type] [seqno] [shared prefix len] [rest key len] [rest key] [value len] [value]
        // 1            2       3                   4              5          6?          7?

        writer.write_u8(u8::from(self.key.value_type))?; // 1
        writer.write_u64_varint(self.key.seqno)?; // 2

        // TODO: maybe we can skip this varint altogether if prefix truncation = false
        writer.write_u16_varint(shared_len as u16)?; // 3

        let rest_len = self.key().len() - shared_len;
        writer.write_u16_varint(rest_len as u16)?; // 4

        let truncated_user_key = self
            .key
            .user_key
            .get(shared_len..)
            .expect("should be in bounds");

        writer.write_all(truncated_user_key)?; // 5

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            // NOTE: We know values are limited to 32-bit length
            #[allow(clippy::cast_possible_truncation)]
            writer.write_u32_varint(self.value.len() as u32)?; // 6
            writer.write_all(&self.value)?; // 7
        }

        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.key.user_key
    }
}

// TODO: allow disabling binary index (for meta block)
// -> saves space in metadata blocks
// -> point reads then need to use iter().find() to find stuff (which is fine)

macro_rules! unwrappy {
    ($x:expr) => {
        $x.expect("should read")

        // unsafe { $x.unwrap_unchecked() }
    };
}

/// Block that contains key-value pairs (user data)
#[derive(Clone)]
pub struct DataBlock {
    pub inner: Block,

    // Cached metadata
    restart_interval: u8,

    binary_index_step_size: u8,
    binary_index_offset: u32,
    binary_index_len: u32,

    hash_index_offset: u32,
    hash_index_len: u32,
}

impl DataBlock {
    #[must_use]
    pub fn new(inner: Block) -> Self {
        let trailer = Trailer::new(&inner);
        let mut reader = trailer.as_slice();

        let _item_count = unwrappy!(reader.read_u32::<LittleEndian>());

        let restart_interval = unwrappy!(reader.read_u8());

        let binary_index_step_size = unwrappy!(reader.read_u8());
        let binary_index_offset = unwrappy!(reader.read_u32::<LittleEndian>());
        let binary_index_len = unwrappy!(reader.read_u32::<LittleEndian>());

        let hash_index_offset = unwrappy!(reader.read_u32::<LittleEndian>());
        let hash_index_len = unwrappy!(reader.read_u32::<LittleEndian>());

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        Self {
            inner,

            restart_interval,

            binary_index_step_size,
            binary_index_offset,
            binary_index_len,

            hash_index_offset,
            hash_index_len,
        }
    }

    /// Access the inner raw bytes
    #[must_use]
    fn bytes(&self) -> &[u8] {
        &self.inner.data
    }

    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        Iter::new(self).map(|kv| kv.materialize(&self.inner.data))
    }

    #[allow(clippy::iter_without_into_iter)]
    pub fn scan(&self) -> impl Iterator<Item = InternalValue> + '_ {
        ForwardReader::new(self).map(|kv| kv.materialize(&self.inner.data))
    }

    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: &'a R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + 'a {
        let offset = 0; // TODO: range & seek to range start using binary index/hash index (first matching restart interval)
                        // TODO: and if range end, seek to range end as well (last matching restart interval)

        ClippingIter::new(
            Iter::new(self)
                // .with_offset(offset) // TODO:
                .map(|kv| kv.materialize(&self.inner.data)),
            range,
        )
    }

    fn get_key_at(&self, pos: usize) -> (&[u8], Reverse<SeqNo>) {
        let bytes = &self.inner.data;

        // NOTE: Skip value type
        let pos = pos + std::mem::size_of::<ValueType>();

        // SAFETY: pos is always retrieved from the binary index,
        // which we consider to be trustworthy
        #[warn(unsafe_code)]
        let mut cursor = Cursor::new(unsafe { bytes.get_unchecked(pos..) });

        let seqno = unwrappy!(cursor.read_u64_varint());
        let key_len: usize = unwrappy!(cursor.read_u16_varint()).into();

        let key_start = pos + cursor.position() as usize;
        let key_end = key_start + key_len;

        #[warn(unsafe_code)]
        let key = bytes.get(key_start..key_end).expect("should read");

        (key, Reverse(seqno))
    }

    /// Returns the binary index length (number of pointers).
    ///
    /// The number of pointers is equal to the number of restart intervals.
    #[must_use]
    pub fn binary_index_len(&self) -> u32 {
        self.binary_index_len
    }

    /// Returns the binary index offset.
    #[must_use]
    fn binary_index_offset(&self) -> u32 {
        self.binary_index_offset
    }

    /// Returns the binary index step size.
    ///
    /// The binary index can either store u16 or u32 pointers,
    /// depending on the size of the data block.
    ///
    /// Typically blocks are < 64K, so u16 pointers reduce the index
    /// size by half.
    #[must_use]
    fn binary_index_step_size(&self) -> u8 {
        self.binary_index_step_size
    }

    /// Returns the hash index offset.
    ///
    /// If 0, the hash index does not exist.
    #[must_use]
    fn hash_index_offset(&self) -> u32 {
        self.hash_index_offset
    }

    /// Returns the number of hash buckets.
    #[must_use]
    pub fn hash_bucket_count(&self) -> Option<u32> {
        if self.hash_index_offset() > 0 {
            Some(self.hash_index_len)
        } else {
            None
        }
    }

    fn get_binary_index_reader(&self) -> BinaryIndexReader {
        BinaryIndexReader::new(
            self.bytes(),
            self.binary_index_offset(),
            self.binary_index_len(),
            self.binary_index_step_size(),
        )
    }

    fn get_hash_index_reader(&self) -> Option<HashIndexReader> {
        self.hash_bucket_count()
            .map(|offset| HashIndexReader::new(&self.inner.data, self.hash_index_offset, offset))
    }

    /// Returns the amount of conflicts in the hash buckets.
    #[must_use]
    pub fn hash_bucket_conflict_count(&self) -> Option<usize> {
        self.get_hash_index_reader()
            .map(|reader| reader.conflict_count())
    }

    /// Returns the amount of empty hash buckets.
    #[must_use]
    pub fn hash_bucket_free_count(&self) -> Option<usize> {
        self.get_hash_index_reader()
            .map(|reader| reader.free_count())
    }

    /// Returns the amount of items in the block.
    #[must_use]
    pub fn len(&self) -> usize {
        Trailer::new(&self.inner).item_count()
    }

    /// Always returns false: a block is never empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    fn binary_search_for_offset(
        &self,
        binary_index: &BinaryIndexReader,
        needle: &[u8],
        seqno: Option<SeqNo>,
    ) -> Option<usize> {
        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        if let Some(seqno) = seqno {
            let seqno_cmp = Reverse(seqno - 1);

            while left < right {
                let mid = (left + right) / 2;

                let offset = binary_index.get(mid);

                if self.get_key_at(offset) <= (needle, seqno_cmp) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            if left == 0 {
                return None;
            }

            let offset = binary_index.get(left - 1);

            Some(offset)
        } else if self.restart_interval == 1 {
            while left < right {
                let mid = (left + right) / 2;

                let offset = binary_index.get(mid);

                if self.get_key_at(offset).0 < needle {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            Some(if left == 0 {
                binary_index.get(0)
            } else if left < binary_index.len() {
                binary_index.get(left)
            } else {
                binary_index.get(binary_index.len() - 1)
            })
        } else {
            while left < right {
                let mid = (left + right) / 2;

                let offset = binary_index.get(mid);

                if self.get_key_at(offset).0 < needle {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }

            Some(if left == 0 {
                binary_index.get(0)
            } else if left < binary_index.len() {
                binary_index.get(left - 1)
            } else {
                binary_index.get(binary_index.len() - 1)
            })
        }
    }

    fn parse_restart_item(reader: &mut Cursor<&[u8]>, offset: usize) -> Option<ParsedItem> {
        let value_type = unwrappy!(reader.read_u8());

        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let seqno = unwrappy!(reader.read_u64_varint());

        let key_len: usize = unwrappy!(reader.read_u16_varint()).into();
        let key_start = offset + reader.position() as usize;
        unwrappy!(reader.seek_relative(key_len as i64));

        let val_len: usize = if value_type == u8::from(ValueType::Value) {
            unwrappy!(reader.read_u32_varint()) as usize
        } else {
            0
        };
        let val_offset = offset + reader.position() as usize;
        unwrappy!(reader.seek_relative(val_len as i64));

        Some(if value_type == u8::from(ValueType::Value) {
            ParsedItem {
                value_type,
                seqno,
                prefix: None,
                key: ParsedSlice(key_start, key_start + key_len),
                value: Some(ParsedSlice(val_offset, val_offset + val_len)),
            }
        } else {
            ParsedItem {
                value_type,
                seqno,
                prefix: None,
                key: ParsedSlice(key_start, key_start + key_len),
                value: None, // TODO: enum value/tombstone, so value is not Option for values
            }
        })
    }

    fn parse_truncated_item(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem> {
        let value_type = unwrappy!(reader.read_u8());

        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let seqno = unwrappy!(reader.read_u64_varint());

        let shared_prefix_len: usize = unwrappy!(reader.read_u16_varint()).into();
        let rest_key_len: usize = unwrappy!(reader.read_u16_varint()).into();

        let key_offset = offset + reader.position() as usize;

        unwrappy!(reader.seek_relative(rest_key_len as i64));

        let val_len: usize = if value_type == u8::from(ValueType::Value) {
            unwrappy!(reader.read_u32_varint()) as usize
        } else {
            0
        };
        let val_offset = offset + reader.position() as usize;
        unwrappy!(reader.seek_relative(val_len as i64));

        Some(if value_type == u8::from(ValueType::Value) {
            ParsedItem {
                value_type,
                seqno,
                prefix: Some(ParsedSlice(
                    base_key_offset,
                    base_key_offset + shared_prefix_len,
                )),
                key: ParsedSlice(key_offset, key_offset + rest_key_len),
                value: Some(ParsedSlice(val_offset, val_offset + val_len)),
            }
        } else {
            ParsedItem {
                value_type,
                seqno,
                prefix: Some(ParsedSlice(
                    base_key_offset,
                    base_key_offset + shared_prefix_len,
                )),
                key: ParsedSlice(key_offset, key_offset + rest_key_len),
                value: None,
            }
        })
    }

    #[must_use]
    pub fn point_read(&self, needle: &[u8], seqno: Option<SeqNo>) -> Option<InternalValue> {
        let mut reader = ForwardReader::new(self);
        reader.point_read(needle, seqno)
    }

    pub fn encode_items(
        items: &[InternalValue],
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<Vec<u8>> {
        let first_key = &items
            .first()
            .expect("chunk should not be empty")
            .key
            .user_key;

        let mut serializer = Encoder::<'_, (), InternalValue>::new(
            items.len(),
            restart_interval,
            hash_index_ratio,
            first_key,
        );

        for item in items {
            serializer.write(item)?;
        }

        serializer.finish()
    }
}
