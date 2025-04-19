// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod iter;

pub use iter::Iter;

use super::block::{
    binary_index::Reader as BinaryIndexReader, hash_index::Reader as HashIndexReader, Block,
    Encodable, Encoder, Trailer, TRAILER_START_MARKER,
};
use crate::clipping_iter::ClippingIter;
use crate::super_segment::util::compare_prefixed_slice;
use crate::{InternalValue, SeqNo, ValueType};
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
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
        // $x.expect("should read")

        unsafe { $x.unwrap_unchecked() }
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

    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        range: &'a R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + 'a {
        let offset = 0; // TODO: range & seek to range start using binary index/hash index (first matching restart interval)
                        // TODO: and if range end, seek to range end as well (last matching restart interval)

        ClippingIter::new(
            Iter::new(self)
                .with_offset(offset)
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

    fn scan(&self, needle: &[u8], seqno: Option<SeqNo>, offset: usize) -> Option<InternalValue> {
        let bytes = self.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(offset..) });

        loop {
            let head = Self::parse_restart_item(&mut reader, offset)?;

            let key = &bytes[head.key.0..head.key.1];
            let base_key_offset = head.key.0;

            match key.cmp(needle) {
                std::cmp::Ordering::Equal => {
                    // TODO: maybe return early if past seqno
                    let should_skip = seqno.is_some_and(|watermark| head.seqno >= watermark);

                    if !should_skip {
                        let kv = head.materialize(&self.inner.data);
                        return Some(kv);
                    }
                }
                std::cmp::Ordering::Greater => {
                    // Already passed needle
                    return None;
                }
                std::cmp::Ordering::Less => {
                    // Continue to next KV
                }
            }

            for _ in 0..(self.restart_interval - 1) {
                let kv = Self::parse_truncated_item(&mut reader, offset, base_key_offset)?;

                let cmp_result = if let Some(prefix) = &kv.prefix {
                    let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
                    let rest_key = unsafe { bytes.get_unchecked(kv.key.0..kv.key.1) };
                    compare_prefixed_slice(prefix, rest_key, needle)
                } else {
                    let key = unsafe { bytes.get_unchecked(kv.key.0..kv.key.1) };
                    key.cmp(needle)
                };

                match cmp_result {
                    std::cmp::Ordering::Equal => {
                        // TODO: maybe return early if past seqno
                        let should_skip = seqno.is_some_and(|watermark| kv.seqno >= watermark);

                        if !should_skip {
                            let kv = kv.materialize(&self.inner.data);
                            return Some(kv);
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        // Already passed needle
                        return None;
                    }
                    std::cmp::Ordering::Less => {
                        // Continue to next KV
                    }
                }
            }
        }
    }

    /// Reads an item by key from the block, if it exists.
    pub fn point_read(&self, needle: &[u8], seqno: Option<SeqNo>) -> Option<InternalValue> {
        let binary_index = self.get_binary_index_reader();

        // NOTE: Try hash index if it exists
        if let Some(lookup) = self
            .get_hash_index_reader()
            .map(|reader| reader.get(needle))
        {
            use super::block::hash_index::Lookup::{Conflicted, Found, NotFound};

            match lookup {
                Found(bucket_value) => {
                    let offset = binary_index.get(usize::from(bucket_value));
                    return self.scan(needle, seqno, offset);
                }
                NotFound => {
                    return None;
                }
                Conflicted => {
                    // NOTE: Fallback to binary search
                }
            }
        }

        let offset = self.binary_search_for_offset(&binary_index, needle, seqno)?;

        self.scan(needle, seqno, offset)
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        super_segment::{
            block::{BlockOffset, Checksum, Header},
            Block,
        },
        InternalValue, Slice,
        ValueType::{Tombstone, Value},
    };
    use std::cmp::Ordering::{Equal, Greater, Less};
    use test_log::test;

    #[test]
    fn v3_compare_prefixed_slice() {
        assert_eq!(Equal, compare_prefixed_slice(b"", b"", b""));

        assert_eq!(Greater, compare_prefixed_slice(b"a", b"", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"a", b""));
        assert_eq!(Greater, compare_prefixed_slice(b"b", b"a", b"a"));
        assert_eq!(Greater, compare_prefixed_slice(b"a", b"b", b"a"));

        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"y"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"a", b"", b"yyy"));
        assert_eq!(Less, compare_prefixed_slice(b"yyyy", b"a", b"yyyyb"));
        assert_eq!(Less, compare_prefixed_slice(b"yyy", b"b", b"yyyyb"));
    }

    #[test]
    fn v3_data_block_point_read_one() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "pla:earth:fact",
            "eaaaaaaaaarth",
            0,
            crate::ValueType::Value,
        )];

        let bytes = DataBlock::encode_items(&items, 16, 0.0)?;
        let serialized_len = bytes.len();

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(!data_block.is_empty());
        assert_eq!(data_block.inner.size(), serialized_len);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, None),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                "pla:earth:fact",
                "eaaaaaaaaarth",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "pla:jupiter:fact",
                "Jupiter is big",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "pla:jupiter:mass",
                "Massive",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "pla:jupiter:name",
                "Jupiter",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, crate::ValueType::Value),
            InternalValue::from_components(
                "pla:saturn:fact",
                "Saturn is pretty big",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, crate::ValueType::Value),
            InternalValue::from_components("pla:venus:fact", "", 1, crate::ValueType::Tombstone),
            InternalValue::from_components(
                "pla:venus:fact",
                "Venus exists",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components("pla:venus:name", "Venus", 0, crate::ValueType::Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], b"", 23_523_531_241_241_242, Value),
            InternalValue::from_components([0], b"", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], [], 5, Value),
            InternalValue::from_components([0], [], 4, Tombstone),
            InternalValue::from_components([0], [], 3, Value),
            InternalValue::from_components([0], [], 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_3() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::from([
                    255, 255, 255, 255, 5, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                    255, 255, 255, 255, 255,
                ]),
                Slice::from([0, 0, 192]),
                18_446_744_073_701_163_007,
                Tombstone,
            ),
            InternalValue::from_components(
                Slice::from([255, 255, 255, 255, 255, 255, 0]),
                Slice::from([]),
                0,
                Value,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 5, 1.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        assert_eq!(items, *data_block.iter().collect::<Vec<_>>(),);

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_4() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::new(&[0]),
                Slice::new(&[]),
                3_834_029_160_418_063_669,
                Value,
            ),
            InternalValue::from_components(Slice::new(&[0]), Slice::new(&[]), 127, Tombstone),
            InternalValue::from_components(
                Slice::new(&[53, 53, 53]),
                Slice::new(&[]),
                18_446_744_073_709_551_615,
                Tombstone,
            ),
            InternalValue::from_components(
                Slice::new(&[255]),
                Slice::new(&[]),
                18_446_744_069_414_584_831,
                Tombstone,
            ),
            InternalValue::from_components(Slice::new(&[255, 255]), Slice::new(&[]), 47, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 1.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for item in data_block.iter() {
            eprintln!("{item:?}");
        }

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"b", b"b", 2, Value),
            InternalValue::from_components(b"c", b"c", 1, Value),
            InternalValue::from_components(b"d", b"d", 65, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, None),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_dense_mvcc_with_hash() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            Some(items.first().cloned().unwrap()),
            data_block.point_read(b"a", None)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(b"b", None)
        );
        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], None)
        );
        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], None)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], None)
        );
        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_3() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], Some(SeqNo::MAX))
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], Some(SeqNo::MAX))
        );
        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_3_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], None)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], None)
        );
        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_dense_mvcc_no_hash() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_shadowing() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert!(data_block
            .point_read(b"pla:venus:fact", None)
            .expect("should exist")
            .is_tombstone());

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_one_time() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "pla:saturn:fact",
            "Saturn is pretty big",
            0,
            Value,
        )];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len()
        );

        assert_eq!(data_block.iter().collect::<Vec<_>>(), items);

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        assert_eq!(items, *data_block.iter().collect::<Vec<_>>(),);

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_dense() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "pla:saturn:fact",
            "Saturn is pretty big",
            0,
            Value,
        )];

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(items.len(), {
            #[allow(clippy::suspicious_map)]
            data_block.iter().count()
        });

        assert_eq!(items, *data_block.iter().collect::<Vec<_>>(),);

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_rev() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(items.len(), {
            #[allow(clippy::suspicious_map)]
            data_block.iter().rev().count()
        });

        assert_eq!(
            items.into_iter().rev().collect::<Vec<_>>(),
            data_block.iter().rev().collect::<Vec<_>>(),
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_ping_pong() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        {
            let mut iter = data_block.iter();

            assert_eq!(b"pla:saturn:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:venus:name", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(b"pla:saturn:name", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:venus:fact", &*iter.next_back().unwrap().key.user_key);

            let last = iter.next().unwrap().key;
            assert_eq!(b"pla:venus:fact", &*last.user_key);
            assert_eq!(Tombstone, last.value_type);
            assert_eq!(1, last.seqno);
        }

        {
            let mut iter = data_block.iter();

            assert_eq!(b"pla:venus:name", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(
                b"pla:saturn:fact",
                &*iter
                    .next()
                    .inspect(|v| {
                        eprintln!("{:?}", String::from_utf8_lossy(&v.key.user_key));
                    })
                    .unwrap()
                    .key
                    .user_key
            );
            assert_eq!(b"pla:venus:fact", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(b"pla:saturn:name", &*iter.next().unwrap().key.user_key);

            let last = iter.next_back().unwrap().key;
            assert_eq!(b"pla:venus:fact", &*last.user_key);
            assert_eq!(Tombstone, last.value_type);
            assert_eq!(1, last.seqno);
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_range() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.range(&((b"pla:venus:" as &[u8])..)).count()
            },
            3,
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_range_rev() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block
                    .range(&((b"pla:venus:" as &[u8])..))
                    .rev()
                    .count()
            },
            3,
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_small_hash_ratio() -> crate::Result<()> {
        let items = (0u64..254)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        // NOTE: If >0.0, buckets are at least 1
        let bytes = DataBlock::encode_items(&items, 1, 0.0001)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_just_enough_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..254)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_too_many_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..255)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_way_too_many_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..1_000)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_no_hash_index() -> crate::Result<()> {
        let items = (0u64..1)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_consume_last_back() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        {
            let mut iter = data_block.iter();
            assert_eq!(b"pla:earth:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:mass", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:name", &*iter.next().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:radius",
                &*iter.next_back().unwrap().key.user_key
            );
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
        }

        {
            let mut iter = data_block.iter();
            assert_eq!(b"pla:earth:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:mass", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:name", &*iter.next().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:radius",
                &*iter.next_back().unwrap().key.user_key
            );
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_consume_last_forwards() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        {
            let mut iter = data_block.iter().rev();
            assert_eq!(b"pla:earth:fact", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:fact",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:mass",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:name",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(b"pla:jupiter:radius", &*iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
        }

        {
            let mut iter = data_block.iter().rev();
            assert_eq!(b"pla:earth:fact", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:fact",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:mass",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:name",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(b"pla:jupiter:radius", &*iter.next().unwrap().key.user_key);
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
        }

        Ok(())
    }
}
