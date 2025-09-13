// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod iter;

pub use iter::Iter;

use super::block::{
    binary_index::Reader as BinaryIndexReader, hash_index::Reader as HashIndexReader, Block,
    Decodable, Decoder, Encodable, Encoder, ParsedItem, Trailer, TRAILER_START_MARKER,
};
use crate::key::InternalKey;
use crate::segment::block::hash_index::{MARKER_CONFLICT, MARKER_FREE};
use crate::segment::util::{compare_prefixed_slice, SliceIndexes};
use crate::{unwrap, InternalValue, SeqNo, Slice, ValueType};
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;
use std::io::Seek;
use varint_rs::{VarintReader, VarintWriter};

impl Decodable<DataBlockParsedItem> for InternalValue {
    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
    ) -> Option<&'a [u8]> {
        let value_type = unwrap!(reader.read_u8());

        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let _seqno = unwrap!(reader.read_u64_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        let key_start = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(key_len as i64));

        data.get(key_start..(key_start + key_len))
    }

    fn parse_full(reader: &mut Cursor<&[u8]>, offset: usize) -> Option<DataBlockParsedItem> {
        let value_type = unwrap!(reader.read_u8());

        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let seqno = unwrap!(reader.read_u64_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        let key_start = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(key_len as i64));

        let val_len: usize = if value_type == u8::from(ValueType::Value) {
            unwrap!(reader.read_u32_varint()) as usize
        } else {
            0
        };
        let val_offset = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(val_len as i64));

        Some(if value_type == u8::from(ValueType::Value) {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: None,
                key: SliceIndexes(key_start, key_start + key_len),
                value: Some(SliceIndexes(val_offset, val_offset + val_len)),
            }
        } else {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: None,
                key: SliceIndexes(key_start, key_start + key_len),
                value: None, // TODO: enum value/tombstone, so value is not Option for values
            }
        })
    }

    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
    ) -> Option<DataBlockParsedItem> {
        let value_type = unwrap!(reader.read_u8());

        if value_type == TRAILER_START_MARKER {
            return None;
        }

        let seqno = unwrap!(reader.read_u64_varint());

        let shared_prefix_len: usize = unwrap!(reader.read_u16_varint()).into();
        let rest_key_len: usize = unwrap!(reader.read_u16_varint()).into();

        let key_offset = offset + reader.position() as usize;

        unwrap!(reader.seek_relative(rest_key_len as i64));

        let val_len: usize = if value_type == u8::from(ValueType::Value) {
            unwrap!(reader.read_u32_varint()) as usize
        } else {
            0
        };
        let val_offset = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(val_len as i64));

        Some(if value_type == u8::from(ValueType::Value) {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: Some(SliceIndexes(
                    base_key_offset,
                    base_key_offset + shared_prefix_len,
                )),
                key: SliceIndexes(key_offset, key_offset + rest_key_len),
                value: Some(SliceIndexes(val_offset, val_offset + val_len)),
            }
        } else {
            DataBlockParsedItem {
                value_type,
                seqno,
                prefix: Some(SliceIndexes(
                    base_key_offset,
                    base_key_offset + shared_prefix_len,
                )),
                key: SliceIndexes(key_offset, key_offset + rest_key_len),
                value: None,
            }
        })
    }
}

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

#[derive(Debug)]
pub struct DataBlockParsedItem {
    pub value_type: u8,
    pub seqno: SeqNo,
    pub prefix: Option<SliceIndexes>,
    pub key: SliceIndexes,
    pub value: Option<SliceIndexes>,
}

impl ParsedItem<InternalValue> for DataBlockParsedItem {
    fn compare_key(&self, needle: &[u8], bytes: &[u8]) -> std::cmp::Ordering {
        if let Some(prefix) = &self.prefix {
            let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
            let rest_key = unsafe { bytes.get_unchecked(self.key.0..self.key.1) };
            compare_prefixed_slice(prefix, rest_key, needle)
        } else {
            let key = unsafe { bytes.get_unchecked(self.key.0..self.key.1) };
            key.cmp(needle)
        }
    }

    fn key_offset(&self) -> usize {
        self.key.0
    }

    fn materialize(&self, bytes: &Slice) -> InternalValue {
        // NOTE: We consider the prefix and key slice indexes to be trustworthy
        #[allow(clippy::indexing_slicing)]
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
            // NOTE: Value type is (or should be) checked when reading it
            #[allow(clippy::expect_used)]
            self.value_type.try_into().expect("should work"),
        );

        let value = self
            .value
            .as_ref()
            .map_or_else(Slice::empty, |v| bytes.slice(v.0..v.1));

        InternalValue { key, value }
    }
}

// TODO: allow disabling binary index (for meta block)
// -> saves space in metadata blocks
// -> point reads then need to use iter().find() to find stuff (which is fine)

/// Block that contains key-value pairs (user data)
#[derive(Clone)]
pub struct DataBlock {
    pub inner: Block,
}

impl DataBlock {
    /// Interprets a block as a data block.
    ///
    /// The caller needs to make sure the block is actually a data block
    /// (e.g. by checking the block type, this is typically done in the `load_block` routine)
    #[must_use]
    pub fn new(inner: Block) -> Self {
        Self { inner }
    }

    /// Accesses the inner raw bytes
    #[must_use]
    pub fn as_slice(&self) -> &Slice {
        &self.inner.data
    }

    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    pub(crate) fn get_binary_index_reader(&self) -> BinaryIndexReader<'_> {
        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip item count (u32) and restart interval (u8)
        let offset = std::mem::size_of::<u32>() + std::mem::size_of::<u8>();

        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        let binary_index_step_size = unwrap!(reader.read_u8());

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        let binary_index_offset = unwrap!(reader.read_u32::<LittleEndian>());
        let binary_index_len = unwrap!(reader.read_u32::<LittleEndian>());

        BinaryIndexReader::new(
            &self.inner.data,
            binary_index_offset,
            binary_index_len,
            binary_index_step_size,
        )
    }

    #[must_use]
    pub fn get_hash_index_reader(&self) -> Option<HashIndexReader<'_>> {
        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip item count (u32), restart interval (u8), binary index step size (u8)
        // and binary stuff (2x u32)
        let offset = std::mem::size_of::<u32>()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u32>()
            + std::mem::size_of::<u32>();

        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        let hash_index_offset = unwrap!(reader.read_u32::<LittleEndian>());
        let hash_index_len = unwrap!(reader.read_u32::<LittleEndian>());

        if hash_index_len > 0 {
            Some(HashIndexReader::new(
                &self.inner.data,
                hash_index_offset,
                hash_index_len,
            ))
        } else {
            None
        }
    }

    /// Returns the number of hash buckets.
    #[must_use]
    pub fn hash_bucket_count(&self) -> Option<usize> {
        self.get_hash_index_reader()
            .map(|reader| reader.bucket_count())
    }

    // TODO: handle seqno more nicely (make Key generic, so we can do binary search over (key, seqno))
    #[must_use]
    pub fn point_read(&self, needle: &[u8], seqno: SeqNo) -> Option<InternalValue> {
        let iter = if let Some(hash_index_reader) = self.get_hash_index_reader() {
            match hash_index_reader.get(needle) {
                MARKER_FREE => {
                    return None;
                }
                MARKER_CONFLICT => {
                    // NOTE: Fallback to binary search
                    let mut iter = self.iter();

                    if !iter.seek(needle) {
                        return None;
                    }

                    iter
                }
                idx => {
                    let offset: usize = self.get_binary_index_reader().get(usize::from(idx));

                    let mut iter = self.iter();
                    iter.seek_to_offset(offset);

                    iter
                }
            }
        } else {
            let mut iter = self.iter();

            // NOTE: Fallback to binary search
            if !iter.seek(needle) {
                return None;
            }

            iter
        };

        // Linear scan
        for item in iter {
            match item.compare_key(needle, &self.inner.data) {
                std::cmp::Ordering::Greater => {
                    // We are before our searched key/seqno
                    return None;
                }
                std::cmp::Ordering::Equal => {
                    // If key is same as needle, check sequence number
                }
                std::cmp::Ordering::Less => {
                    // We are past our searched key
                    continue;
                }
            }

            if item.seqno >= seqno {
                continue;
            }

            return Some(item.materialize(&self.inner.data));
        }

        None
    }

    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(
            &self.inner.data,
            Decoder::<InternalValue, DataBlockParsedItem>::new(&self.inner),
        )
    }

    /// Returns the binary index length (number of pointers).
    ///
    /// The number of pointers is equal to the number of restart intervals.
    #[must_use]
    pub fn binary_index_len(&self) -> u32 {
        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip item count (u32), restart interval (u8), binary index step size (u8),
        // and binary index offset (u32)
        let offset = std::mem::size_of::<u32>()
            + (2 * std::mem::size_of::<u8>())
            + std::mem::size_of::<u32>();
        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        unwrap!(reader.read_u32::<LittleEndian>())
    }

    /// Returns the amount of items in the block.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Trailer::new(&self.inner).item_count()
    }

    pub fn encode_into_vec(
        items: &[InternalValue],
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<Vec<u8>> {
        let mut buf = vec![];

        Self::encode_into(&mut buf, items, restart_interval, hash_index_ratio)?;

        Ok(buf)
    }

    pub fn encode_into(
        writer: &mut Vec<u8>,
        items: &[InternalValue],
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<()> {
        let first_key = &items
            .first()
            .expect("chunk should not be empty")
            .key
            .user_key;

        let mut serializer = Encoder::<'_, (), InternalValue>::new(
            writer,
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
mod tests {
    use crate::{
        segment::{
            block::{BlockType, Header, ParsedItem},
            Block, BlockOffset, DataBlock,
        },
        Checksum, InternalValue, SeqNo, Slice,
        ValueType::{Tombstone, Value},
    };
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_ping_pong_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::from([111]),
                Slice::from([119]),
                8_602_264_972_526_186_597,
                Value,
            ),
            InternalValue::from_components(
                Slice::from([121, 120, 99]),
                Slice::from([101, 101, 101, 101, 101, 101, 101, 101, 101, 101, 101]),
                11_426_548_769_907,
                Value,
            ),
        ];

        let ping_pong_code = [1, 0];

        let bytes: Vec<u8> = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        let expected_ping_ponged_items = {
            let mut iter = items.iter();
            let mut v = vec![];

            for &x in &ping_pong_code {
                if x == 0 {
                    v.push(iter.next().cloned().unwrap());
                } else {
                    v.push(iter.next_back().cloned().unwrap());
                }
            }

            v
        };

        let real_ping_ponged_items = {
            let mut iter = data_block
                .iter()
                .map(|x| x.materialize(data_block.as_slice()));

            let mut v = vec![];

            for &x in &ping_pong_code {
                if x == 0 {
                    v.push(iter.next().unwrap());
                } else {
                    v.push(iter.next_back().unwrap());
                }
            }

            v
        };

        assert_eq!(expected_ping_ponged_items, real_ping_ponged_items);

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_simple() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes: Vec<u8> = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert!(
                data_block.point_read(b"a", SeqNo::MAX).is_none(),
                "should return None because a does not exist",
            );

            assert!(
                data_block.point_read(b"b", SeqNo::MAX).is_some(),
                "should return Some because b exists",
            );

            assert!(
                data_block.point_read(b"z", SeqNo::MAX).is_none(),
                "should return Some because z does not exist",
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_one() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "pla:earth:fact",
            "eaaaaaaaaarth",
            0,
            crate::ValueType::Value,
        )];

        let bytes = DataBlock::encode_into_vec(&items, 16, 0.0)?;
        let serialized_len = bytes.len();

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(data_block.inner.size(), serialized_len);
        assert_eq!(1, data_block.binary_index_len());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, SeqNo::MAX),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    fn v3_data_block_mvcc_read_first() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "hello",
            "world",
            0,
            crate::ValueType::Value,
        )];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let serialized_len = bytes.len();

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert_eq!(data_block.inner.size(), serialized_len);

            assert_eq!(Some(items[0].clone()), data_block.point_read(b"hello", 777));
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], b"", 23_523_531_241_241_242, Value),
            InternalValue::from_components([0], b"", 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], [], 5, Value),
            InternalValue::from_components([0], [], 4, Tombstone),
            InternalValue::from_components([0], [], 3, Value),
            InternalValue::from_components([0], [], 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
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
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"b", b"b", 2, Value),
            InternalValue::from_components(b"c", b"c", 1, Value),
            InternalValue::from_components(b"d", b"d", 65, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, SeqNo::MAX),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_dense_mvcc_with_hash() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_point_read_mvcc_latest_fuzz_1() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_point_read_mvcc_latest_fuzz_2() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_point_read_mvcc_latest_fuzz_3() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_into_vec(&items, 2, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_point_read_mvcc_latest_fuzz_3_dense() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_dense_mvcc_no_hash() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
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
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

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

        let bytes = DataBlock::encode_into_vec(&items, 16, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert!(data_block
            .point_read(b"pla:venus:fact", SeqNo::MAX)
            .expect("should exist")
            .is_tombstone());

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_dense_2() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_into_vec(&items, 1, 1.33)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }
}
