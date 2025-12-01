// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod iter;

#[cfg(test)]
mod iter_test;

pub use iter::Iter;

use super::block::{
    binary_index::Reader as BinaryIndexReader, hash_index::Reader as HashIndexReader, Block,
    Decodable, Decoder, Encodable, Encoder, ParsedItem, Trailer, TRAILER_START_MARKER,
};
use crate::key::InternalKey;
use crate::table::block::hash_index::{MARKER_CONFLICT, MARKER_FREE};
use crate::table::util::{compare_prefixed_slice, SliceIndexes};
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
        let value_type = ValueType::try_from(value_type).expect("should be valid value type");

        let seqno = unwrap!(reader.read_u64_varint());

        let key_len: usize = unwrap!(reader.read_u16_varint()).into();
        let key_start = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(key_len as i64));

        let is_value = !value_type.is_tombstone();

        let val_len: usize = if is_value {
            unwrap!(reader.read_u32_varint()) as usize
        } else {
            0
        };
        let val_offset = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(val_len as i64));

        Some(if is_value {
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
        let value_type = unwrap!(ValueType::try_from(value_type));

        let seqno = unwrap!(reader.read_u64_varint());

        let shared_prefix_len: usize = unwrap!(reader.read_u16_varint()).into();
        let rest_key_len: usize = unwrap!(reader.read_u16_varint()).into();

        let key_offset = offset + reader.position() as usize;

        unwrap!(reader.seek_relative(rest_key_len as i64));

        let is_value = !value_type.is_tombstone();

        let val_len: usize = if is_value {
            unwrap!(reader.read_u32_varint()) as usize
        } else {
            0
        };
        let val_offset = offset + reader.position() as usize;
        unwrap!(reader.seek_relative(val_len as i64));

        Some(if is_value {
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

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        writer.write_u16_varint(self.key.user_key.len() as u16)?; // 3
        writer.write_all(&self.key.user_key)?; // 4

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
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

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        writer.write_u16_varint(shared_len as u16)?; // 3

        let rest_len = self.key().len() - shared_len;

        #[expect(clippy::cast_possible_truncation, reason = "keys are u16 length max")]
        writer.write_u16_varint(rest_len as u16)?; // 4

        #[expect(
            clippy::expect_used,
            reason = "the shared len should not be greater than key length"
        )]
        let truncated_user_key = self
            .key
            .user_key
            .get(shared_len..)
            .expect("should be in bounds");

        writer.write_all(truncated_user_key)?; // 5

        // NOTE: Only write value len + value if we are actually a value
        if !self.is_tombstone() {
            #[expect(clippy::cast_possible_truncation, reason = "values are u32 length max")]
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
    pub value_type: ValueType,
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
        #[expect(clippy::indexing_slicing)]
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.key.0..self.key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.key.0..self.key.1)
        };

        let key = InternalKey::new(key, self.seqno, self.value_type);

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
        use std::mem::size_of;

        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip restart interval (u8)
        let offset = size_of::<u8>();

        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        let binary_index_step_size = unwrap!(reader.read_u8());

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        let binary_index_len = unwrap!(reader.read_u32::<LittleEndian>());
        let binary_index_offset = unwrap!(reader.read_u32::<LittleEndian>());

        BinaryIndexReader::new(
            &self.inner.data,
            binary_index_offset,
            binary_index_len,
            binary_index_step_size,
        )
    }

    #[must_use]
    pub fn get_hash_index_reader(&self) -> Option<HashIndexReader<'_>> {
        use std::mem::size_of;

        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip restart interval (u8), binary index step size (u8)
        // and binary stuff (2x u32)
        let offset = size_of::<u8>() + size_of::<u8>() + size_of::<u32>() + size_of::<u32>();

        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        let hash_index_len = unwrap!(reader.read_u32::<LittleEndian>());
        let hash_index_offset = unwrap!(reader.read_u32::<LittleEndian>());

        if hash_index_len == 0 {
            debug_assert_eq!(
                0, hash_index_offset,
                "hash index offset should be 0 if its length is 0"
            );
            None
        } else {
            Some(HashIndexReader::new(
                &self.inner.data,
                hash_index_offset,
                hash_index_len,
            ))
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
    #[expect(clippy::iter_without_into_iter)]
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
        use std::mem::size_of;

        let trailer = Trailer::new(&self.inner);

        // NOTE: Skip restart interval (u8) and binary index step size (u8)
        let offset = 2 * size_of::<u8>();
        let mut reader = unwrap!(trailer.as_slice().get(offset..));

        unwrap!(reader.read_u32::<LittleEndian>())
    }

    /// Returns the number of items in the block.
    #[must_use]
    #[expect(clippy::len_without_is_empty)]
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

    /// Builds an data block.
    ///
    /// # Panics
    ///
    /// Panics if the given item array if empty.
    pub fn encode_into(
        writer: &mut Vec<u8>,
        items: &[InternalValue],
        restart_interval: u8,
        hash_index_ratio: f32,
    ) -> crate::Result<()> {
        #[expect(clippy::expect_used, reason = "the chunk should not be empty")]
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
