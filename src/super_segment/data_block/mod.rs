mod encoder;
mod iter;

use super::hash_index::Reader as HashIndexReader;
use super::{binary_index::Reader as BinaryIndexReader, Block};
use crate::{coding::DecodeError, InternalValue, SeqNo, Slice, ValueType};
use byteorder::{LittleEndian, ReadBytesExt};
use encoder::{TERMINATOR_MARKER, TRAILER_SIZE};
use std::cmp::Ordering;
use std::{
    cmp::Reverse,
    io::{Cursor, Seek},
};
use varint_rs::VarintReader;

pub use encoder::Encoder;
pub use iter::Iter;

type DataBlockEncoder<'a> = Encoder<'a>;

// TODO: Fuzz test
fn compare_prefixed_slice(prefix: &[u8], suffix: &[u8], needle: &[u8]) -> Ordering {
    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();

        return if combined_len > 0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
    }

    match prefix.len().cmp(&needle.len()) {
        Ordering::Equal => match prefix.cmp(needle) {
            Ordering::Equal => {}
            ordering => return ordering,
        },
        Ordering::Greater => {
            // SAFETY: We know that the prefix is longer than the needle, so we can safely
            // truncate it to the needle's length
            #[allow(unsafe_code)]
            let prefix = unsafe { prefix.get_unchecked(0..needle.len()) };
            return prefix.cmp(needle);
        }
        Ordering::Less => {
            // SAFETY: We know that the needle is longer than the prefix, so we can safely
            // truncate it to the prefix's length
            #[allow(unsafe_code)]
            let needle = unsafe { needle.get_unchecked(0..prefix.len()) };

            match prefix.cmp(needle) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }
    }

    // SAFETY: We know that the prefix is definitely not longer than the needle
    // so we can safely truncate
    #[allow(unsafe_code)]
    let needle = unsafe { needle.get_unchecked(prefix.len()..) };
    suffix.cmp(needle)
}

/// Block that contains key-value pairs (user data)
#[derive(Clone)]
pub struct DataBlock {
    pub inner: Block,
}

struct RestartHead {
    value_type: u8,
    seqno: SeqNo,
    key_start: usize,
    key_len: usize,
}

impl DataBlock {
    #[must_use]
    pub fn new(inner: Block) -> Self {
        let bytes = &inner.data;
        Self { inner }
    }

    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    #[must_use]
    pub fn iter(&self) -> Iter {
        Iter::new(self.clone())
    }

    fn get_key_at(&self, pos: usize) -> crate::Result<(&[u8], Reverse<SeqNo>)> {
        // eprintln!("probe {pos}");

        let bytes = &self.inner.data;
        let mut cursor = Cursor::new(&bytes[pos..]);

        let parsed = Self::parse_restart_item(&mut cursor)?;
        let key_start = pos + parsed.key_start;
        let key_end = key_start + parsed.key_len;
        let key = &bytes[key_start..key_end];

        Ok((key, Reverse(parsed.seqno)))
    }

    fn parse_restart_item(cursor: &mut Cursor<&[u8]>) -> crate::Result<RestartHead> {
        let value_type = cursor.read_u8()?;

        let seqno = cursor.read_u64_varint()?;

        let key_len: usize = cursor.read_u16_varint()?.into();
        let key_start = cursor.position() as usize;
        cursor.seek_relative(key_len as i64)?;

        Ok(RestartHead {
            value_type,
            seqno,
            key_start,
            key_len,
        })
    }

    fn walk(
        &self,
        needle: &[u8],
        seqno_watermark: Option<SeqNo>,
        pos: usize,
        restart_interval: usize,
    ) -> crate::Result<Option<InternalValue>> {
        use std::cmp::Ordering::{Equal, Greater, Less};

        let bytes = &self.inner.data;
        let mut cursor = Cursor::new(&bytes[pos..]);

        let mut base_key_pos = 0;

        // NOTE: Check the full item
        let base_key = {
            let parsed = Self::parse_restart_item(&mut cursor)?;

            let value_type: ValueType = parsed
                .value_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("ValueType", parsed.value_type)))?;

            let seqno = parsed.seqno;

            let key_start = pos + parsed.key_start;
            let key_end = key_start + parsed.key_len;
            let key = &bytes[key_start..key_end];

            base_key_pos = key_start;

            let val_len: usize = if value_type == ValueType::Value {
                cursor.read_u32_varint().expect("should read") as usize
            } else {
                0
            };

            match key.cmp(needle) {
                Equal => {
                    let should_skip = seqno_watermark
                        .map(|watermark| seqno >= watermark)
                        .unwrap_or(false);

                    if !should_skip {
                        let key = bytes.slice(key_start..key_end);

                        return Ok(Some(if value_type == ValueType::Value {
                            let val_offset = pos + cursor.position() as usize;
                            let value = bytes.slice(val_offset..(val_offset + val_len));
                            InternalValue::from_components(key, value, seqno, value_type)
                        } else {
                            InternalValue::from_components(key, b"", seqno, value_type)
                        }));
                    }
                }
                Greater => {
                    // NOTE: Already passed searched key
                    return Ok(None);
                }
                Less => {
                    // NOTE: Continue
                }
            }

            cursor.seek_relative(val_len as i64).expect("should read");

            key
        };

        // NOTE: Check the rest items
        for _idx in 1..restart_interval {
            let value_type = cursor.read_u8()?;

            if value_type == TERMINATOR_MARKER {
                return Ok(None);
            }

            let value_type: ValueType = value_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))?;

            let seqno = cursor.read_u64_varint()?;

            let shared_prefix_len: usize = cursor.read_u16_varint()?.into();
            let rest_key_len: usize = cursor.read_u16_varint()?.into();

            let key_offset = pos + cursor.position() as usize;

            let prefix_part = &base_key[0..shared_prefix_len];
            let rest_key = &bytes[key_offset..(key_offset + rest_key_len)];
            cursor.seek_relative(rest_key_len as i64)?;

            let val_len: usize = if value_type == ValueType::Value {
                cursor.read_u32_varint().expect("should read") as usize
            } else {
                0
            };

            match compare_prefixed_slice(prefix_part, rest_key, needle) {
                Equal => {
                    let should_skip = seqno_watermark
                        .map(|watermark| seqno >= watermark)
                        .unwrap_or(false);

                    if !should_skip {
                        let key = if shared_prefix_len == 0 {
                            bytes.slice(key_offset..(key_offset + rest_key_len))
                        } else if rest_key_len == 0 {
                            bytes.slice(base_key_pos..(base_key_pos + shared_prefix_len))
                        } else {
                            // Stitch key
                            Slice::fused(prefix_part, rest_key)
                        };

                        return Ok(Some(if value_type == ValueType::Value {
                            let val_offset = pos + cursor.position() as usize;
                            let value = bytes.slice(val_offset..(val_offset + val_len));
                            InternalValue::from_components(key, value, seqno, value_type)
                        } else {
                            InternalValue::from_components(key, b"", seqno, value_type)
                        }));
                    }
                }
                Greater => {
                    // NOTE: Already passed searched key
                    return Ok(None);
                }
                Less => {
                    // NOTE: Continue
                }
            }

            if value_type == ValueType::Value {
                cursor.seek_relative(val_len as i64)?;
            }
        }

        Ok(None)
    }

    pub fn binary_index_pointer_count(&self) -> usize {
        let bytes = &self.inner.data;

        // SAFETY: We know that there is always a trailer, so we cannot go out of bounds
        #[warn(unsafe_code)]
        let mut reader = unsafe { bytes.get_unchecked(self.trailer_offset()..) };

        let _item_count = reader.read_u32::<LittleEndian>().expect("should read") as usize;
        let _restart_interval = reader.read_u8().expect("should read") as usize;

        let _binary_index_step_size = reader.read_u8().expect("should read") as usize;

        let _binary_index_offset = reader.read_u32::<LittleEndian>().expect("should read") as usize;

        reader.read_u32::<LittleEndian>().expect("should read") as usize
    }

    pub fn hash_bucket_count(&self) -> usize {
        let bytes = &self.inner.data;

        // SAFETY: We know that there is always a trailer, so we cannot go out of bounds
        #[warn(unsafe_code)]
        let mut reader = unsafe { bytes.get_unchecked(self.trailer_offset()..) };

        let _item_count = reader.read_u32::<LittleEndian>().expect("should read") as usize;
        let _restart_interval = reader.read_u8().expect("should read") as usize;

        let _binary_index_step_size = reader.read_u8().expect("should read") as usize;
        let _binary_index_offset = reader.read_u32::<LittleEndian>().expect("should read") as usize;
        let _binary_index_len = reader.read_u32::<LittleEndian>().expect("should read") as usize;

        let hash_index_offset = reader.read_u32::<LittleEndian>().expect("should read") as usize;

        if hash_index_offset > 0 {
            reader.read_u32::<LittleEndian>().expect("should read") as usize
        } else {
            0
        }
    }

    fn trailer_offset(&self) -> usize {
        self.inner.data.len() - TRAILER_SIZE
    }

    /// Returns the amount of items in the block
    #[must_use]
    pub fn len(&self) -> usize {
        let bytes = &self.inner.data;

        // SAFETY: We know that there is always a trailer, so we cannot go out of bounds
        #[warn(unsafe_code)]
        let mut reader = unsafe { bytes.get_unchecked(self.trailer_offset()..) };

        reader.read_u32::<LittleEndian>().expect("should read") as usize
    }

    /// Always returns false: a block is never empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn point_read(
        &self,
        key: &[u8],
        seqno: Option<SeqNo>,
    ) -> crate::Result<Option<InternalValue>> {
        let bytes = &self.inner.data;

        let start_pos = self.trailer_offset()
            + /* skip item count */ std::mem::size_of::<u32>();

        // SAFETY: We know that there is always a trailer, so we cannot go out of bounds
        #[warn(unsafe_code)]
        let mut reader = unsafe { bytes.get_unchecked(start_pos..) };

        let restart_interval = reader.read_u8().expect("should read") as usize;

        let binary_index_step_size = reader.read_u8().expect("should read") as usize;

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        // eprintln!("binary index step size={binary_index_step_size}");

        let binary_index_offset = reader.read_u32::<LittleEndian>().expect("should read") as usize;
        let binary_index_len = reader.read_u32::<LittleEndian>().expect("should read") as usize;
        let binary_index = BinaryIndexReader::new(
            bytes,
            binary_index_offset,
            binary_index_len,
            binary_index_step_size,
        );

        // TODO: if the binary index is really dense, don't look into hash index, or
        // maybe don't even build it in the first place

        let hash_index_offset = reader.read_u32::<LittleEndian>().expect("should read") as usize;

        if hash_index_offset > 0 {
            let hash_bucket_count =
                reader.read_u32::<LittleEndian>().expect("should read") as usize;

            let hash_index = HashIndexReader::new(bytes, hash_index_offset, hash_bucket_count);

            if let Some(bucket_value) = hash_index.get(key) {
                let restart_entry_pos = binary_index.get(usize::from(bucket_value));

                return self.walk(key, seqno, restart_entry_pos, restart_interval);
            }
        }

        // NOTE: Fallback to binary search

        let mut left = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return Ok(None);
        }

        let seqno_cmp = Reverse(seqno.unwrap_or(u64::MAX) - 1);

        while left < right {
            let mid = (left + right) / 2;

                let offset = binary_index.get(mid);

                if (key, seqno_cmp) >= self.get_key_at(offset)? {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == 0 {
            return Ok(None);
        }

        let offset = binary_index.get(left - 1);

        self.walk(key, seqno, offset, restart_interval)
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

        let mut serializer =
            DataBlockEncoder::new(items.len(), restart_interval, hash_index_ratio, first_key);

        for item in items {
            serializer.write(item)?;
        }

        serializer.finish()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        segment::block::{header::Header, offset::BlockOffset},
        super_segment::Block,
        Checksum, InternalValue,
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

        let bytes = DataBlock::encode_items(&items, 16, 0.75)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None)?);

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], b"", 23_523_531_241_241_242, Value),
            InternalValue::from_components([0], b"", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 0.75)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None)?);

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], [], 18_446_568_565_776_614_018, Value),
            InternalValue::from_components([0], [], 6_989_411_799_330_193_407, Tombstone),
            InternalValue::from_components([0], [], 864_515_618_921_971_552, Value),
            InternalValue::from_components([0], [], 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() == 0);

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None)?);

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
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        assert_eq!(
            data_block.iter().map(|x| x.expect("should be ok")).count(),
            items.len(),
        );

        assert_eq!(
            items,
            *data_block
                .iter()
                .map(|x| x.expect("should be ok"))
                .collect::<Vec<_>>(),
        );

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

        let bytes = DataBlock::encode_items(&items, 1, 0.75)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None)?);

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
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() == 0);

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None)?);

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

        let bytes = DataBlock::encode_items(&items, 16, 0.75)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        assert!(data_block
            .point_read(b"pla:venus:fact", None)?
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

        let bytes = DataBlock::encode_items(&items, 1, 0.75)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None)?);

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

        let bytes = DataBlock::encode_items(&items, 16, 0.75)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(
            data_block.iter().map(|x| x.expect("should be ok")).count(),
            items.len()
        );

        assert_eq!(data_block.iter().flatten().collect::<Vec<_>>(), items);

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

        let bytes = DataBlock::encode_items(&items, 16, 0.75)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        assert_eq!(
            data_block.iter().map(|x| x.expect("should be ok")).count(),
            items.len(),
        );

        assert_eq!(
            items,
            *data_block
                .iter()
                .map(|x| x.expect("should be ok"))
                .collect::<Vec<_>>(),
        );

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

        let bytes = DataBlock::encode_items(&items, 1, 0.75)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(
            items.len(),
            data_block.iter().map(|x| x.expect("should be ok")).count(),
        );

        assert_eq!(
            items,
            *data_block
                .iter()
                .map(|x| x.expect("should be ok"))
                .collect::<Vec<_>>(),
        );

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

        let bytes = DataBlock::encode_items(&items, 16, 0.75)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(data_block.hash_bucket_count() > 0);

        assert_eq!(
            items.len(),
            data_block
                .iter()
                .rev()
                .map(|x| x.expect("should be ok"))
                .count(),
        );

        assert_eq!(
            items.into_iter().rev().collect::<Vec<_>>(),
            data_block
                .iter()
                .rev()
                .map(|x| x.expect("should be ok"))
                .collect::<Vec<_>>(),
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_just_enough_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..254)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 0.75)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(0, data_block.hash_bucket_count());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_too_many_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..255)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 0.75)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(0, data_block.hash_bucket_count());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
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
                    compression: crate::CompressionType::None,
                    data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(0, data_block.hash_bucket_count());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1))?,
            );
        }

        Ok(())
    }
}
