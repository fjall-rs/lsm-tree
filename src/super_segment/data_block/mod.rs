mod encoder;

use super::hash_index::Reader as HashIndexReader;
use super::{binary_index::Reader as BinaryIndexReader, Block};
use crate::{coding::DecodeError, InternalValue, SeqNo, Slice, ValueType};
use byteorder::{BigEndian, ReadBytesExt};
use encoder::{TERMINATOR_MARKER, TRAILER_SIZE};
use std::cmp::Ordering;
use std::{
    cmp::Reverse,
    io::{Cursor, Seek},
};
use varint_rs::VarintReader;

pub use encoder::Encoder;

type DataBlockEncoder<'a> = Encoder<'a>;

fn compare_slices<T: Ord>(prefix_part: &[T], key: &[T], needle: &[T]) -> Ordering {
    let combined = prefix_part.iter().chain(key.iter());
    let mut needle_iter = needle.iter();

    for (a, b) in combined.zip(needle_iter.by_ref()) {
        match a.cmp(b) {
            Ordering::Equal => continue,
            other => return other,
        }
    }

    if needle_iter.next().is_some() {
        return Ordering::Less;
    }

    if prefix_part.len() + key.len() > needle.len() {
        return Ordering::Greater;
    }

    Ordering::Equal
}

/// Block that contains key-value pairs (user data)
pub struct DataBlock {
    pub inner: Block,
}

impl DataBlock {
    /// Returns the uncompressed block size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    pub fn get_key_at(&self, pos: usize) -> (&[u8], Reverse<SeqNo>) {
        let bytes = &self.inner.data;
        let mut cursor = Cursor::new(&bytes[pos..]);

        let value_type = cursor.read_u8().expect("should read");

        let _value_type: ValueType = value_type
            .try_into()
            .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))
            .expect("should read");

        let seqno = cursor.read_u64_varint().expect("should read");

        let key_len: usize = cursor.read_u16_varint().expect("should read").into();

        let key_offset = pos + cursor.position() as usize;
        let key = &bytes[key_offset..(key_offset + key_len)];

        (key, Reverse(seqno))
    }

    pub fn walk(
        &self,
        needle: &[u8],
        seqno_watermark: Option<SeqNo>,
        pos: usize,
        restart_interval: usize,
    ) -> crate::Result<Option<InternalValue>> {
        use std::cmp::Ordering::{Equal, Greater, Less};

        let bytes = &self.inner.data;
        let mut cursor = Cursor::new(&bytes[pos..]);

        // NOTE: Check the full item
        let base_key = {
            let value_type = cursor.read_u8().expect("should read");

            if value_type == TERMINATOR_MARKER {
                return Ok(None);
            }

            let value_type: ValueType = value_type
                .try_into()
                .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))
                .expect("should read");

            let seqno = cursor.read_u64_varint().expect("should read");

            let key_len: usize = cursor.read_u16_varint().expect("should read").into();

            let key_offset = pos + cursor.position() as usize;
            let key = &bytes[key_offset..(key_offset + key_len)];
            cursor.seek_relative(key_len as i64).expect("should read");

            let val_len: usize = cursor.read_u32_varint().expect("should read") as usize;
            let val_offset = pos + cursor.position() as usize;

            match key.cmp(needle) {
                Equal => {
                    let should_skip = seqno_watermark
                        .map(|watermark| seqno >= watermark)
                        .unwrap_or(false);

                    if !should_skip {
                        let key = bytes.slice(key_offset..(key_offset + key_len));
                        let value = bytes.slice(val_offset..(val_offset + val_len));

                        return Ok(Some(InternalValue::from_components(
                            key, value, seqno, value_type,
                        )));
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

            let val_offset = pos + cursor.position() as usize;

            match compare_slices(prefix_part, rest_key, needle) {
                Equal => {
                    let should_skip = seqno_watermark
                        .map(|watermark| seqno >= watermark)
                        .unwrap_or(false);

                    if !should_skip {
                        let key = if shared_prefix_len == 0 {
                            bytes.slice(key_offset..(key_offset + rest_key_len))
                        } else {
                            // Stitch key
                            Slice::fuse(prefix_part, rest_key)
                        };

                        return Ok(Some(if value_type == ValueType::Value {
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

    pub fn point_read(&self, key: &[u8], seqno: Option<SeqNo>) -> Option<InternalValue> {
        let bytes = &self.inner.data;

        let mut reader = &bytes[bytes.len() - TRAILER_SIZE..];

        let _item_count = reader.read_u32::<BigEndian>().expect("should read") as usize;
        let restart_interval = reader.read_u8().expect("should read") as usize;

        let binary_index_offset = reader.read_u32::<BigEndian>().expect("should read") as usize;
        let binary_index_len = reader.read_u32::<BigEndian>().expect("should read") as usize;
        let binary_index = BinaryIndexReader::new(bytes, binary_index_offset, binary_index_len);

        // TODO: if the binary index is really dense, don't look into hash index, or
        // maybe don't even build it in the first place

        let hash_index_offset = reader.read_u32::<BigEndian>().expect("should read") as usize;

        if hash_index_offset > 0 {
            let hash_bucket_count = reader.read_u32::<BigEndian>().expect("should read") as usize;

            let hash_index = HashIndexReader::new(bytes, hash_index_offset, hash_bucket_count);

            if let Some(bucket_value) = hash_index.get(key) {
                let restart_entry_pos = binary_index.get(usize::from(bucket_value));

                return self
                    .walk(key, seqno, restart_entry_pos as usize, restart_interval)
                    .expect("OH NO");
            }
        }

        // NOTE: Fallback to binary search

        let mut left = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        // TODO: try to refactor this somehow
        if let Some(seqno) = seqno {
            let seqno_cmp = Reverse(seqno - 1);

            while left < right {
                let mid = (left + right) / 2;

                let offset = binary_index.get(mid);

                if (key, seqno_cmp) >= self.get_key_at(offset as usize) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
        } else {
            while left < right {
                let mid = (left + right) / 2;

                let offset = binary_index.get(mid);

                if key >= self.get_key_at(offset as usize).0 {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
        }

        if left == 0 {
            return None;
        }

        let offset = binary_index.get(left - 1);

        self.walk(key, seqno, offset as usize, restart_interval)
            .expect("OH NO")
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
mod tests {
    use super::DataBlock;
    use crate::{
        segment::block::{header::Header, offset::BlockOffset},
        super_segment::Block,
        Checksum, InternalValue,
    };
    use test_log::test;

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

        let data_block = DataBlock {
            inner: Block {
                data: bytes.into(),
                header: Header {
                    checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            },
        };

        /*  use std::time::Instant;

        let start = Instant::now();
        for _ in 0..1_000_000 {
            data_block.point_read(&needle.key.user_key);
        }
        eprintln!("one read took {:?}ns", {
            let ns = start.elapsed().as_nanos();
            ns / 1_000_000
        }); */

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
    fn v3_data_block_point_read_shadowing() -> crate::Result<()> {
        let items = [
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

        let data_block = DataBlock {
            inner: Block {
                data: bytes.into(),
                header: Header {
                    checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            },
        };

        assert!(data_block
            .point_read(b"pla:venus:fact", None)
            .expect("should exist")
            .is_tombstone());

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_dense() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_items(&items, 1, 0.75)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        let data_block = DataBlock {
            inner: Block {
                data: bytes.into(),
                header: Header {
                    checksum: Checksum::from_raw(0),
                    compression: crate::CompressionType::None,
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            },
        };

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", None));

        Ok(())
    }
}
