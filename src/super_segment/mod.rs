mod binary_index;
mod hash_index;

use crate::{
    coding::{DecodeError, Encode},
    segment::block::header::Header,
    InternalValue, SeqNo, Slice, ValueType,
};
use binary_index::{Builder as BinaryIndexBuilder, Reader as BinaryIndexReader};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use hash_index::{Builder as HashIndexBuilder, Reader as HashIndexReader};
use std::{
    cmp::{Ordering, Reverse},
    io::{Cursor, Seek, Write},
};
use varint_rs::{VarintReader, VarintWriter};

const TERMINATOR_MARKER: u8 = 255;

/// A block on disk.
///
/// Consists of a header and some bytes (the data/payload)
pub struct Block {
    pub header: Header,
    pub data: Slice,
}

impl Block {
    /// Returns the uncompressed block size in bytes
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/* impl Decode for Block {
    fn decode_from<R: std::io::Read>(reader: &mut R) -> Result<Self, DecodeError>
    where
        Self: Sized,
    {
        let header = Header::decode_from(reader)?;
        let data = Slice::from_reader(reader, header.data_length as usize)?;
        let data = match header.compression {
            CompressionType::None => data,

            #[cfg(feature = "lz4")]
            CompressionType::Lz4 => lz4_flex::decompress_size_prepended(&data)
                .map(Into::into)
                .map_err(|_| crate::Error::Decompress(header.compression))?,

            #[cfg(feature = "miniz")]
            CompressionType::Miniz(_) => miniz_oxide::inflate::decompress_to_vec(&data)
                .map(Into::into)
                .map_err(|_| crate::Error::Decompress(header.compression))?,
        };

        Ok(Self { header, data })
    }
} */

fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|(c1, c2)| c1 == c2)
        .count()
}

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

        let mut reader = &bytes[bytes.len()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u8>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u8>()..];

        let _item_count = reader.read_u32::<BigEndian>().expect("should read") as usize;
        let restart_interval = reader.read_u8().expect("should read") as usize;

        let binary_index_offset = reader.read_u32::<BigEndian>().expect("should read") as usize;
        let binary_index_len = reader.read_u32::<BigEndian>().expect("should read") as usize;
        let binary_index = BinaryIndexReader::new(
            &bytes[binary_index_offset
                ..binary_index_offset + binary_index_len * std::mem::size_of::<u32>()],
        );

        // TODO: if the binary index is really dense, don't look into hash index, or
        // maybe don't even build it in the first place

        let hash_index_offset = reader.read_u32::<BigEndian>().expect("should read") as usize;

        if hash_index_offset > 0 {
            let hash_bucket_count = reader.read_u8().expect("should read") as usize;

            let hash_index_bytes = &bytes[hash_index_offset..hash_index_offset + hash_bucket_count];
            let hash_index = HashIndexReader::new(hash_index_bytes);

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
        let mut writer = Vec::with_capacity(u16::MAX.into());

        let mut binary_index_builder =
            BinaryIndexBuilder::new(items.len() / usize::from(restart_interval));

        let bucket_count = (items.len() as f32 * hash_index_ratio) as u8;
        let mut hash_index_builder = HashIndexBuilder::new(bucket_count);

        let mut base_key: &Slice = &items
            .first()
            .expect("chunk should not be empty")
            .key
            .user_key;

        let mut restart_count: u32 = 0;

        // Serialize each value
        for (idx, kv) in items.iter().enumerate() {
            // We encode restart markers as
            // [value type] [seqno] [user key len] [user key] [value len] [value]
            if idx % usize::from(restart_interval) == 0 {
                restart_count += 1;

                binary_index_builder.insert(writer.len() as u32);

                kv.key.encode_into(&mut writer)?;

                base_key = &kv.key.user_key;
            } else {
                // We encode truncated values as
                // [value type] [seqno] [shared prefix len] [rest key len] [rest key] [value len] [value]

                writer.write_u8(u8::from(kv.key.value_type))?;

                writer.write_u64_varint(kv.key.seqno)?;

                let shared_prefix_len =
                    longest_shared_prefix_length(base_key, &kv.key.user_key) as u16;

                writer.write_u16_varint(shared_prefix_len)?;

                let rest_len = kv.key.user_key.len() as u16 - shared_prefix_len;
                writer.write_u16_varint(rest_len)?;

                let truncated_user_key: &[u8] = &kv.key.user_key;
                let truncated_user_key = &truncated_user_key[shared_prefix_len as usize..];
                writer.write_all(truncated_user_key)?;
            }

            if bucket_count > 0 {
                hash_index_builder.set(&kv.key.user_key, (restart_count - 1) as u8);
            }

            // NOTE: Only write value len + value if we are actually a value
            if !kv.is_tombstone() {
                // NOTE: We know values are limited to 32-bit length
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32_varint(kv.value.len() as u32)?;
                writer.write_all(&kv.value)?;
            }
        }

        // IMPORTANT: Terminator marker
        writer.write_u8(TERMINATOR_MARKER)?;

        let binary_index_offset = writer.len() as u32;
        let binary_index_len = binary_index_builder.write(&mut writer)?;

        let mut hash_index_offset = 0u32;
        let mut hash_index_len = 0u8;

        // TODO: unit test when binary index is too long
        // NOTE: We can only use a hash index when there are 254 buckets or less
        // Because 254 and 255 are reserved marker values
        //
        // With the default restart interval of 16, that still gives us support
        // for up to ~4000 KVs
        if bucket_count > 0 && binary_index_len <= (u8::MAX - 2).into() {
            hash_index_offset = writer.len() as u32;
            hash_index_len = hash_index_builder.len();

            hash_index_builder.write(&mut writer)?;
        }

        // Trailer
        writer.write_u32::<BigEndian>(items.len() as u32)?;
        writer.write_u8(restart_interval)?;
        writer.write_u32::<BigEndian>(binary_index_offset)?;
        writer.write_u32::<BigEndian>(binary_index_len as u32)?;
        writer.write_u32::<BigEndian>(hash_index_offset)?;
        writer.write_u8(if hash_index_offset > 0 {
            hash_index_len
        } else {
            0
        })?;

        Ok(writer)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        segment::block::{header::Header, offset::BlockOffset},
        super_segment::{Block, DataBlock},
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
