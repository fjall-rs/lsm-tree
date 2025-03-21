use std::{
    hash::Hash,
    io::{Cursor, Seek, Write},
};

use crate::{
    key::InternalKey, segment::block::header::Header, CompressionType, Decode, DecodeError, Encode,
    EncodeError, InternalValue, Slice, ValueType,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use varint_rs::{VarintReader, VarintWriter};
use xxhash_rust::xxh3::xxh3_64;

/// A block on disk.
///
/// Consists of a header and some bytes (the data/payload)
pub struct Block {
    header: Header,
    data: Slice,
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

const MARKER_FREE: u8 = u8::MAX - 1;
const MARKER_CONFLICT: u8 = u8::MAX;

/// Block that contains key-value pairs (user data)
pub struct DataBlock {
    inner: Block,
}

impl DataBlock {
    pub fn point_read(&self, key: &[u8]) -> crate::Result<()> {
        let bytes = &self.inner.data;

        let mut cursor = &bytes[bytes.len()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u8>()..];

        let binary_index_offset: usize = cursor.read_u32::<BigEndian>().unwrap() as usize;
        let binary_index_len: usize = cursor.read_u32::<BigEndian>().unwrap() as usize;
        eprintln!(
            "we got binary_idx_offset={binary_index_offset}, binary_index_len={binary_index_len}"
        );

        let hash_index_offset: usize = cursor.read_u32::<BigEndian>().unwrap() as usize;
        let hash_bucket_count: usize = cursor.read_u8().unwrap().into();
        eprintln!(
            "we got hash_idx_offset={hash_index_offset}, hash_bucket_count={hash_bucket_count}"
        );

        if hash_index_offset > 0 {
            let hash = xxh3_64(key);
            let bucket_no = (hash % hash_bucket_count as u64) as usize;

            eprintln!(
                "{:?} may be in bucket {bucket_no}",
                String::from_utf8_lossy(key)
            );

            let bucket_value_pos = hash_index_offset + bucket_no;
            let bucket_value = bytes[bucket_value_pos] as usize;

            if bucket_value < MARKER_FREE.into() {
                eprintln!("binary index hash short circuit idx = {bucket_value}");

                let binary_index_pos =
                    binary_index_offset + bucket_value * std::mem::size_of::<u32>();

                let mut cursor = &bytes[binary_index_pos..];

                let restart_entry_pos = cursor.read_u32::<BigEndian>()?;

                eprintln!("we have to jump to {restart_entry_pos}");

                todo!();
            } else {
                // NOTE: Fallback to binary search

                unimplemented!()
            }
        }

        Ok(())
    }

    pub fn iter(&self) -> crate::Result<()> {
        let bytes = &self.inner.data;

        let mut cursor = &bytes[bytes.len()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u8>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u32>()
            - std::mem::size_of::<u8>()..];

        let item_count = cursor.read_u32::<BigEndian>().unwrap();
        let restart_count: usize = cursor.read_u8().unwrap().into();
        eprintln!("we got item_count={item_count}, restart_interval={restart_count}");

        let mut cursor = Cursor::new(&bytes[..]);
        let mut base_key: Option<InternalKey> = None;

        for idx in 0..item_count as usize {
            if idx % restart_count == 0 {
                eprintln!("-- full item");

                let seqno = cursor.read_u64_varint()?;

                let value_type = cursor.read_u8()?;
                let value_type: ValueType = value_type
                    .try_into()
                    .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))?;

                let key_len: usize = cursor.read_u16_varint()?.into();

                let offset = cursor.position() as usize;
                let key = bytes.slice(offset..(offset + key_len));
                cursor.seek_relative(key_len as i64)?;
                // eprintln!("{:?}", String::from_utf8_lossy(&key));

                let val_len: usize = cursor.read_u32_varint()? as usize;

                let offset = cursor.position() as usize;
                let value = bytes.slice(offset..(offset + val_len));
                cursor.seek_relative(val_len as i64)?;

                // eprintln!("{:?}", String::from_utf8_lossy(&value));

                let item = InternalValue::from_components(key, value, seqno, value_type);
                eprintln!("{item:?}");

                base_key = Some(item.key.clone());
            } else {
                eprintln!("-- truncated item");

                let seqno = cursor.read_u64_varint()?;

                let value_type = cursor.read_u8()?;
                let value_type: ValueType = value_type
                    .try_into()
                    .map_err(|()| DecodeError::InvalidTag(("ValueType", value_type)))?;

                let shared_prefix_len: usize = cursor.read_u16_varint()?.into();
                let rest_key_len: usize = cursor.read_u16_varint()?.into();

                // eprintln!("shared={shared_prefix_len}, rest={rest_key_len}");

                let key = if shared_prefix_len > 0 {
                    // Stitch key

                    // TODO: use Slice::with_size_unzeroed
                    let mut key = Vec::with_capacity(shared_prefix_len + rest_key_len);
                    key.extend_from_slice(
                        &base_key.as_ref().unwrap().user_key[0..shared_prefix_len],
                    );

                    for _ in 0..rest_key_len {
                        key.push(cursor.read_u8()?);
                    }

                    Slice::from(key)
                } else {
                    // Is full key already
                    let offset = cursor.position() as usize;
                    let key = bytes.slice(offset..(offset + rest_key_len));
                    cursor.seek_relative(rest_key_len as i64)?;
                    key
                };
                // eprintln!("{:?}", String::from_utf8_lossy(&key));

                if value_type == ValueType::Value {
                    let val_len: usize = cursor.read_u32_varint()? as usize;

                    // eprintln!("val len={val_len}");

                    let offset = cursor.position() as usize;
                    let value = bytes.slice(offset..(offset + val_len));
                    cursor.seek_relative(val_len as i64)?;

                    // eprintln!("{:?}", String::from_utf8_lossy(&value));

                    let item = InternalValue::from_components(key, value, seqno, value_type);
                    eprintln!("{item:?}");
                } else {
                    let item = InternalValue::from_components(key, b"", seqno, value_type);
                    eprintln!("{item:?}");
                }
            }
        }

        Ok(())
    }

    pub fn encode_items(items: &[InternalValue], restart_interval: u8) -> crate::Result<Vec<u8>> {
        let mut writer = Vec::with_capacity(u16::MAX.into());

        eprintln!("encoding {} items", items.len());

        let mut binary_index = Vec::<u32>::with_capacity(items.len());

        let hash_bucket_count = items.len();
        let mut hash_index: Vec<u8> = vec![MARKER_FREE; hash_bucket_count];

        let mut base_key: &Slice = &items
            .first()
            .expect("chunk should not be empty")
            .key
            .user_key;

        let mut restart_count: u32 = 0;

        #[cfg(debug_assertions)]
        let mut hash_conflicts = 0;

        // Serialize each value
        for (idx, kv) in items.iter().enumerate() {
            // We encode restart markers as
            // [seqno] [value type] [user key len] [user key] [value len] [value]
            if idx % usize::from(restart_interval) == 0 {
                eprintln!("restart!");
                restart_count += 1;

                binary_index.push(writer.len() as u32);

                kv.key.encode_into(&mut writer)?;

                base_key = &kv.key.user_key;
            } else {
                // We encode truncated values as
                // [seqno] [value type] [shared prefix len] [rest key len] [rest key] [value len] [value]

                eprintln!("encode with prefix truncation");
                eprintln!("base key is {:?}", String::from_utf8_lossy(base_key));

                writer.write_u64_varint(kv.key.seqno)?;
                writer.write_u8(u8::from(kv.key.value_type))?;

                let shared_prefix_len =
                    longest_shared_prefix_length(base_key, &kv.key.user_key) as u16;

                writer.write_u16_varint(shared_prefix_len)?;

                let rest_len = kv.key.user_key.len() as u16 - shared_prefix_len;
                writer.write_u16_varint(rest_len)?;

                let truncated_user_key: &[u8] = &kv.key.user_key;
                let truncated_user_key = &truncated_user_key[shared_prefix_len as usize..];
                writer.write_all(truncated_user_key)?;

                eprintln!(
                    "shared prefix is {:?}",
                    String::from_utf8_lossy(&base_key[0..shared_prefix_len as usize]),
                );
            }

            let hash = xxh3_64(&kv.key.user_key);
            let pos = (hash % hash_bucket_count as u64) as usize;

            if hash_index[pos] == MARKER_FREE {
                // Free slot
                hash_index[pos] = (restart_count as u8) - 1;

                eprintln!(
                    "hash ref for {:?} => bucket={}->{}",
                    String::from_utf8_lossy(&kv.key.user_key),
                    pos,
                    restart_count - 1,
                );
            } else if hash_index[pos] < MARKER_FREE {
                // Mark as conflicted
                hash_index[pos] = MARKER_CONFLICT;

                eprintln!("{pos} is now conflicted");
                hash_conflicts += 1;
            }

            // NOTE: Only write value len + value if we are actually a value
            if !kv.is_tombstone() {
                // NOTE: We know values are limited to 32-bit length
                #[allow(clippy::cast_possible_truncation)]
                writer.write_u32_varint(kv.value.len() as u32)?;
                writer.write_all(&kv.value)?;
            }
        }

        let binary_index_offset = writer.len() as u32;

        eprintln!("binary index @ {binary_index_offset}: {binary_index:?}");

        for &offset in &binary_index {
            writer.write_u32::<BigEndian>(offset)?; // TODO: benchmark little endian on x86_64
        }

        let mut hash_index_offset = 0u32;

        // TODO: unit test when binary index is too long
        if binary_index.len() <= (u8::MAX - 2).into() {
            hash_index_offset = writer.len() as u32;

            eprintln!("hash index @ {hash_index_offset}: {hash_index:?}");

            for &idx in &hash_index {
                writer.write_u8(idx)?;
            }
        }

        // Trailer
        writer.write_u32::<BigEndian>(items.len() as u32)?;
        writer.write_u8(restart_interval)?;
        writer.write_u32::<BigEndian>(binary_index_offset)?;
        writer.write_u32::<BigEndian>(binary_index.len() as u32)?;
        writer.write_u32::<BigEndian>(hash_index_offset)?;
        writer.write_u8(if hash_index_offset > 0 {
            hash_index.len() as u8
        } else {
            0
        })?;

        #[cfg(debug_assertions)]
        eprintln!(
            "hash index had {hash_conflicts} conflicts (rate={}%)",
            (hash_conflicts as f32 / hash_bucket_count as f32) * 100.0
        );

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
    fn v3_data_block_simple() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                "planet:earth:fact",
                "eaaaaaaaaarth",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:jupiter:fact",
                "Jupiter is big",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:jupiter:mass",
                "Massive",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:jupiter:name",
                "Jupiter",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:jupiter:radius",
                "Big",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:saturn:fact",
                "Saturn is pretty big",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:saturn:name",
                "Saturn",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components("planet:venus:fact", "", 1, crate::ValueType::Tombstone),
            InternalValue::from_components(
                "planet:venus:fact",
                "Venus exists",
                0,
                crate::ValueType::Value,
            ),
            InternalValue::from_components(
                "planet:venus:name",
                "Venus",
                0,
                crate::ValueType::Value,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        eprintln!("encoded into {} bytes", bytes.len());

        {
            let bytes = lz4_flex::compress_prepend_size(&bytes);
            eprintln!("compressed into {} bytes", bytes.len());
        }

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
        data_block.iter()?;

        data_block.point_read(b"planet:jupiter:name")?;

        panic!();

        Ok(())
    }
}
