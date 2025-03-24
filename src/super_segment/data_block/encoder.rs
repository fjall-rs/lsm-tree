use super::super::binary_index::Builder as BinaryIndexBuilder;
use super::super::hash_index::Builder as HashIndexBuilder;
use crate::{coding::Encode, InternalValue};
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Write;
use varint_rs::VarintWriter;

pub const TERMINATOR_MARKER: u8 = 255;

pub const TRAILER_SIZE: usize = 5 * std::mem::size_of::<u32>() + std::mem::size_of::<u8>();

fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter()
        .zip(s2.iter())
        .take_while(|(c1, c2)| c1 == c2)
        .count()
}

pub struct Encoder<'a> {
    writer: Vec<u8>,

    binary_index_builder: BinaryIndexBuilder,
    hash_index_builder: HashIndexBuilder,

    restart_interval: u8,

    base_key: &'a [u8],

    restart_count: usize,
    item_count: usize,
}

impl<'a> Encoder<'a> {
    pub fn new(
        item_count: usize,
        restart_interval: u8,
        hash_index_ratio: f32,
        first_key: &'a [u8],
    ) -> Self {
        let binary_index_len = item_count / usize::from(restart_interval);
        let bucket_count = (item_count as f32 * hash_index_ratio) as u32; // TODO: verify

        Self {
            writer: Vec::with_capacity(u16::MAX.into()),

            binary_index_builder: BinaryIndexBuilder::new(binary_index_len),
            hash_index_builder: HashIndexBuilder::new(bucket_count),

            restart_interval,

            base_key: first_key,

            restart_count: 0,
            item_count: 0,
        }
    }

    pub fn write(&mut self, kv: &'a InternalValue) -> crate::Result<()> {
        // NOTE: Check if we are a restart marker
        if self.item_count % usize::from(self.restart_interval) == 0 {
            // We encode restart markers as:
            // [value type] [seqno] [user key len] [user key] [value len] [value]

            self.restart_count += 1;

            // NOTE: We know that data blocks will never even approach 4 GB in size
            #[allow(clippy::cast_possible_truncation)]
            self.binary_index_builder.insert(self.writer.len() as u32);

            kv.key.encode_into(&mut self.writer)?;

            self.base_key = &kv.key.user_key;
        } else {
            // We encode truncated values as:
            // [value type] [seqno] [shared prefix len] [rest key len] [rest key] [value len] [value]

            self.writer.write_u8(u8::from(kv.key.value_type))?;

            self.writer.write_u64_varint(kv.key.seqno)?;

            // NOTE: We can safely cast to u16, because keys are u16 long max
            #[allow(clippy::cast_possible_truncation)]
            let shared_prefix_len =
                longest_shared_prefix_length(self.base_key, &kv.key.user_key) as u16;

            self.writer.write_u16_varint(shared_prefix_len)?;

            // NOTE: We can safely cast to u16, because keys are u16 long max
            #[allow(clippy::cast_possible_truncation)]
            let rest_len = kv.key.user_key.len() as u16 - shared_prefix_len;

            self.writer.write_u16_varint(rest_len)?;

            let truncated_user_key = &kv
                .key
                .user_key
                .get(shared_prefix_len as usize..)
                .expect("should be in bounds");

            self.writer.write_all(truncated_user_key)?;
        }

        if self.hash_index_builder.bucket_count() > 0 {
            // NOTE: The max binary index is bound by u8 (technically u8::MAX - 2)
            #[allow(clippy::cast_possible_truncation)]
            self.hash_index_builder
                .set(&kv.key.user_key, (self.restart_count - 1) as u8);
        }

        // NOTE: Only write value len + value if we are actually a value
        if !kv.is_tombstone() {
            // NOTE: We know values are limited to 32-bit length
            #[allow(clippy::cast_possible_truncation)]
            self.writer.write_u32_varint(kv.value.len() as u32)?;
            self.writer.write_all(&kv.value)?;
        }

        self.item_count += 1;

        Ok(())
    }

    pub fn finish(mut self) -> crate::Result<Vec<u8>> {
        // IMPORTANT: Terminator marker
        self.writer.write_u8(TERMINATOR_MARKER)?;

        // NOTE: We know that data blocks will never even approach 4 GB in size
        #[allow(clippy::cast_possible_truncation)]
        let binary_index_offset = self.writer.len() as u32;

        let binary_index_len = self.binary_index_builder.write(&mut self.writer)?;

        let mut hash_index_offset = 0u32;
        let mut hash_index_len = 0u32;

        // TODO: unit test when binary index is too long
        // NOTE: We can only use a hash index when there are 254 buckets or less
        // Because 254 and 255 are reserved marker values
        //
        // With the default restart interval of 16, that still gives us support
        // for up to ~4000 KVs
        if self.hash_index_builder.bucket_count() > 0 && binary_index_len <= (u8::MAX - 2).into() {
            // NOTE: We know that data blocks will never even approach 4 GB in size
            #[allow(clippy::cast_possible_truncation)]
            {
                hash_index_offset = self.writer.len() as u32;
            }

            hash_index_len = self.hash_index_builder.bucket_count();

            self.hash_index_builder.write(&mut self.writer)?;
        }

        #[cfg(debug_assertions)]
        let bytes_before = self.writer.len();

        // Trailer:
        // [item_count] [restart_interval] [binary_index_offset] [binary_index_len] [hash_index_offset] [hash_index_len]

        // NOTE: We know that data blocks will never even approach 4 GB in size, so there can't be that many items either
        #[allow(clippy::cast_possible_truncation)]
        self.writer.write_u32::<BigEndian>(self.item_count as u32)?;

        self.writer.write_u8(self.restart_interval)?;

        self.writer.write_u32::<BigEndian>(binary_index_offset)?;

        // NOTE: Even with a dense index, there can't be more index pointers than items
        #[allow(clippy::cast_possible_truncation)]
        self.writer
            .write_u32::<BigEndian>(binary_index_len as u32)?;

        self.writer.write_u32::<BigEndian>(hash_index_offset)?;

        self.writer
            .write_u32::<BigEndian>(if hash_index_offset > 0 {
                hash_index_len
            } else {
                0
            })?;

        #[cfg(debug_assertions)]
        assert_eq!(
            TRAILER_SIZE,
            self.writer.len() - bytes_before,
            "trailer size does not match",
        );

        Ok(self.writer)
    }
}
