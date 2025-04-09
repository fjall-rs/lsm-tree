use super::super::hash_index::Builder as HashIndexBuilder;
use super::{super::binary_index::Builder as BinaryIndexBuilder, NewKeyedBlockHandle};
use crate::super_segment::util::longest_shared_prefix_length;
use crate::{
    segment::{block::offset::BlockOffset, trailer::TRAILER_SIZE},
    super_segment::{block::TRAILER_START_MARKER, hash_index::MAX_POINTERS_FOR_HASH_INDEX},
};
use byteorder::{LittleEndian, WriteBytesExt};
use std::io::Write;
use varint_rs::VarintWriter;

pub struct Encoder<'a> {
    writer: Vec<u8>,

    binary_index_builder: BinaryIndexBuilder,
    hash_index_builder: HashIndexBuilder,

    restart_interval: u8,

    use_prefix_truncation: bool,
    base_key: &'a [u8],

    offset: BlockOffset,

    restart_count: usize,
    item_count: usize,
}

impl<'a> Encoder<'a> {
    pub fn new(
        item_count: usize,
        restart_interval: u8,
        hash_index_ratio: f32,
        use_prefix_truncation: bool,
        first_key: &'a [u8],
    ) -> Self {
        let binary_index_len = item_count / usize::from(restart_interval);
        let bucket_count = (item_count as f32 * hash_index_ratio) as u32; // TODO: verify

        Self {
            writer: Vec::with_capacity(u16::MAX.into()),

            binary_index_builder: BinaryIndexBuilder::new(binary_index_len),
            hash_index_builder: HashIndexBuilder::new(bucket_count),

            restart_interval,

            use_prefix_truncation,
            base_key: first_key,

            offset: BlockOffset(0),

            restart_count: 0,
            item_count: 0,
        }
    }

    pub fn write(&mut self, handle: &'a NewKeyedBlockHandle) -> crate::Result<()> {
        // NOTE: Check if we are a restart marker
        if self.item_count % usize::from(self.restart_interval) == 0 {
            // We encode restart markers as:
            // [offset] [size] [key len] [end key]

            self.restart_count += 1;

            // NOTE: We know that data blocks will never even approach 4 GB in size
            #[allow(clippy::cast_possible_truncation)]
            self.binary_index_builder.insert(self.writer.len() as u32);

            self.writer.write_u64_varint(*handle.offset)?;
            self.writer.write_u32_varint(handle.size)?;
            self.writer.write_u16_varint(handle.end_key.len() as u16)?;
            self.writer.write_all(&handle.end_key)?;

            self.base_key = &handle.end_key;
            self.offset = BlockOffset(*handle.offset + u64::from(handle.size));
        } else {
            // We encode truncated handles as:
            // [size] [shared prefix len] [rest key len] [rest key]

            self.writer.write_u32_varint(handle.size)?;

            let shared_prefix_len = if self.use_prefix_truncation {
                // NOTE: We can safely cast to u16, because keys are u16 long max
                #[allow(clippy::cast_possible_truncation)]
                let shared_prefix_len =
                    longest_shared_prefix_length(self.base_key, &handle.end_key) as u16;

                shared_prefix_len
            } else {
                self.writer.write_u8(0)?;
                0
            };

            // TODO: maybe we can skip this varint altogether if prefix truncation = false
            self.writer.write_u16_varint(shared_prefix_len)?;

            // NOTE: We can safely cast to u16, because keys are u16 long max
            #[allow(clippy::cast_possible_truncation)]
            let rest_len = handle.end_key.len() as u16 - shared_prefix_len;

            self.writer.write_u16_varint(rest_len)?;

            let truncated_user_key = handle
                .end_key
                .get(shared_prefix_len as usize..)
                .expect("should be in bounds");

            self.writer.write_all(truncated_user_key)?;

            self.offset += u64::from(handle.size);
        }

        if self.hash_index_builder.bucket_count() > 0 {
            // NOTE: The max binary index is bound by u8 (technically u8::MAX - 2)
            #[allow(clippy::cast_possible_truncation)]
            self.hash_index_builder
                .set(&handle.end_key, (self.restart_count - 1) as u8);
        }

        self.item_count += 1;

        Ok(())
    }

    // TODO: trailer of data block and index block are the same... consolidate into some
    // kind of TrailerWriter or whatever
    pub fn finish(mut self) -> crate::Result<Vec<u8>> {
        // IMPORTANT: Terminator marker
        self.writer.write_u8(TRAILER_START_MARKER)?;

        // TODO: version u8? -> add to segment metadata instead

        // NOTE: We know that data blocks will never even approach 4 GB in size
        #[allow(clippy::cast_possible_truncation)]
        let binary_index_offset = self.writer.len() as u32;

        // Write binary index
        let (binary_index_step_size, binary_index_len) =
            self.binary_index_builder.write(&mut self.writer)?;

        let mut hash_index_offset = 0u32;
        let hash_index_len = self.hash_index_builder.bucket_count();

        // NOTE: We can only use a hash index when there are 254 buckets or less
        // Because 254 and 255 are reserved marker values
        //
        // With the default restart interval of 16, that still gives us support
        // for up to ~4000 KVs
        if self.hash_index_builder.bucket_count() > 0
            && binary_index_len <= MAX_POINTERS_FOR_HASH_INDEX.into()
        {
            // NOTE: We know that data blocks will never even approach 4 GB in size
            #[allow(clippy::cast_possible_truncation)]
            {
                hash_index_offset = self.writer.len() as u32;
            }

            // Write hash index
            self.hash_index_builder.write(&mut self.writer)?;
        }

        // Trailer:
        // [item_count] [restart_interval] [binary_index_offset] [binary_index_len] [hash_index_offset] [hash_index_len]
        {
            #[cfg(debug_assertions)]
            let bytes_before = self.writer.len();

            // NOTE: We know that data blocks will never even approach 4 GB in size, so there can't be that many items either
            #[allow(clippy::cast_possible_truncation)]
            self.writer
                .write_u32::<LittleEndian>(self.item_count as u32)?;

            self.writer.write_u8(self.restart_interval)?;

            self.writer.write_u8(binary_index_step_size)?;

            self.writer.write_u32::<LittleEndian>(binary_index_offset)?;

            // NOTE: Even with a dense index, there can't be more index pointers than items
            #[allow(clippy::cast_possible_truncation)]
            self.writer
                .write_u32::<LittleEndian>(binary_index_len as u32)?;

            self.writer.write_u32::<LittleEndian>(hash_index_offset)?;

            self.writer
                .write_u32::<LittleEndian>(if hash_index_offset > 0 {
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
        }

        Ok(self.writer)
    }
}
