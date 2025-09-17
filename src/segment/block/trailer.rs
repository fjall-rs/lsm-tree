// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    encoder::{Encodable, Encoder},
    Block,
};
use crate::segment::block::hash_index::MAX_POINTERS_FOR_HASH_INDEX;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub const TRAILER_START_MARKER: u8 = 255;

const TRAILER_SIZE: usize = 5 * std::mem::size_of::<u32>() + (2 * std::mem::size_of::<u8>());

/// Block trailer
///
/// ## Format
///
/// \[item_count\] \[restart_interval\] \[binary_index_offset\] \[binary_index_len\] \[hash_index_offset\] \[hash_index_len\]
#[allow(clippy::doc_markdown)]
pub struct Trailer<'a> {
    block: &'a Block,
}

impl<'a> Trailer<'a> {
    pub fn new(block: &'a Block) -> Self {
        Self { block }
    }

    /// Returns the trailer position.
    pub fn trailer_offset(&self) -> usize {
        self.block.data.len() - TRAILER_SIZE
    }

    /// Returns the number of items in the block
    #[must_use]
    pub fn item_count(&self) -> usize {
        let mut reader = self.as_slice();

        // NOTE: We know the trailer offset is valid, and the trailer has a fixed size
        // so the next item must be the item count
        #[allow(clippy::expect_used)]
        {
            reader
                .read_u32::<LittleEndian>()
                .expect("should read item count") as usize
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        let start = self.trailer_offset();

        // SAFETY: We know that a block always has a trailer, so the
        // `block_size - TRAILER_SIZE` cannot go out of bounds
        #[allow(unsafe_code)]
        unsafe {
            self.block.data.get_unchecked(start..)
        }
    }

    pub fn write<S: Default, T: Encodable<S>>(mut encoder: Encoder<'_, S, T>) -> crate::Result<()> {
        // IMPORTANT: Terminator marker
        encoder.writer.write_u8(TRAILER_START_MARKER)?;

        // TODO: version u8? -> add to segment metadata instead

        // NOTE: We know that data blocks will never even approach 4 GB in size
        #[allow(clippy::cast_possible_truncation)]
        let binary_index_offset = encoder.writer.len() as u32;

        // Write binary index
        let (binary_index_step_size, binary_index_len) =
            encoder.binary_index_builder.write(&mut encoder.writer)?;

        let mut hash_index_offset = 0u32;
        let hash_index_len = encoder.hash_index_builder.bucket_count();

        // NOTE: We can only use a hash index when there are 254 buckets or less
        // Because 254 and 255 are reserved marker values
        //
        // With the default restart interval of 16, that still gives us support
        // for up to ~4000 KVs
        if encoder.hash_index_builder.bucket_count() > 0
            && binary_index_len <= MAX_POINTERS_FOR_HASH_INDEX
        {
            // NOTE: We know that data blocks will never even approach 4 GB in size
            #[allow(clippy::cast_possible_truncation)]
            {
                hash_index_offset = encoder.writer.len() as u32;
            }

            // Write hash index
            encoder.hash_index_builder.write(&mut encoder.writer)?;
        }

        // Write trailer

        #[cfg(debug_assertions)]
        let bytes_before = encoder.writer.len();

        // NOTE: We know that data blocks will never even approach 4 GB in size, so there can't be that many items either
        #[allow(clippy::cast_possible_truncation)]
        encoder
            .writer
            .write_u32::<LittleEndian>(encoder.item_count as u32)?;

        encoder.writer.write_u8(encoder.restart_interval)?;

        encoder.writer.write_u8(binary_index_step_size)?;

        encoder
            .writer
            .write_u32::<LittleEndian>(binary_index_offset)?;

        // NOTE: Even with a dense index, there can't be more index pointers than items
        #[allow(clippy::cast_possible_truncation)]
        encoder
            .writer
            .write_u32::<LittleEndian>(binary_index_len as u32)?;

        encoder
            .writer
            .write_u32::<LittleEndian>(hash_index_offset)?;

        encoder
            .writer
            .write_u32::<LittleEndian>(if hash_index_offset > 0 {
                hash_index_len
            } else {
                0
            })?;

        #[cfg(debug_assertions)]
        assert_eq!(
            TRAILER_SIZE,
            encoder.writer.len() - bytes_before,
            "trailer size does not match",
        );

        Ok(())
    }
}
