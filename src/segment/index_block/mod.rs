// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod block_handle;
mod forward_reader;

pub use block_handle::{BlockHandle, KeyedBlockHandle};

use super::{
    block::{binary_index::Reader as BinaryIndexReader, BlockOffset, Encoder, Trailer},
    Block,
};
use crate::segment::block::TRAILER_START_MARKER;
use byteorder::{LittleEndian, ReadBytesExt};
use forward_reader::{ForwardReader, ParsedItem, ParsedSlice};
use std::io::{Cursor, Seek};
use varint_rs::VarintReader;

macro_rules! unwrappy {
    ($x:expr) => {
        $x.expect("should read")

        // unsafe { $x.unwrap_unchecked() }
    };
}

/// Block that contains block handles (file offset + size)
#[derive(Clone)]
pub struct IndexBlock {
    pub inner: Block,

    // Cached metadata
    restart_interval: u8,

    binary_index_step_size: u8,
    binary_index_offset: u32,
    binary_index_len: u32,
}

impl IndexBlock {
    #[must_use]
    pub fn new(inner: Block) -> Self {
        let trailer = Trailer::new(&inner);
        let mut reader = trailer.as_slice();

        let _item_count = reader.read_u32::<LittleEndian>().expect("should read");

        let restart_interval = unwrappy!(reader.read_u8());

        let binary_index_step_size = unwrappy!(reader.read_u8());

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        let binary_index_offset = unwrappy!(reader.read_u32::<LittleEndian>());
        let binary_index_len = unwrappy!(reader.read_u32::<LittleEndian>());

        Self {
            inner,

            restart_interval,

            binary_index_step_size,
            binary_index_offset,
            binary_index_len,
        }
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

    /// Access the inner raw bytes
    #[must_use]
    fn bytes(&self) -> &[u8] {
        &self.inner.data
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

    fn get_binary_index_reader(&self) -> BinaryIndexReader {
        BinaryIndexReader::new(
            self.bytes(),
            self.binary_index_offset(),
            self.binary_index_len(),
            self.binary_index_step_size(),
        )
    }

    // TODO: should not return Option<>?
    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn forward_reader(
        &self,
        needle: &[u8],
    ) -> Option<impl Iterator<Item = KeyedBlockHandle> + '_> {
        let offset = self
            .search_lowest(&self.get_binary_index_reader(), needle)
            .unwrap_or_default();

        // SAFETY: pos is always retrieved from the binary index,
        // which we consider to be trustworthy
        #[warn(unsafe_code)]
        let mut cursor = Cursor::new(unsafe { self.inner.data.get_unchecked(offset..) });

        let item = Self::parse_restart_item(&mut cursor, offset)?;

        let key = &self.inner.data[item.end_key.0..item.end_key.1];

        if needle > key {
            return None;
        }

        Some(
            ForwardReader::new(self)
                .with_offset(offset)
                .map(|kv| kv.materialize(&self.inner.data)),
        )
    }

    fn parse_restart_item(reader: &mut Cursor<&[u8]>, pos: usize) -> Option<ParsedItem> {
        let marker = unwrappy!(reader.read_u8());

        if marker == TRAILER_START_MARKER {
            return None;
        }

        let offset = unwrappy!(reader.read_u64_varint());
        let size = unwrappy!(reader.read_u32_varint());

        let key_len: usize = unwrappy!(reader.read_u16_varint()).into();
        let key_start = pos + reader.position() as usize;

        unwrappy!(reader.seek_relative(key_len as i64));

        Some(ParsedItem {
            prefix: None,
            end_key: ParsedSlice(key_start, key_start + key_len),
            offset: BlockOffset(offset),
            size,
        })
    }

    fn parse_truncated_item(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem> {
        let marker = unwrappy!(reader.read_u8());

        if marker == TRAILER_START_MARKER {
            return None;
        }

        let size = unwrappy!(reader.read_u32_varint());

        todo!()
    }

    fn get_key_at(&self, pos: usize) -> &[u8] {
        let bytes = &self.inner.data;

        // SAFETY: pos is always retrieved from the binary index,
        // which we consider to be trustworthy
        #[warn(unsafe_code)]
        let mut cursor = Cursor::new(unsafe { bytes.get_unchecked(pos..) });

        let item = Self::parse_restart_item(&mut cursor, pos).expect("should exist");

        &bytes[item.end_key.0..item.end_key.1]
    }

    /// Search for the lowest block that may possibly contain the needle.
    fn search_lowest(&self, binary_index: &BinaryIndexReader, needle: &[u8]) -> Option<usize> {
        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        while left < right {
            let mid = (left + right) / 2;

            let offset = binary_index.get(mid);

            if self.get_key_at(offset) < needle {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        Some(if left < binary_index.len() {
            binary_index.get(left)
        } else {
            binary_index.get(binary_index.len() - 1)
        })
    }

    /// Search for the last block that may possibly contain the needle.
    fn search_highest(&self, binary_index: &BinaryIndexReader, needle: &[u8]) -> Option<usize> {
        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        while left < right {
            let mid = (left + right) / 2;

            let offset = binary_index.get(mid);

            if self.get_key_at(offset) <= needle {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == 0 {
            Some(binary_index.get(0))
        } else if left == binary_index.len() {
            Some(binary_index.get(binary_index.len() - 1))
        } else {
            Some(binary_index.get(left))
        }
    }

    #[must_use]
    pub fn get_lowest_possible_block(&self, needle: &[u8]) -> Option<KeyedBlockHandle> {
        let binary_index = self.get_binary_index_reader();

        /*
         // NOTE: Currently, the hash index is never initialized for index blocks
         /*  // NOTE: Try hash index if it exists
         if let Some(bucket_value) = self
             .get_hash_index_reader()
             .and_then(|reader| reader.get(key))
         {
             let restart_entry_pos = binary_index.get(usize::from(bucket_value));
             return self.walk(key, seqno, restart_entry_pos, self.restart_interval.into());
         } */
        ) */

        let offset = self.search_lowest(&binary_index, needle)?;

        // SAFETY: offset is always retrieved from the binary index,
        // which we consider to be trustworthy
        #[warn(unsafe_code)]
        let mut cursor = Cursor::new(unsafe { self.inner.data.get_unchecked(offset..) });

        let item = Self::parse_restart_item(&mut cursor, offset)?;

        let key = &self.inner.data[item.end_key.0..item.end_key.1];

        if needle > key {
            return None;
        }

        // TODO: 3.0.0 scan(), delta encoding etc., add test with restart interval > 1

        Some(item.materialize(&self.inner.data))
    }

    #[must_use]
    pub fn get_highest_possible_block(&self, needle: &[u8]) -> Option<KeyedBlockHandle> {
        let binary_index = self.get_binary_index_reader();

        let offset = self.search_highest(&binary_index, needle)?;

        // SAFETY: offset is always retrieved from the binary index,
        // which we consider to be trustworthy
        #[warn(unsafe_code)]
        let mut cursor = Cursor::new(unsafe { self.inner.data.get_unchecked(offset..) });

        let item = Self::parse_restart_item(&mut cursor, offset)?;

        let key = &self.inner.data[item.end_key.0..item.end_key.1];

        if needle > key {
            return None;
        }

        Some(item.materialize(&self.inner.data))
    }

    pub fn encode_items(
        items: &[KeyedBlockHandle],
        restart_interval: u8,
    ) -> crate::Result<Vec<u8>> {
        let first_key = items.first().expect("chunk should not be empty").end_key();

        let mut serializer = Encoder::<'_, BlockOffset, KeyedBlockHandle>::new(
            items.len(),
            restart_interval,
            0.0, // TODO: hard-coded for now
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
    use super::*;
    use crate::segment::block::{Checksum, Header};
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_simple() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        assert_eq!(
            Some(items.first().unwrap().clone()),
            index_block.get_lowest_possible_block(b"a")
        );
        assert_eq!(
            Some(items.first().unwrap().clone()),
            index_block.get_lowest_possible_block(b"b")
        );
        assert_eq!(
            Some(items.get(1).unwrap().clone()),
            index_block.get_lowest_possible_block(b"ba")
        );
        assert_eq!(
            Some(items.get(2).unwrap().clone()),
            index_block.get_lowest_possible_block(b"d")
        );

        // assert_eq!(None, data_block.get_lowest_possible_block(b"zzz"));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_span() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"a".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"a".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"b".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        assert_eq!(
            Some(items.first().unwrap().clone()),
            index_block.get_lowest_possible_block(b"a")
        );
        assert_eq!(
            Some(items.last().unwrap().clone()),
            index_block.get_lowest_possible_block(b"abc")
        );
        assert_eq!(
            Some(items.last().unwrap().clone()),
            index_block.get_lowest_possible_block(b"b")
        );

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_span_highest() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"c".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"c".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"d".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        assert_eq!(
            Some(items.first().unwrap().clone()),
            index_block.get_highest_possible_block(b"a")
        );
        assert_eq!(
            Some(items.get(1).unwrap().clone()),
            index_block.get_highest_possible_block(b"abc")
        );
        assert_eq!(
            Some(items.last().unwrap().clone()),
            index_block.get_highest_possible_block(b"c")
        );
        assert_eq!(
            Some(items.last().unwrap().clone()),
            index_block.get_highest_possible_block(b"cef")
        );
        assert_eq!(
            Some(items.last().unwrap().clone()),
            index_block.get_highest_possible_block(b"d")
        );
        assert_eq!(None, index_block.get_highest_possible_block(b"zzz"));

        Ok(())
    }

    #[test]
    fn v3_index_block_one() -> crate::Result<()> {
        let item = KeyedBlockHandle::new(b"c".into(), BlockOffset(0), 6_000);

        let bytes = IndexBlock::encode_items(&[item.clone()], 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), 1);

        assert_eq!(
            Some(item.clone()),
            index_block.get_lowest_possible_block(b"a")
        );
        assert_eq!(
            Some(item.clone()),
            index_block.get_lowest_possible_block(b"asdasd")
        );
        assert_eq!(
            Some(item.clone()),
            index_block.get_lowest_possible_block(b"b")
        );
        assert_eq!(Some(item), index_block.get_lowest_possible_block(b"c"));
        assert_eq!(None, index_block.get_lowest_possible_block(b"d"));
        assert_eq!(None, index_block.get_lowest_possible_block(b"z"));

        Ok(())
    }

    #[test]
    fn v3_index_block_one_highest() -> crate::Result<()> {
        let item = KeyedBlockHandle::new(b"c".into(), BlockOffset(0), 6_000);

        let bytes = IndexBlock::encode_items(&[item.clone()], 1)?;
        // eprintln!("{bytes:?}");
        // eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), 1);

        assert_eq!(
            Some(item.clone()),
            index_block.get_lowest_possible_block(b"a")
        );
        assert_eq!(
            Some(item.clone()),
            index_block.get_lowest_possible_block(b"asdasd")
        );
        assert_eq!(
            Some(item.clone()),
            index_block.get_lowest_possible_block(b"b")
        );
        assert_eq!(Some(item), index_block.get_lowest_possible_block(b"c"));
        assert_eq!(None, index_block.get_lowest_possible_block(b"d"));
        assert_eq!(None, index_block.get_lowest_possible_block(b"z"));

        Ok(())
    }
}
