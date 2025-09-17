// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{binary_index::Reader as BinaryIndexReader, hash_index::Reader as HashIndexReader};
use crate::{
    segment::{block::Trailer, Block},
    unwrap, Slice,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::{io::Cursor, marker::PhantomData};

/// Represents an object that was parsed from a byte array
///
/// Parsed items only hold references to their keys and values, use `materialize` to create an owned value.
pub trait ParsedItem<M> {
    /// Compares this item's key with a needle.
    ///
    /// We can not access the key directly because it may be comprised of prefix + suffix.
    fn compare_key(&self, needle: &[u8], bytes: &[u8]) -> std::cmp::Ordering;

    /// Returns the byte offset of the key's start position.
    fn key_offset(&self) -> usize;

    /// Converts the parsed representation to an owned value.
    fn materialize(&self, bytes: &Slice) -> M;
}

/// Describes an object that can be parsed from a block, either a full item (restart head), or a truncated item
pub trait Decodable<ParsedItem> {
    /// Parses the key of the next restart head from a reader.
    ///
    /// This is used for the binary search index.
    fn parse_restart_key<'a>(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        data: &'a [u8],
    ) -> Option<&'a [u8]>;

    /// Parses a restart head from a reader.
    ///
    /// `offset` is the position of the item to read in the block's byte slice.
    fn parse_full(reader: &mut Cursor<&[u8]>, offset: usize) -> Option<ParsedItem>;

    /// Parses a (possibly) prefix truncated item from a reader.
    fn parse_truncated(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem>;
}

#[derive(Debug)]
struct LoScanner {
    offset: usize,
    remaining_in_interval: usize,
    base_key_offset: Option<usize>,
}

#[derive(Debug)]
struct HiScanner {
    offset: usize,
    ptr_idx: usize,
    stack: Vec<usize>, // TODO: SmallVec?
    base_key_offset: Option<usize>,
}

/// Generic block decoder for RocksDB-style blocks
///
/// Supports prefix truncation and binary search index (through restart intervals).
pub struct Decoder<'a, Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> {
    block: &'a Block,
    phantom: PhantomData<(Item, Parsed)>,

    lo_scanner: LoScanner,
    hi_scanner: HiScanner,

    // Cached metadata
    pub(crate) restart_interval: u8,

    binary_index_step_size: u8,
    binary_index_offset: u32,
    binary_index_len: u32,
}

impl<'a, Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> Decoder<'a, Item, Parsed> {
    #[must_use]
    pub fn new(block: &'a Block) -> Self {
        let trailer = Trailer::new(block);
        let mut reader = trailer.as_slice();

        let _item_count = reader.read_u32::<LittleEndian>().expect("should read");

        let restart_interval = unwrap!(reader.read_u8());

        let binary_index_step_size = unwrap!(reader.read_u8());

        debug_assert!(
            binary_index_step_size == 2 || binary_index_step_size == 4,
            "invalid binary index step size",
        );

        let binary_index_offset = unwrap!(reader.read_u32::<LittleEndian>());
        let binary_index_len = unwrap!(reader.read_u32::<LittleEndian>());

        Self {
            block,
            phantom: PhantomData,

            lo_scanner: LoScanner {
                offset: 0,
                remaining_in_interval: 0,
                base_key_offset: None,
            },

            hi_scanner: HiScanner {
                offset: 0,
                ptr_idx: binary_index_len as usize,
                stack: Vec::new(),
                base_key_offset: None,
            },

            restart_interval,

            binary_index_step_size,
            binary_index_offset,
            binary_index_len,
        }
    }

    #[must_use]
    pub fn block(&self) -> &Block {
        self.block
    }

    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.block.data
    }

    /// Returns the number of items in the block.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Trailer::new(self.block).item_count()
    }

    fn get_binary_index_reader(&self) -> BinaryIndexReader<'_> {
        BinaryIndexReader::new(
            &self.block.data,
            self.binary_index_offset,
            self.binary_index_len,
            self.binary_index_step_size,
        )
    }

    fn get_key_at(&self, pos: usize) -> &[u8] {
        let bytes = &self.block.data;

        // SAFETY: pos is always retrieved from the binary index,
        // which we consider to be trustworthy
        #[warn(unsafe_code)]
        let mut cursor = Cursor::new(unsafe { bytes.get_unchecked(pos..) });

        Item::parse_restart_key(&mut cursor, pos, bytes).expect("should exist")
    }

    fn partition_point(
        &self,
        pred: impl Fn(&[u8]) -> bool,
    ) -> Option<(/* offset */ usize, /* idx */ usize)> {
        let binary_index = self.get_binary_index_reader();

        debug_assert!(
            binary_index.len() >= 1,
            "binary index should never be empty",
        );

        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        while left < right {
            let mid = (left + right) / 2;

            let offset = binary_index.get(mid);

            let head_key = self.get_key_at(offset);

            if pred(head_key) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == 0 {
            return Some((0, 0));
        }

        let offset = binary_index.get(left - 1);

        Some((offset, left - 1))
    }

    // TODO:
    fn partition_point_2(
        &self,
        pred: impl Fn(&[u8]) -> bool,
    ) -> Option<(/* offset */ usize, /* idx */ usize)> {
        let binary_index = self.get_binary_index_reader();

        debug_assert!(
            binary_index.len() >= 1,
            "binary index should never be empty",
        );

        let mut left: usize = 0;
        let mut right = binary_index.len();

        if right == 0 {
            return None;
        }

        while left < right {
            let mid = (left + right) / 2;

            let offset = binary_index.get(mid);

            let head_key = self.get_key_at(offset);

            if pred(head_key) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == binary_index.len() {
            let idx = binary_index.len() - 1;
            let offset = binary_index.get(idx);
            return Some((offset, idx));
        }

        let offset = binary_index.get(left);

        Some((offset, left))
    }

    pub fn set_lo_offset(&mut self, offset: usize) {
        self.lo_scanner.offset = offset;
    }

    /// Seeks using the given predicate.
    ///
    /// Returns `false` if the key does not possible exist.
    pub fn seek(&mut self, pred: impl Fn(&[u8]) -> bool, second_partition: bool) -> bool {
        // TODO: make this nicer, maybe predicate that can affect the resulting index...?
        let result = if second_partition {
            self.partition_point_2(pred)
        } else {
            self.partition_point(pred)
        };

        // Binary index lookup
        let Some((offset, _)) = result else {
            return false;
        };

        self.lo_scanner.offset = offset;

        true
    }

    /// Seeks the upper bound using the given predicate.
    ///
    /// Returns `false` if the key does not possible exist.
    pub fn seek_upper(&mut self, pred: impl Fn(&[u8]) -> bool, second_partition: bool) -> bool {
        let result = if second_partition {
            self.partition_point_2(pred)
        } else {
            self.partition_point(pred)
        };

        // Binary index lookup
        let Some((offset, idx)) = result else {
            return false;
        };

        self.hi_scanner.offset = offset;
        self.hi_scanner.ptr_idx = idx;
        self.hi_scanner.stack.clear();
        self.hi_scanner.base_key_offset = None;

        self.fill_stack();

        true
    }

    fn parse_current_item(
        reader: &mut Cursor<&[u8]>,
        offset: usize,
        base_key_offset: Option<usize>,
        is_restart: bool,
    ) -> Option<Parsed> {
        if is_restart {
            Item::parse_full(reader, offset)
        } else {
            Item::parse_truncated(reader, offset, base_key_offset.expect("should exist"))
        }
    }

    fn fill_stack(&mut self) {
        let binary_index = self.get_binary_index_reader();

        {
            self.hi_scanner.offset = binary_index.get(self.hi_scanner.ptr_idx);

            let offset = self.hi_scanner.offset;

            // SAFETY: The cursor is advanced by read_ operations which check for EOF,
            // And the cursor starts at 0 - the slice is never empty
            #[warn(unsafe_code)]
            let mut reader = Cursor::new(unsafe { self.block.data.get_unchecked(offset..) });

            if Item::parse_full(&mut reader, offset)
                .inspect(|item| {
                    self.hi_scanner.offset += reader.position() as usize;
                    self.hi_scanner.base_key_offset = Some(item.key_offset());
                })
                .is_some()
            {
                self.hi_scanner.stack.push(offset);
            }
        }

        for _ in 1..self.restart_interval {
            let offset = self.hi_scanner.offset;

            // SAFETY: The cursor is advanced by read_ operations which check for EOF,
            // And the cursor starts at 0 - the slice is never empty
            #[warn(unsafe_code)]
            let mut reader = Cursor::new(unsafe { self.block.data.get_unchecked(offset..) });

            if Item::parse_truncated(
                &mut reader,
                offset,
                self.hi_scanner.base_key_offset.expect("should exist"),
            )
            .inspect(|_| {
                self.hi_scanner.offset += reader.position() as usize;
            })
            .is_some()
            {
                self.hi_scanner.stack.push(offset);
            } else {
                break;
            }
        }
    }

    fn consume_stack_top(&mut self) -> Option<Parsed> {
        let offset = self.hi_scanner.stack.pop()?;

        if self.lo_scanner.offset > 0 && offset < self.lo_scanner.offset {
            return None;
        }

        self.hi_scanner.offset = offset;

        let is_restart = self.hi_scanner.stack.is_empty();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { self.block.data.get_unchecked(offset..) });

        Self::parse_current_item(
            &mut reader,
            offset,
            self.hi_scanner.base_key_offset,
            is_restart,
        )
    }
}

impl<Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> Iterator for Decoder<'_, Item, Parsed> {
    type Item = Parsed;

    fn next(&mut self) -> Option<Self::Item> {
        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset >= self.hi_scanner.offset
        {
            return None;
        }

        let is_restart: bool = self.lo_scanner.remaining_in_interval == 0;

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader =
            Cursor::new(unsafe { self.block.data.get_unchecked(self.lo_scanner.offset..) });

        let item = Self::parse_current_item(
            &mut reader,
            self.lo_scanner.offset,
            self.lo_scanner.base_key_offset,
            is_restart,
        )
        .inspect(|item| {
            self.lo_scanner.offset += reader.position() as usize;

            if is_restart {
                self.lo_scanner.base_key_offset = Some(item.key_offset());
            }
        });

        if is_restart {
            self.lo_scanner.remaining_in_interval = usize::from(self.restart_interval) - 1;
        } else {
            self.lo_scanner.remaining_in_interval -= 1;
        }

        item
    }
}

impl<Item: Decodable<Parsed>, Parsed: ParsedItem<Item>> DoubleEndedIterator
    for Decoder<'_, Item, Parsed>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(top) = self.consume_stack_top() {
            return Some(top);
        }

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_scanner.ptr_idx == usize::MAX {
            return None;
        }

        self.hi_scanner.ptr_idx = self.hi_scanner.ptr_idx.wrapping_sub(1);

        // NOTE: If we wrapped, we are at the end
        // This is safe to do, because there cannot be that many restart intervals
        if self.hi_scanner.ptr_idx == usize::MAX {
            return None;
        }

        self.fill_stack();

        self.consume_stack_top()
    }
}
