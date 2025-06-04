// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    forward_reader::{ForwardReader, ParsedItem},
    DataBlock,
};
use std::io::Cursor;

#[derive(Debug)]
struct HiScanner {
    offset: usize,
    ptr_idx: usize,
    stack: Vec<usize>, // TODO: SmallVec?
    base_key_offset: Option<usize>,
}

/// Double-ended iterator over data blocks
pub struct Iter<'a> {
    block: &'a DataBlock,
    restart_interval: usize,

    lo_scanner: ForwardReader<'a>,
    hi_scanner: HiScanner,
}

impl<'a> Iter<'a> {
    #[must_use]
    pub fn new(block: &'a DataBlock) -> Self {
        let restart_interval = block.restart_interval.into();
        let binary_index_len = block.binary_index_len as usize;

        Self {
            block,

            restart_interval,

            lo_scanner: ForwardReader::new(block),

            /* lo_scanner: LoScanner::default(), */
            hi_scanner: HiScanner {
                offset: 0,
                ptr_idx: binary_index_len,
                stack: Vec::new(),
                base_key_offset: None,
            },
        }
    }

    /* pub fn with_offset(mut self, offset: usize) -> Self {
        self.lo_scanner.offset = offset;
        self
    } */

    fn parse_restart_item(
        block: &DataBlock,
        offset: &mut usize,
        base_key_offset: &mut Option<usize>,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = DataBlock::parse_restart_item(&mut reader, *offset)?;

        *offset += reader.position() as usize;
        *base_key_offset = Some(item.key.0);

        Some(item)
    }

    fn parse_truncated_item(
        block: &DataBlock,
        offset: &mut usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = DataBlock::parse_truncated_item(&mut reader, *offset, base_key_offset)?;

        *offset += reader.position() as usize;

        Some(item)
    }

    fn consume_stack_top(&mut self) -> Option<ParsedItem> {
        if let Some(offset) = self.hi_scanner.stack.pop() {
            if self.lo_scanner.offset() > 0 && offset < self.lo_scanner.offset() {
                return None;
            }

            self.hi_scanner.offset = offset;

            let is_restart = self.hi_scanner.stack.is_empty();

            if is_restart {
                Self::parse_restart_item(
                    self.block,
                    &mut self.hi_scanner.offset,
                    &mut self.hi_scanner.base_key_offset,
                )
            } else {
                Self::parse_truncated_item(
                    self.block,
                    &mut self.hi_scanner.offset,
                    self.hi_scanner.base_key_offset.expect("should exist"),
                )
            }
        } else {
            None
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = ParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset() >= self.hi_scanner.offset
        {
            return None;
        }

        /* let is_restart = self.lo_scanner.remaining_in_interval == 0;

        let item = if is_restart {
            self.lo_scanner.remaining_in_interval = self.restart_interval;

            Self::parse_restart_item(
                self.block,
                &mut self.lo_scanner.offset,
                &mut self.lo_scanner.base_key_offset,
            )
        } else {
            Self::parse_truncated_item(
                self.block,
                &mut self.lo_scanner.offset,
                self.lo_scanner.base_key_offset.expect("should exist"),
            )
        };

        self.lo_scanner.remaining_in_interval -= 1; */

        let item = self.lo_scanner.next();

        if self.hi_scanner.base_key_offset.is_some()
            && self.lo_scanner.offset() >= self.hi_scanner.offset
        {
            return None;
        }

        item
    }
}

impl DoubleEndedIterator for Iter<'_> {
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

        let binary_index = self.block.get_binary_index_reader();

        {
            self.hi_scanner.offset = binary_index.get(self.hi_scanner.ptr_idx);
            let offset = self.hi_scanner.offset;

            if Self::parse_restart_item(
                self.block,
                &mut self.hi_scanner.offset,
                &mut self.hi_scanner.base_key_offset,
            )
            .is_some()
            {
                self.hi_scanner.stack.push(offset);
            }
        }

        for _ in 1..self.restart_interval {
            let offset = self.hi_scanner.offset;

            if Self::parse_truncated_item(
                self.block,
                &mut self.hi_scanner.offset,
                self.hi_scanner.base_key_offset.expect("should exist"),
            )
            .is_some()
            {
                self.hi_scanner.stack.push(offset);
            }
        }

        if self.hi_scanner.stack.is_empty() {
            return None;
        }

        self.consume_stack_top()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        segment::{
            block::{BlockOffset, Checksum, Header},
            Block,
        },
        InternalValue,
        ValueType::Value,
    };
    use test_log::test;

    #[test]
    fn v3_data_block_consume_last_back() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        {
            let mut iter = data_block.iter();
            assert_eq!(b"pla:earth:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:mass", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:name", &*iter.next().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:radius",
                &*iter.next_back().unwrap().key.user_key
            );
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
        }

        {
            let mut iter = data_block.iter();
            assert_eq!(b"pla:earth:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:mass", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:jupiter:name", &*iter.next().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:radius",
                &*iter.next_back().unwrap().key.user_key
            );
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_consume_last_forwards() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        {
            let mut iter = data_block.iter().rev();
            assert_eq!(b"pla:earth:fact", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:fact",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:mass",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:name",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(b"pla:jupiter:radius", &*iter.next().unwrap().key.user_key);
            assert!(iter.next().is_none());
            assert!(iter.next_back().is_none());
        }

        {
            let mut iter = data_block.iter().rev();
            assert_eq!(b"pla:earth:fact", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(
                b"pla:jupiter:fact",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:mass",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(
                b"pla:jupiter:name",
                &*iter.next_back().unwrap().key.user_key
            );
            assert_eq!(b"pla:jupiter:radius", &*iter.next().unwrap().key.user_key);
            assert!(iter.next_back().is_none());
            assert!(iter.next().is_none());
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_ping_pong_exhaust() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("a", "a", 0, Value),
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 0, Value),
            InternalValue::from_components("e", "e", 0, Value),
        ];

        for restart_interval in 1..=u8::MAX {
            let bytes = DataBlock::encode_items(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block.iter();
                assert_eq!(b"a", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"b", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"c", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"d", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"e", &*iter.next().unwrap().key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block.iter();
                assert_eq!(b"e", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"d", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"c", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"b", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"a", &*iter.next_back().unwrap().key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }

            {
                let mut iter = data_block.iter();
                assert_eq!(b"a", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"b", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"c", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"d", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"e", &*iter.next().unwrap().key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block.iter();
                assert_eq!(b"e", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"d", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"c", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"b", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"a", &*iter.next_back().unwrap().key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }
        }

        Ok(())
    }

    /*  #[test]
        fn v3_data_block_ping_pongs()   -> crate::Result<()>{
            use crate::{UserKey, UserValue};


            pub struct BinaryCodeIterator {
        length: usize,
        current_number: u128, // Use u128 to support lengths up to 128 bits
        max_number: u128,
    }

    impl BinaryCodeIterator {
        /// Creates a new iterator for all binary codes of a given length.
        ///
        /// # Panics
        /// Panics if `length` is greater than 128, as `u128` cannot hold
        /// numbers with more than 128 bits.
        pub fn new(length: usize) -> Self {
            if length > 128 {
                panic!("Length too large for u128 to represent all combinations.");
            }
            let max_number = if length == 0 {
                0 // Special case for length 0, only one combination (empty vector)
            } else {
                (1 << length) - 1 // 2^len - 1 is the maximum value for a 'len'-bit number
            };
            BinaryCodeIterator {
                length,
                current_number: 0,
                max_number,
            }
        }
    }

    impl Iterator for BinaryCodeIterator {
        // The iterator will yield Vec<u8> where each u8 is either 0 or 1.
        type Item = Vec<u8>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.current_number > self.max_number {
                return None; // All codes have been generated
            }

            // Convert the current_number into a binary Vec<u8>
            let mut code = Vec::with_capacity(self.length);
            if self.length == 0 {
                // For length 0, only one item: an empty vector
                // We've handled max_number=0 already, so this will only run once.
            } else {
                // Iterate from the least significant bit (LSB) to the most significant bit (MSB)
                // or from MSB to LSB depending on desired order.
                // This implementation generates from MSB to LSB to match typical binary representation
                // e.g., 0b101 -> [1, 0, 1]
                for i in (0..self.length).rev() {
                    // Check if the i-th bit is set
                    if (self.current_number >> i) & 1 == 1 {
                        code.push(1);
                    } else {
                        code.push(0);
                    }
                }
            }


            // Increment for the next iteration
            self.current_number += 1;

            Some(code)
        }
    }

            let items = [
                InternalValue::from_components(UserKey::from([22, 192]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 193]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 194]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 195]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 196]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 197]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 198]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 199]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 200]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 201]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 202]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 203]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 204]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 205]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 206]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 207]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 208]), UserValue::from([]), 0, Value),
                InternalValue::from_components(UserKey::from([22, 209]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 210]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 211]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 212]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 213]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 214]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 215]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 216]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 217]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 218]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 219]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 220]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 221]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 222]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 223]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 224]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 225]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 226]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 227]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 228]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 229]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 230]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 231]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 232]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 233]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 234]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 235]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 236]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 237]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 238]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 239]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 240]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 241]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 242]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 243]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 244]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 245]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 246]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 247]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 248]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 249]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 250]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 251]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 252]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 253]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 254]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 22, 255]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 0]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 1]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 2]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 3]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 4]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 5]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 6]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 7]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 8]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 9]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 10]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 11]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 12]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 13]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 14]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 15]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 16]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 17]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 18]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 19]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 20]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 21]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 22]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 23]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 24]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 25]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 26]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 27]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 28]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 29]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 30]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 31]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 32]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 33]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 34]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 35]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 36]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 37]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 38]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 39]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 40]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 41]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 42]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 43]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 44]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 45]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 46]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 47]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 48]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 49]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 50]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 51]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 52]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 53]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 54]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 55]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 56]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 57]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 58]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 59]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 60]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 61]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 62]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 63]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 64]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 65]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 66]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 67]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 68]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 69]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 70]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 71]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 72]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 73]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 74]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 75]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 76]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 77]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 78]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 79]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 80]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 81]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 82]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 83]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 84]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 85]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 86]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 87]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 88]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 89]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 90]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 91]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 92]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 93]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 94]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 95]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 96]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 97]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 98]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 99]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 100]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 101]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 102]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 103]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 104]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 105]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 106]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 107]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 108]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 109]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 110]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 111]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 112]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 113]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 114]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 115]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 116]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 117]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 118]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 119]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 120]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 121]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 122]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 123]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 124]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 125]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 126]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 127]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 128]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 129]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 130]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 131]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 132]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 133]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 134]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 135]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 136]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 137]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 138]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 139]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 140]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 141]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 142]), UserValue::from([]), 0, Value),
                // InternalValue::from_components(UserKey::from([0, 0, 0, 0, 0, 0, 23, 143]), UserValue::from([]), 0, Value),
            ];

            let bytes = DataBlock::encode_items(&items, 1, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            for code in BinaryCodeIterator::new(items.len()) {
                let mut iter = data_block.iter();

                for &x in &code {
                    log::warn!("code: {code:?}");

                    if x % 2 == 0 {
                        eprintln!("[{x}] next");

                        let Some(_) = iter.next() else {
                            break;
                        };

                        // count += 1;
                    } else {
                        eprintln!("[{x}] next_back");

                        let Some(_) = iter.next_back() else {
                            break;
                        };

                        // count += 1;
                    }
                }
            }

            Ok(())
        } */
}
