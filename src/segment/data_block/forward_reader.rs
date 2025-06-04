// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::DataBlock;
use crate::{key::InternalKey, segment::util::compare_prefixed_slice, InternalValue, SeqNo, Slice};
use std::io::{Cursor, Seek};

/// [start, end] slice indexes
#[derive(Debug)]
pub struct ParsedSlice(pub usize, pub usize);

impl ParsedItem {
    pub fn materialize(&self, bytes: &Slice) -> InternalValue {
        // NOTE: We consider the prefix and key slice indexes to be trustworthy
        #[allow(clippy::indexing_slicing)]
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.key.0..self.key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.key.0..self.key.1)
        };
        let key = InternalKey::new(
            key,
            self.seqno,
            // NOTE: Value type is (or should be) checked when reading it
            #[allow(clippy::expect_used)]
            self.value_type.try_into().expect("should work"),
        );

        let value = self
            .value
            .as_ref()
            .map_or_else(Slice::empty, |v| bytes.slice(v.0..v.1));

        InternalValue { key, value }
    }
}

#[derive(Debug)]
pub struct ParsedItem {
    pub value_type: u8,
    pub seqno: SeqNo,
    pub prefix: Option<ParsedSlice>,
    pub key: ParsedSlice,
    pub value: Option<ParsedSlice>,
}

// TODO: flatten into main struct
#[derive(Default, Debug)]
struct LoScanner {
    offset: usize,
    remaining_in_interval: usize,
    base_key_offset: Option<usize>,
}

/// Specialized reader to scan an index block only in forwards direction
///
/// Is less expensive than a double ended iterator.
pub struct ForwardReader<'a> {
    block: &'a DataBlock,
    restart_interval: usize,
    lo_scanner: LoScanner,
}

impl<'a> ForwardReader<'a> {
    #[must_use]
    pub fn new(block: &'a DataBlock) -> Self {
        let restart_interval = block.restart_interval.into();

        Self {
            block,

            restart_interval,

            lo_scanner: LoScanner::default(),
        }
    }

    #[must_use]
    pub fn offset(&self) -> usize {
        self.lo_scanner.offset
    }

    /// Reads an item by key from the block, if it exists.
    #[must_use]
    pub fn point_read(&mut self, needle: &[u8], seqno: SeqNo) -> Option<InternalValue> {
        let may_exist = self.seek(needle, seqno);

        if !may_exist {
            return None;
        }

        let bytes = self.block.bytes();

        for item in &mut *self {
            let cmp_result = if let Some(prefix) = &item.prefix {
                let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
                let rest_key = unsafe { bytes.get_unchecked(item.key.0..item.key.1) };
                compare_prefixed_slice(prefix, rest_key, needle)
            } else {
                let key = unsafe { bytes.get_unchecked(item.key.0..item.key.1) };
                key.cmp(needle)
            };

            match cmp_result {
                std::cmp::Ordering::Equal => {
                    if item.seqno < seqno {
                        let kv = item.materialize(&self.block.inner.data);
                        return Some(kv);
                    }
                }
                std::cmp::Ordering::Greater => {
                    // Already passed needle
                    return None;
                }
                std::cmp::Ordering::Less => {
                    // Continue to next KV
                }
            }
        }

        None
    }

    /// Seeks to the lowest item that is eligible based on the requested
    /// needle and seqno.
    ///
    /// Returns `false` if `next()` can be safely skipped because the item definitely
    /// does not exist.
    pub fn seek(&mut self, needle: &[u8], seqno: SeqNo) -> bool {
        let binary_index = self.block.get_binary_index_reader();

        // NOTE: Try hash index if it exists
        if let Some(lookup) = self
            .block
            .get_hash_index_reader()
            .map(|reader| reader.get(needle))
        {
            use super::super::block::hash_index::Lookup::{Conflicted, Found, NotFound};

            match lookup {
                Found(bucket_value) => {
                    let offset = binary_index.get(usize::from(bucket_value));
                    self.lo_scanner.offset = offset;
                    self.linear_probe(needle, seqno);
                    return true;
                }
                NotFound => {
                    return false;
                }
                Conflicted => {
                    // NOTE: Fallback to binary search
                }
            }
        }

        let Some(offset) = self
            .block
            .binary_search_for_offset(&binary_index, needle, seqno)
        else {
            return false;
        };

        self.lo_scanner.offset = offset;

        self.linear_probe(needle, seqno)
    }

    fn linear_probe(&mut self, needle: &[u8], seqno: SeqNo /* TODO: use */) -> bool {
        let bytes = self.block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(bytes);

        reader
            .seek_relative(self.lo_scanner.offset as i64)
            .expect("should be in bounds");

        loop {
            let Some(head) = DataBlock::parse_restart_item(&mut reader, 0) else {
                return false;
            };

            let cmp_result = {
                let key = unsafe { bytes.get_unchecked(head.key.0..head.key.1) };
                key.cmp(needle)
            };

            match cmp_result {
                std::cmp::Ordering::Equal => {
                    // TODO: return true
                    return true;
                }
                std::cmp::Ordering::Greater => {
                    // Already passed needle

                    return false;
                }
                std::cmp::Ordering::Less => {
                    // Continue to next KV
                }
            }

            let base_key_offset = head.key.0;
            self.lo_scanner.base_key_offset = Some(base_key_offset);

            self.lo_scanner.remaining_in_interval = self.restart_interval;
            self.lo_scanner.offset = reader.position() as usize;
            self.lo_scanner.remaining_in_interval -= 1;

            for _ in 0..(self.restart_interval - 1) {
                let Some(head) = DataBlock::parse_truncated_item(&mut reader, 0, base_key_offset)
                else {
                    return false;
                };

                let cmp_result = if let Some(prefix) = &head.prefix {
                    let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
                    let rest_key = unsafe { bytes.get_unchecked(head.key.0..head.key.1) };
                    compare_prefixed_slice(prefix, rest_key, needle)
                } else {
                    let key = unsafe { bytes.get_unchecked(head.key.0..head.key.1) };
                    key.cmp(needle)
                };

                match cmp_result {
                    std::cmp::Ordering::Equal => {
                        return true;
                    }
                    std::cmp::Ordering::Greater => {
                        // Already passed needle

                        return false;
                    }
                    std::cmp::Ordering::Less => {
                        // Continue to next KV
                    }
                }

                self.lo_scanner.offset = reader.position() as usize;
                self.lo_scanner.remaining_in_interval -= 1;
            }
        }
    }

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
}

impl Iterator for ForwardReader<'_> {
    type Item = ParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        let is_restart = self.lo_scanner.remaining_in_interval == 0;

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

        self.lo_scanner.remaining_in_interval -= 1;

        item
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        segment::{block::Header, Block, BlockOffset, Checksum},
        Slice,
        ValueType::{Tombstone, Value},
    };
    use test_log::test;

    #[test]
    fn v3_data_block_seek_too_low() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert!(
            data_block.point_read(b"a", SeqNo::MAX).is_none(),
            "should return None because a does not exist",
        );

        assert!(
            data_block.point_read(b"b", SeqNo::MAX).is_some(),
            "should return Some because b exists",
        );

        assert!(
            data_block.point_read(b"z", SeqNo::MAX).is_none(),
            "should return Some because z does not exist",
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_snapshot_read_first() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "hello",
            "world",
            0,
            crate::ValueType::Value,
        )];

        let bytes = DataBlock::encode_items(&items, 16, 0.0)?;
        let serialized_len = bytes.len();

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
        assert!(!data_block.is_empty());
        assert_eq!(data_block.inner.size(), serialized_len);

        assert_eq!(Some(items[0].clone()), data_block.point_read(b"hello", 777));

        Ok(())
    }

    #[test]
    fn v3_data_block_point_read_one() -> crate::Result<()> {
        let items = [InternalValue::from_components(
            "pla:earth:fact",
            "eaaaaaaaaarth",
            0,
            crate::ValueType::Value,
        )];

        let bytes = DataBlock::encode_items(&items, 16, 0.0)?;
        let serialized_len = bytes.len();

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
        assert!(!data_block.is_empty());
        assert_eq!(data_block.inner.size(), serialized_len);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, SeqNo::MAX),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
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

        for restart_interval in 1..=20 {
            let bytes = DataBlock::encode_items(&items, restart_interval, 1.33)?;

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
            assert!(data_block.hash_bucket_count().unwrap() > 0);

            for needle in &items {
                assert_eq!(
                    Some(needle.clone()),
                    data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
                );
            }

            assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], b"", 23_523_531_241_241_242, Value),
            InternalValue::from_components([0], b"", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components([0], [], 5, Value),
            InternalValue::from_components([0], [], 4, Tombstone),
            InternalValue::from_components([0], [], 3, Value),
            InternalValue::from_components([0], [], 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

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

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

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
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        assert_eq!(items, *data_block.iter().collect::<Vec<_>>(),);

        Ok(())
    }

    #[test]
    fn v3_data_block_fuzz_4() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::new(&[0]),
                Slice::new(&[]),
                3_834_029_160_418_063_669,
                Value,
            ),
            InternalValue::from_components(Slice::new(&[0]), Slice::new(&[]), 127, Tombstone),
            InternalValue::from_components(
                Slice::new(&[53, 53, 53]),
                Slice::new(&[]),
                18_446_744_073_709_551_615,
                Tombstone,
            ),
            InternalValue::from_components(
                Slice::new(&[255]),
                Slice::new(&[]),
                18_446_744_069_414_584_831,
                Tombstone,
            ),
            InternalValue::from_components(Slice::new(&[255, 255]), Slice::new(&[]), 47, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 1.0)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for item in data_block.iter() {
            eprintln!("{item:?}");
        }

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"b", b"b", 2, Value),
            InternalValue::from_components(b"c", b"c", 1, Value),
            InternalValue::from_components(b"d", b"d", 65, Value),
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

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, SeqNo::MAX),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

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

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(b"a", b"a", 3, Value),
            InternalValue::from_components(b"a", b"a", 2, Value),
            InternalValue::from_components(b"a", b"a", 1, Value),
            InternalValue::from_components(b"b", b"b", 65, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            Some(items.first().cloned().unwrap()),
            data_block.point_read(b"a", SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(b"b", SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_1() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

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

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_2() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

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

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_3() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
        ];

        let bytes = DataBlock::encode_items(&items, 2, 0.0)?;

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

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_mvcc_latest_fuzz_3_dense() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 8, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 7, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 6, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 5, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 4, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 3, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 2, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 1, Value),
            InternalValue::from_components(Slice::from([233, 233]), Slice::from([]), 0, Value),
            InternalValue::from_components(
                Slice::from([255, 255, 0]),
                Slice::from([]),
                127_886_946_205_696,
                Tombstone,
            ),
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

        assert_eq!(
            Some(items.get(1).cloned().unwrap()),
            data_block.point_read(&[233, 233], SeqNo::MAX)
        );
        assert_eq!(
            Some(items.last().cloned().unwrap()),
            data_block.point_read(&[255, 255, 0], SeqNo::MAX)
        );
        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

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

        for needle in items {
            eprintln!("NEEDLE {needle:?}");

            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

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

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert!(data_block
            .point_read(b"pla:venus:fact", SeqNo::MAX)
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

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        assert_eq!(None, data_block.point_read(b"yyy", SeqNo::MAX));

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

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len()
        );

        assert_eq!(data_block.iter().collect::<Vec<_>>(), items);

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

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        assert_eq!(items, *data_block.iter().collect::<Vec<_>>(),);

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

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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

        assert_eq!(items.len(), {
            #[allow(clippy::suspicious_map)]
            data_block.iter().count()
        });

        assert_eq!(items, *data_block.iter().collect::<Vec<_>>(),);

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

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(items.len(), {
            #[allow(clippy::suspicious_map)]
            data_block.iter().rev().count()
        });

        assert_eq!(
            items.into_iter().rev().collect::<Vec<_>>(),
            data_block.iter().rev().collect::<Vec<_>>(),
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_ping_pong() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        {
            let mut iter = data_block.iter();

            assert_eq!(b"pla:saturn:fact", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:venus:name", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(b"pla:saturn:name", &*iter.next().unwrap().key.user_key);
            assert_eq!(b"pla:venus:fact", &*iter.next_back().unwrap().key.user_key);

            let last = iter.next().unwrap().key;
            assert_eq!(b"pla:venus:fact", &*last.user_key);
            assert_eq!(Tombstone, last.value_type);
            assert_eq!(1, last.seqno);
        }

        {
            let mut iter = data_block.iter();

            assert_eq!(b"pla:venus:name", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(
                b"pla:saturn:fact",
                &*iter
                    .next()
                    .inspect(|v| {
                        eprintln!("{:?}", String::from_utf8_lossy(&v.key.user_key));
                    })
                    .unwrap()
                    .key
                    .user_key
            );
            assert_eq!(b"pla:venus:fact", &*iter.next_back().unwrap().key.user_key);
            assert_eq!(b"pla:saturn:name", &*iter.next().unwrap().key.user_key);

            let last = iter.next_back().unwrap().key;
            assert_eq!(b"pla:venus:fact", &*last.user_key);
            assert_eq!(Tombstone, last.value_type);
            assert_eq!(1, last.seqno);
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_range() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.range(&((b"pla:venus:" as &[u8])..)).count()
            },
            3,
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_range_rev() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:saturn:fact", "Saturn is pretty big", 0, Value),
            InternalValue::from_components("pla:saturn:name", "Saturn", 0, Value),
            InternalValue::from_components("pla:venus:fact", "", 1, Tombstone),
            InternalValue::from_components("pla:venus:fact", "Venus exists", 0, Value),
            InternalValue::from_components("pla:venus:name", "Venus", 0, Value),
        ];

        let bytes = DataBlock::encode_items(&items, 16, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block
                    .range(&((b"pla:venus:" as &[u8])..))
                    .rev()
                    .count()
            },
            3,
        );

        Ok(())
    }

    #[test]
    fn v3_data_block_small_hash_ratio() -> crate::Result<()> {
        let items = (0u64..254)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        // NOTE: If >0.0, buckets are at least 1
        let bytes = DataBlock::encode_items(&items, 1, 0.0001)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_just_enough_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..254)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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
        assert!(data_block.hash_bucket_count().unwrap() > 0);

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_too_many_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..255)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_way_too_many_pointers_for_hash_bucket() -> crate::Result<()> {
        let items = (0u64..1_000)
            .map(|x| InternalValue::from_components(x.to_be_bytes(), x.to_be_bytes(), 0, Value))
            .collect::<Vec<_>>();

        let bytes = DataBlock::encode_items(&items, 1, 1.33)?;

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

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
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
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(data_block.hash_bucket_count().is_none());

        for needle in items {
            assert_eq!(
                Some(needle.clone()),
                data_block.point_read(&needle.key.user_key, needle.key.seqno + 1),
            );
        }

        Ok(())
    }
}
