// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt},
    segment::{
        block::{Decoder, ParsedItem},
        data_block::DataBlockParsedItem,
    },
    InternalValue,
};

/// The data block iterator handles double-ended scans over a data block
pub struct Iter<'a> {
    bytes: &'a [u8],
    decoder:
        DoubleEndedPeekable<DataBlockParsedItem, Decoder<'a, InternalValue, DataBlockParsedItem>>,
}

impl<'a> Iter<'a> {
    /// Creates a new iterator over a data block.
    #[must_use]
    pub fn new(bytes: &'a [u8], decoder: Decoder<'a, InternalValue, DataBlockParsedItem>) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self { bytes, decoder }
    }

    /// Seek the iterator to an byte offset.
    ///
    /// This is used when the hash index returns a hit.
    pub fn seek_to_offset(&mut self, offset: usize) -> bool {
        self.decoder.inner_mut().set_lo_offset(offset);
        true
    }

    // TODO: the peek() + next() pattern is a bit unfortunate
    // TODO: maybe just seek the decoder, and then let the caller handle the linear search...
    pub fn seek(&mut self, needle: &[u8]) -> bool {
        if !self
            .decoder
            .inner_mut()
            .seek(|head_key| head_key < needle, false)
        {
            return false;
        }

        // TODO: make sure we only linear scan over the current restart interval
        // TODO: if we do more steps, something has gone wrong with the seek probably, maybe...?

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek() else {
                return false;
            };

            match item.compare_key(needle, self.bytes) {
                std::cmp::Ordering::Equal => {
                    return true;
                }
                std::cmp::Ordering::Greater => {
                    return false;
                }
                std::cmp::Ordering::Less => {
                    // Continue

                    self.decoder.next().expect("should exist");
                }
            }
        }
    }

    // TODO: the peek_back() + next_back() pattern is a bit unfortunate
    // TODO: maybe just seek the decoder, and then let the caller handle the linear search...
    pub fn seek_upper(&mut self, needle: &[u8]) -> bool {
        if !self
            .decoder
            .inner_mut()
            .seek_upper(|head_key| head_key <= needle, false)
        {
            return false;
        }

        // Linear scan
        loop {
            let Some(item) = self.decoder.peek_back() else {
                return false;
            };

            match item.compare_key(needle, self.bytes) {
                std::cmp::Ordering::Equal => {
                    return true;
                }
                std::cmp::Ordering::Less => {
                    return false;
                }
                std::cmp::Ordering::Greater => {
                    // Continue

                    self.decoder.next_back().expect("should exist");
                }
            }
        }
    }
}

impl Iterator for Iter<'_> {
    type Item = DataBlockParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.decoder.next_back()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        segment::{
            block::{BlockType, Header, ParsedItem},
            Block, BlockOffset, DataBlock,
        },
        Checksum, InternalValue, Slice,
        ValueType::{Tombstone, Value},
    };
    use test_log::test;

    #[test]
    fn v3_data_block_wtf() -> crate::Result<()> {
        let keys = [
            [0, 0, 0, 0, 0, 0, 0, 108],
            [0, 0, 0, 0, 0, 0, 0, 109],
            [0, 0, 0, 0, 0, 0, 0, 110],
            [0, 0, 0, 0, 0, 0, 0, 111],
            [0, 0, 0, 0, 0, 0, 0, 112],
            [0, 0, 0, 0, 0, 0, 0, 113],
            [0, 0, 0, 0, 0, 0, 0, 114],
            [0, 0, 0, 0, 0, 0, 0, 115],
            [0, 0, 0, 0, 0, 0, 0, 116],
            [0, 0, 0, 0, 0, 0, 0, 117],
            [0, 0, 0, 0, 0, 0, 0, 118],
            [0, 0, 0, 0, 0, 0, 0, 119],
            [0, 0, 0, 0, 0, 0, 0, 120],
            [0, 0, 0, 0, 0, 0, 0, 121],
            [0, 0, 0, 0, 0, 0, 0, 122],
            [0, 0, 0, 0, 0, 0, 0, 123],
            [0, 0, 0, 0, 0, 0, 0, 124],
            [0, 0, 0, 0, 0, 0, 0, 125],
            [0, 0, 0, 0, 0, 0, 0, 126],
            [0, 0, 0, 0, 0, 0, 0, 127],
            [0, 0, 0, 0, 0, 0, 0, 128],
            [0, 0, 0, 0, 0, 0, 0, 129],
            [0, 0, 0, 0, 0, 0, 0, 130],
            [0, 0, 0, 0, 0, 0, 0, 131],
            [0, 0, 0, 0, 0, 0, 0, 132],
            [0, 0, 0, 0, 0, 0, 0, 133],
            [0, 0, 0, 0, 0, 0, 0, 134],
            [0, 0, 0, 0, 0, 0, 0, 135],
            [0, 0, 0, 0, 0, 0, 0, 136],
            [0, 0, 0, 0, 0, 0, 0, 137],
            [0, 0, 0, 0, 0, 0, 0, 138],
            [0, 0, 0, 0, 0, 0, 0, 139],
            [0, 0, 0, 0, 0, 0, 0, 140],
            [0, 0, 0, 0, 0, 0, 0, 141],
            [0, 0, 0, 0, 0, 0, 0, 142],
            [0, 0, 0, 0, 0, 0, 0, 143],
        ];

        let items = keys
            .into_iter()
            .map(|key| InternalValue::from_components(key, "", 0, Value))
            .collect::<Vec<_>>();

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            {
                let mut iter = data_block.iter();
                iter.seek(&10u64.to_be_bytes());
                iter.seek_upper(&110u64.to_be_bytes());
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(3).cloned().collect::<Vec<_>>(),
                    iter.collect::<Vec<_>>(),
                );
            }

            {
                let mut iter: crate::segment::data_block::Iter<'_> = data_block.iter();
                iter.seek(&10u64.to_be_bytes());
                iter.seek_upper(&110u64.to_be_bytes());
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(3).rev().cloned().collect::<Vec<_>>(),
                    iter.rev().collect::<Vec<_>>(),
                );
            }

            {
                let mut iter = data_block.iter();
                iter.seek(&10u64.to_be_bytes());
                iter.seek_upper(&110u64.to_be_bytes());

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));
                let mut count = 0;

                for x in 0.. {
                    if x % 2 == 0 {
                        let Some(_) = iter.next() else {
                            break;
                        };

                        count += 1;
                    } else {
                        let Some(_) = iter.next_back() else {
                            break;
                        };

                        count += 1;
                    }
                }

                assert_eq!(3, count);
            }
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_range() -> crate::Result<()> {
        let items = (100u64..110)
            .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, Value))
            .collect::<Vec<_>>();

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            {
                let mut iter = data_block.iter();
                iter.seek(&10u64.to_be_bytes());
                iter.seek_upper(&109u64.to_be_bytes());
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(10).cloned().collect::<Vec<_>>(),
                    iter.collect::<Vec<_>>(),
                );
            }

            {
                let mut iter: crate::segment::data_block::Iter<'_> = data_block.iter();
                iter.seek(&10u64.to_be_bytes());
                iter.seek_upper(&109u64.to_be_bytes());
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(10).rev().cloned().collect::<Vec<_>>(),
                    iter.rev().collect::<Vec<_>>(),
                );
            }

            {
                let mut iter = data_block.iter();
                iter.seek(&10u64.to_be_bytes());
                iter.seek_upper(&109u64.to_be_bytes());

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));
                let mut count = 0;

                for x in 0.. {
                    if x % 2 == 0 {
                        let Some(_) = iter.next() else {
                            break;
                        };

                        count += 1;
                    } else {
                        let Some(_) = iter.next_back() else {
                            break;
                        };

                        count += 1;
                    }
                }

                assert_eq!(10, count);
            }
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_range_ping_pong() -> crate::Result<()> {
        let items = (0u64..100)
            .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, Value))
            .collect::<Vec<_>>();

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();
            iter.seek(&5u64.to_be_bytes());
            iter.seek_upper(&9u64.to_be_bytes());

            let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));
            let mut count = 0;

            for x in 0.. {
                if x % 2 == 0 {
                    let Some(_) = iter.next() else {
                        break;
                    };

                    count += 1;
                } else {
                    let Some(_) = iter.next_back() else {
                        break;
                    };

                    count += 1;
                }
            }

            assert_eq!(5, count);
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let iter = data_block
                .iter()
                .map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_rev() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let iter = data_block
                .iter()
                .rev()
                .map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().rev().cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_rev_seek_back() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(iter.seek_upper(b"d"), "should seek");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.rev().collect();

            assert_eq!(
                items.iter().rev().skip(2).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_range_edges() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            {
                let mut iter = data_block.iter();

                assert!(!iter.seek(b"a"), "should not seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(items.to_vec(), &*real_items);
            }

            {
                let mut iter = data_block.iter();

                assert!(!iter.seek_upper(b"g"), "should not seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(items.to_vec(), &*real_items);
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek_upper(b"b"), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek(b"f"), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().rev().take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_range() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(iter.seek(b"c"), "should seek");
            assert!(iter.seek_upper(b"d"), "should seek");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().skip(1).take(2).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_only_first() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(iter.seek_upper(b"b"), "should seek");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().take(1).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_range_same_key() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            {
                let mut iter = data_block.iter();

                assert!(iter.seek(b"d"), "should seek");
                assert!(iter.seek_upper(b"d"), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek_upper(b"d"), "should seek");
                assert!(iter.seek(b"d"), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek(b"d"), "should seek");
                assert!(iter.seek_upper(b"d"), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.rev().collect();

                assert_eq!(
                    items
                        .iter()
                        .rev()
                        .skip(2)
                        .take(1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek_upper(b"d"), "should seek");
                assert!(iter.seek(b"d"), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.rev().collect();

                assert_eq!(
                    items
                        .iter()
                        .rev()
                        .skip(2)
                        .take(1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    &*real_items,
                );
            }
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_range_empty() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            {
                let mut iter = data_block.iter();

                assert!(iter.seek(b"f"), "should seek");
                iter.seek_upper(b"e");

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek(b"f"), "should seek");
                iter.seek_upper(b"e");

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek_upper(b"e"), "should seek");
                iter.seek(b"f");

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter();

                assert!(iter.seek_upper(b"e"), "should seek");
                iter.seek(b"f");

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_seek_restart_head() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(iter.seek(b"b"), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_seek_in_interval() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(iter.seek(b"d"), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().skip(2).cloned().collect::<Vec<_>>(),
                real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_seek_last() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(iter.seek(b"f"), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(
                items.iter().skip(4).cloned().collect::<Vec<_>>(),
                real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_seek_before_first() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(!iter.seek(b"a"), "should not find exact match");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn v3_data_block_iter_forward_seek_after_last() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 1, Tombstone),
            InternalValue::from_components("e", "e", 0, Value),
            InternalValue::from_components("f", "f", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 1.33)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            let mut iter = data_block.iter();

            assert!(!iter.seek(b"g"), "should not find exact match");

            assert!(iter.next().is_none(), "should not collect any items");
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_iter_consume_last_back() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"pla:earth:fact", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"pla:jupiter:fact", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"pla:jupiter:mass", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"pla:jupiter:name", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"pla:jupiter:radius", &*iter.next().unwrap().key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter()
                    .map(|item| item.materialize(&data_block.inner.data));

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
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_iter_consume_last_forwards() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("pla:earth:fact", "eaaaaaaaaarth", 0, Value),
            InternalValue::from_components("pla:jupiter:fact", "Jupiter is big", 0, Value),
            InternalValue::from_components("pla:jupiter:mass", "Massive", 0, Value),
            InternalValue::from_components("pla:jupiter:name", "Jupiter", 0, Value),
            InternalValue::from_components("pla:jupiter:radius", "Big", 0, Value),
        ];

        for restart_interval in 1..=16 {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter()
                    .rev()
                    .map(|item| item.materialize(&data_block.inner.data));

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
                let mut iter = data_block
                    .iter()
                    .rev()
                    .map(|item| item.materialize(&data_block.inner.data));

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
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_data_block_iter_ping_pong_exhaust() -> crate::Result<()> {
        let items = [
            InternalValue::from_components("a", "a", 0, Value),
            InternalValue::from_components("b", "b", 0, Value),
            InternalValue::from_components("c", "c", 0, Value),
            InternalValue::from_components("d", "d", 0, Value),
            InternalValue::from_components("e", "e", 0, Value),
        ];

        for restart_interval in 1..=u8::MAX {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;

            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                    previous_block_offset: BlockOffset(0),
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"a", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"b", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"c", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"d", &*iter.next().unwrap().key.user_key);
                assert_eq!(b"e", &*iter.next().unwrap().key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"e", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"d", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"c", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"b", &*iter.next_back().unwrap().key.user_key);
                assert_eq!(b"a", &*iter.next_back().unwrap().key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }

            {
                let mut iter = data_block
                    .iter()
                    .map(|item| item.materialize(&data_block.inner.data));

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
                let mut iter = data_block
                    .iter()
                    .map(|item| item.materialize(&data_block.inner.data));

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

    #[test]
    fn v3_data_block_iter_fuzz_3() -> crate::Result<()> {
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

        let bytes = DataBlock::encode_into_vec(&items, 5, 1.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

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
    fn v3_data_block_iter_fuzz_4() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(
                Slice::new(&[0]),
                Slice::empty(),
                3_834_029_160_418_063_669,
                Value,
            ),
            InternalValue::from_components(Slice::new(&[0]), Slice::new(&[]), 127, Tombstone),
            InternalValue::from_components(
                Slice::new(&[53, 53, 53]),
                Slice::empty(),
                18_446_744_073_709_551_615,
                Tombstone,
            ),
            InternalValue::from_components(
                Slice::new(&[255]),
                Slice::empty(),
                18_446_744_069_414_584_831,
                Tombstone,
            ),
            InternalValue::from_components(Slice::new(&[255, 255]), Slice::empty(), 47, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 2, 1.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert_eq!(
            {
                #[allow(clippy::suspicious_map)]
                data_block.iter().count()
            },
            items.len(),
        );

        Ok(())
    }
}
