#[expect(clippy::expect_used)]
mod tests {
    use crate::comparator::default_comparator;
    use crate::{
        table::{
            block::{BlockType, Header, ParsedItem},
            Block, DataBlock,
        },
        Checksum, InternalValue, SeqNo, Slice,
        ValueType::{Tombstone, Value},
    };
    use test_log::test;

    #[test]
    fn data_block_wtf() -> crate::Result<()> {
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
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&110u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(3).cloned().collect::<Vec<_>>(),
                    iter.collect::<Vec<_>>(),
                );
            }

            {
                let mut iter: crate::table::data_block::Iter<'_> =
                    data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&110u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(3).rev().cloned().collect::<Vec<_>>(),
                    iter.rev().collect::<Vec<_>>(),
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&110u64.to_be_bytes(), SeqNo::MAX);

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
    fn data_block_range() -> crate::Result<()> {
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
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&109u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(10).cloned().collect::<Vec<_>>(),
                    iter.collect::<Vec<_>>(),
                );
            }

            {
                let mut iter: crate::table::data_block::Iter<'_> =
                    data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&109u64.to_be_bytes(), SeqNo::MAX);
                let iter = iter.map(|x| x.materialize(data_block.as_slice()));

                assert_eq!(
                    items.iter().take(10).rev().cloned().collect::<Vec<_>>(),
                    iter.rev().collect::<Vec<_>>(),
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());
                iter.seek(&10u64.to_be_bytes(), SeqNo::MAX);
                iter.seek_upper(&109u64.to_be_bytes(), SeqNo::MAX);

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
    fn data_block_range_ping_pong() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());
            iter.seek(&5u64.to_be_bytes(), SeqNo::MAX);
            iter.seek_upper(&9u64.to_be_bytes(), SeqNo::MAX);

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
    fn data_block_iter_forward() -> crate::Result<()> {
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
                },
            });

            let iter = data_block
                .iter(default_comparator())
                .map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_rev() -> crate::Result<()> {
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
                },
            });

            let iter = data_block
                .iter(default_comparator())
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
    fn data_block_iter_rev_seek_back() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

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
    fn data_block_iter_range_edges() -> crate::Result<()> {
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
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(!iter.seek(b"a", SeqNo::MAX), "should not seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(items.to_vec(), &*real_items);
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(!iter.seek_upper(b"g", SeqNo::MAX), "should not seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(items.to_vec(), &*real_items);
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"b", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"f", SeqNo::MAX), "should seek");

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
    fn data_block_iter_range() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"c", SeqNo::MAX), "should seek");
            assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

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
    fn data_block_iter_only_first() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek_upper(b"b", SeqNo::MAX), "should seek");

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
    fn data_block_iter_range_same_key() -> crate::Result<()> {
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
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");

                let iter = iter.map(|item| item.materialize(&data_block.inner.data));

                let real_items: Vec<_> = iter.collect();

                assert_eq!(
                    items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                    &*real_items,
                );
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");

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
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"d", SeqNo::MAX), "should seek");
                assert!(iter.seek(b"d", SeqNo::MAX), "should seek");

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
    fn data_block_iter_range_empty() -> crate::Result<()> {
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
                },
            });

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"f", SeqNo::MAX), "should seek");
                iter.seek_upper(b"e", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek(b"f", SeqNo::MAX), "should seek");
                iter.seek_upper(b"e", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"e", SeqNo::MAX), "should seek");
                iter.seek(b"f", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }

            {
                let mut iter = data_block.iter(default_comparator());

                assert!(iter.seek_upper(b"e", SeqNo::MAX), "should seek");
                iter.seek(b"f", SeqNo::MAX);

                let mut iter = iter.map(|item| item.materialize(&data_block.inner.data));

                assert!(iter.next_back().is_none(), "iter should be empty");
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_restart_head() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"b", SeqNo::MAX), "should seek correctly");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_in_interval() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"d", SeqNo::MAX), "should seek correctly");

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
    fn data_block_iter_forward_seek_last() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(iter.seek(b"f", SeqNo::MAX), "should seek correctly");

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
    fn data_block_iter_forward_seek_before_first() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(!iter.seek(b"a", SeqNo::MAX), "should not find exact match");

            let iter = iter.map(|item| item.materialize(&data_block.inner.data));

            let real_items: Vec<_> = iter.collect();

            assert_eq!(items, &*real_items);
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_forward_seek_after_last() -> crate::Result<()> {
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
                },
            });

            let mut iter = data_block.iter(default_comparator());

            assert!(!iter.seek(b"g", SeqNo::MAX), "should not find exact match");

            assert!(iter.next().is_none(), "should not collect any items");
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_consume_last_back() -> crate::Result<()> {
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
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_consume_last_forwards() -> crate::Result<()> {
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
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .rev()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .rev()
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(
                    b"pla:earth:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:fact",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:mass",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:name",
                    &*iter.next_back().expect("should exist").key.user_key,
                );
                assert_eq!(
                    b"pla:jupiter:radius",
                    &*iter.next().expect("should exist").key.user_key,
                );
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_ping_pong_exhaust() -> crate::Result<()> {
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
                },
            });

            assert_eq!(data_block.len(), items.len());
            assert!(data_block.hash_bucket_count().is_none());

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"a", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"e", &*iter.next().expect("should exist").key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"e", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"a", &*iter.next_back().expect("should exist").key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"a", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next().expect("should exist").key.user_key);
                assert_eq!(b"e", &*iter.next().expect("should exist").key.user_key);
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
            }

            {
                let mut iter = data_block
                    .iter(default_comparator())
                    .map(|item| item.materialize(&data_block.inner.data));

                assert_eq!(b"e", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"d", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"c", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"b", &*iter.next_back().expect("should exist").key.user_key);
                assert_eq!(b"a", &*iter.next_back().expect("should exist").key.user_key);
                assert!(iter.next().is_none());
                assert!(iter.next().is_none());
                assert!(iter.next_back().is_none());
                assert!(iter.next_back().is_none());
            }
        }

        Ok(())
    }

    #[test]
    fn data_block_iter_fuzz_3() -> crate::Result<()> {
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
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert_eq!(data_block.iter(default_comparator()).count(), items.len());

        Ok(())
    }

    #[test]
    fn data_block_iter_fuzz_4() -> crate::Result<()> {
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
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert!(
            data_block
                .hash_bucket_count()
                .expect("should have built hash index")
                > 0,
        );

        assert_eq!(data_block.iter(default_comparator()).count(), items.len());

        Ok(())
    }

    #[test]
    fn data_block_seek_closed_range() -> crate::Result<()> {
        let items = [
            InternalValue::from_components(Slice::new(&[0, 161]), Slice::empty(), 1, Tombstone),
            InternalValue::from_components(Slice::new(&[0, 161]), Slice::empty(), 0, Tombstone),
            InternalValue::from_components(Slice::new(&[1]), Slice::empty(), 0, Value),
        ];

        let bytes = DataBlock::encode_into_vec(&items, 100, 0.0)?;

        let data_block = DataBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Data,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        assert_eq!(data_block.len(), items.len());
        assert_eq!(data_block.iter(default_comparator()).count(), items.len());

        let mut iter = data_block.iter(default_comparator());
        iter.seek(&[0], SeqNo::MAX);
        iter.seek_upper(&[0], SeqNo::MAX);

        assert_eq!(0, iter.count());

        Ok(())
    }

    /// Verifies that `seek(needle, seqno)` with a seqno-aware predicate still
    /// positions the iterator correctly when a key has many versions spanning
    /// multiple restart intervals.
    #[test]
    fn data_block_seek_seqno_aware() -> crate::Result<()> {
        // Build a block where key "b" has 10 versions (seqno 10..1) with
        // restart_interval=2, so versions span 5 restart intervals.
        let mut items = Vec::new();
        for seqno in (1..=10).rev() {
            items.push(InternalValue::from_components(b"b", b"", seqno, Value));
        }

        for restart_interval in [1, 2, 3, 5] {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            // With SeqNo::MAX, seek behaves like key-only (no seqno filtering).
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(
                    iter.seek(b"b", SeqNo::MAX),
                    "should find key with MAX seqno"
                );
                let entry = iter.next().expect("should have entry");
                let materialized = entry.materialize(&data_block.inner.data);
                assert_eq!(materialized.key.user_key.as_ref(), b"b");
                // First version returned is the newest (seqno 10).
                assert_eq!(materialized.key.seqno, 10);
            }

            // With a specific snapshot seqno, the binary search lands on the
            // restart interval containing (or nearest to) the target seqno.
            // The first entry returned is the head of that interval.
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(iter.seek(b"b", 5), "should find key with snapshot seqno 5");
                let entry = iter.next().expect("should have entry");
                let materialized = entry.materialize(&data_block.inner.data);
                assert_eq!(materialized.key.user_key.as_ref(), b"b");
                // The landing entry's seqno must be >= the snapshot boundary,
                // proving the seqno-aware predicate skipped past older intervals.
                assert!(
                    materialized.key.seqno >= 5,
                    "restart_interval={restart_interval}: landing seqno {} should be >= snapshot 5",
                    materialized.key.seqno,
                );
                // With restart_interval=1 each entry is its own interval, so
                // the predicate lands exactly on the target seqno — a key-only
                // seek would land on seqno 10 instead.
                if restart_interval == 1 {
                    assert_eq!(
                        materialized.key.seqno, 5,
                        "with restart_interval=1, seqno-aware seek must land exactly on target"
                    );
                }
            }
        }

        Ok(())
    }

    /// Verifies that `seek` with seqno still works correctly when the block
    /// contains multiple distinct keys with versions.
    #[test]
    fn data_block_seek_seqno_aware_mixed_keys() -> crate::Result<()> {
        let items = vec![
            InternalValue::from_components(b"a", b"", 10, Value),
            InternalValue::from_components(b"a", b"", 5, Value),
            InternalValue::from_components(b"b", b"", 10, Value),
            InternalValue::from_components(b"b", b"", 7, Value),
            InternalValue::from_components(b"b", b"", 3, Value),
            InternalValue::from_components(b"c", b"", 10, Value),
        ];

        for restart_interval in [1, 2, 3] {
            let bytes = DataBlock::encode_into_vec(&items, restart_interval, 0.0)?;
            let data_block = DataBlock::new(Block {
                data: bytes.into(),
                header: Header {
                    block_type: BlockType::Data,
                    checksum: Checksum::from_raw(0),
                    data_length: 0,
                    uncompressed_length: 0,
                },
            });

            // Forward seek with seqno narrows restart interval selection.
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(iter.seek(b"b", 5), "should find b at snapshot 5");
                let entry = iter.next().expect("should have entry");
                let mat = entry.materialize(&data_block.inner.data);
                assert_eq!(mat.key.user_key.as_ref(), b"b");
                // Landing seqno must be >= snapshot boundary.
                assert!(
                    mat.key.seqno >= 5,
                    "restart_interval={restart_interval}: seqno {} should be >= 5",
                    mat.key.seqno,
                );
                // With restart_interval=1, seqno-aware seek lands on (b,7) —
                // the last head with seqno >= 5 — whereas key-only would land
                // on (b,10).
                if restart_interval == 1 {
                    assert_eq!(mat.key.seqno, 7);
                }
            }

            // Exclusive forward seek with seqno.
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(
                    iter.seek_exclusive(b"b", 5),
                    "should find entry > b at snapshot 5"
                );
                let entry = iter.next().expect("should have entry");
                let mat = entry.materialize(&data_block.inner.data);
                assert_eq!(mat.key.user_key.as_ref(), b"c");
            }

            // Upper seek still works with seqno (predicate unchanged for backward).
            {
                let mut iter = data_block.iter(default_comparator());
                assert!(iter.seek_upper(b"b", 5), "should find upper bound b");
                let entry = iter.next_back().expect("should have entry");
                let mat = entry.materialize(&data_block.inner.data);
                assert_eq!(mat.key.user_key.as_ref(), b"b");
            }
        }

        Ok(())
    }
}
