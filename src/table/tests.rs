// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::*;
use crate::{
    config::BloomConstructionPolicy, table::filter::standard_bloom::Builder as BloomBuilder,
};
use tempfile::tempdir;
use test_log::test;

#[expect(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation,
    clippy::unwrap_used
)]
fn test_with_table(
    items: &[InternalValue],
    f: impl Fn(Table) -> crate::Result<()>,
    rotate_every: Option<usize>,
    config_writer: Option<impl Fn(Writer) -> Writer>,
) -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = Writer::new(file.clone(), 0, 0)?;

        if let Some(f) = &config_writer {
            writer = f(writer);
        }

        for (idx, item) in items.iter().enumerate() {
            if let Some(rotate) = rotate_every {
                if idx % rotate == 0 {
                    writer.spill_block()?;
                }
            }
            writer.write(item.clone())?;
        }
        let (_, checksum) = writer.finish()?.unwrap();

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                false,
                false,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert_eq!(0, table.pinned_block_index_size(), "should not pin index");
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                true,
                false,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert_eq!(0, table.pinned_block_index_size(), "should not pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                false,
                true,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                true,
                true,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                None,
                true,
                true,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(table.file_accessor, FileAccessor::File(..)));

            f(table)?;
        }
    }

    std::fs::remove_file(&file)?;

    // Test with partitioned indexes
    {
        let mut writer = Writer::new(file.clone(), 0, 0)?.use_partitioned_index();

        if let Some(f) = config_writer {
            writer = f(writer);
        }

        for (idx, item) in items.iter().enumerate() {
            if let Some(rotate) = rotate_every {
                if idx % rotate == 0 {
                    writer.spill_block()?;
                }
            }
            writer.write(item.clone())?;
        }
        let (_, checksum) = writer.finish()?.unwrap();

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                false,
                false,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                true,
                false,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                false,
                true,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert_eq!(0, table.pinned_filter_size(), "should not pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Some(Arc::new(DescriptorTable::new(10))),
                true,
                true,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(
                table.file_accessor,
                FileAccessor::DescriptorTable(..)
            ));

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file,
                checksum,
                0,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                None,
                true,
                true,
                None,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(table.pinned_filter_size() > 0, "should pin filter");
            assert!(matches!(table.file_accessor, FileAccessor::File(..)));

            f(table)?;
        }
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_point_read() -> crate::Result<()> {
    let items = [crate::InternalValue::from_components(
        b"abc",
        b"asdasdasd",
        3,
        crate::ValueType::Value,
    )];

    test_with_table(
        &items,
        |table| {
            assert_eq!(
                b"abc",
                &*table
                    .get(b"abc", SeqNo::MAX, BloomBuilder::get_hash(b"abc"))?
                    .unwrap()
                    .key
                    .user_key,
            );
            assert_eq!(
                None,
                table.get(b"def", SeqNo::MAX, BloomBuilder::get_hash(b"def"))?,
            );
            assert_eq!(
                None,
                table.get(b"____", SeqNo::MAX, BloomBuilder::get_hash(b"____"))?,
            );

            assert_eq!(
                table.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"abc".into())),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_exclusive_bounds() -> crate::Result<()> {
    use std::ops::Bound::{Excluded, Included};

    let items = [
        crate::InternalValue::from_components(b"a", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"v", 0, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"v", 0, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            let res = table
                .range((Excluded(UserKey::from("b")), Included(UserKey::from("d"))))
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items.iter().skip(2).take(2).cloned().collect::<Vec<_>>(),
                &*res,
            );

            let res = table
                .range((Excluded(UserKey::from("b")), Included(UserKey::from("d"))))
                .rev()
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items
                    .iter()
                    .skip(2)
                    .take(2)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*res,
            );

            let res = table
                .range((Excluded(UserKey::from("b")), Excluded(UserKey::from("d"))))
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items.iter().skip(2).take(1).cloned().collect::<Vec<_>>(),
                &*res,
            );

            let res = table
                .range((Excluded(UserKey::from("b")), Excluded(UserKey::from("d"))))
                .rev()
                .flatten()
                .collect::<Vec<_>>();
            assert_eq!(
                items
                    .iter()
                    .skip(2)
                    .take(1)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*res,
            );

            Ok(())
        },
        None,
        Some(|x: Writer| x.use_data_block_size(1)),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_point_read_mvcc_block_boundary() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"5", 5, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"4", 4, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"3", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"2", 2, crate::ValueType::Value),
        crate::InternalValue::from_components(b"a", b"1", 1, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(2, table.metadata.data_block_count);

            let key_hash = BloomBuilder::get_hash(b"a");

            assert_eq!(
                b"5",
                &*table.get(b"a", SeqNo::MAX, key_hash)?.unwrap().value
            );
            assert_eq!(b"4", &*table.get(b"a", 5, key_hash)?.unwrap().value);
            assert_eq!(b"3", &*table.get(b"a", 4, key_hash)?.unwrap().value);
            assert_eq!(b"2", &*table.get(b"a", 3, key_hash)?.unwrap().value);
            assert_eq!(b"1", &*table.get(b"a", 2, key_hash)?.unwrap().value);

            Ok(())
        },
        Some(3),
        Some(|x| x),
    )
}

#[test]
fn table_scan() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(items, &*table.scan()?.flatten().collect::<Vec<_>>());

            assert_eq!(
                table.metadata.key_range,
                crate::KeyRange::new((b"abc".into(), b"xyz".into())),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_iter_simple() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(items, &*table.iter().flatten().collect::<Vec<_>>());
            assert_eq!(
                items.iter().rev().cloned().collect::<Vec<_>>(),
                &*table.iter().rev().flatten().collect::<Vec<_>>(),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_simple() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(
                items.iter().skip(1).cloned().collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..)
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..)
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_ping_pong() -> crate::Result<()> {
    let items = (0u64..10)
        .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, crate::ValueType::Value))
        .collect::<Vec<_>>();

    test_with_table(
        &items,
        |table| {
            let mut iter =
                table.range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()));

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

            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_range_multiple_data_blocks() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(5, table.metadata.data_block_count);

            assert_eq!(
                items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .flatten()
                    .collect::<Vec<_>>()
            );

            assert_eq!(
                items
                    .iter()
                    .skip(1)
                    .take(3)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*table
                    .range(UserKey::from("b")..=UserKey::from("d"))
                    .rev()
                    .flatten()
                    .collect::<Vec<_>>(),
            );

            Ok(())
        },
        None,
        Some(|x: Writer| x.use_data_block_size(1)),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_point_read_partitioned_filter_smoke_test() -> crate::Result<()> {
    let items = [
        crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(1, table.metadata.data_block_count);

            for item in &items {
                let key_hash = BloomBuilder::get_hash(&item.key.user_key);

                assert_eq!(
                    item.value,
                    table
                        .get(&item.key.user_key, SeqNo::MAX, key_hash)
                        .unwrap()
                        .unwrap()
                        .value,
                );
            }

            Ok(())
        },
        None,
        Some(|x: Writer| x.use_partitioned_filter()),
    )
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_partitioned_filter() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", "a7", 7, Value),
        InternalValue::from_components("a", "a6", 6, Value),
        InternalValue::from_components("a", "a5", 5, Value),
        InternalValue::from_components("a", "a4", 4, Value),
        InternalValue::from_components("a", "a3", 3, Value),
        InternalValue::from_components("b", "b5", 5, Value),
        InternalValue::from_components("c", "c8", 8, Value),
        InternalValue::from_components("d", "d10", 10, Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert!(table.regions.filter.is_some(), "filter should exist");
            assert!(
                table.regions.filter_tli.is_some(),
                "filter TLI should exist"
            );

            assert_eq!(
                b"a7",
                &*table
                    .get(b"a", 8, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a6",
                &*table
                    .get(b"a", 7, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a5",
                &*table
                    .get(b"a", 6, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a4",
                &*table
                    .get(b"a", 5, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"a3",
                &*table
                    .get(b"a", 4, BloomBuilder::get_hash(b"a"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"b5",
                &*table
                    .get(b"b", 6, BloomBuilder::get_hash(b"b"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"c8",
                &*table
                    .get(b"c", 9, BloomBuilder::get_hash(b"c"))?
                    .unwrap()
                    .value,
            );
            assert_eq!(
                b"d10",
                &*table
                    .get(b"d", 11, BloomBuilder::get_hash(b"d"))?
                    .unwrap()
                    .value,
            );
            Ok(())
        },
        None,
        Some(|x: Writer| x.use_partitioned_filter().use_meta_partition_size(3)),
    )
}

#[test]
fn table_seqnos() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", nanoid::nanoid!().as_bytes(), 7, Value),
        InternalValue::from_components("b", nanoid::nanoid!().as_bytes(), 5, Value),
        InternalValue::from_components("c", nanoid::nanoid!().as_bytes(), 8, Value),
        InternalValue::from_components("d", nanoid::nanoid!().as_bytes(), 10, Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert_eq!(5, table.metadata.seqnos.0);
            assert_eq!(10, table.metadata.seqnos.1);
            Ok(())
        },
        None,
        Some(|x| x),
    )
}

#[test]
fn table_zero_bpk() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", nanoid::nanoid!().as_bytes(), 7, Value),
        InternalValue::from_components("b", nanoid::nanoid!().as_bytes(), 5, Value),
        InternalValue::from_components("c", nanoid::nanoid!().as_bytes(), 8, Value),
        InternalValue::from_components("d", nanoid::nanoid!().as_bytes(), 10, Value),
    ];

    test_with_table(
        &items,
        |table| {
            assert!(table.regions.filter.is_none());
            Ok(())
        },
        None,
        Some(|x: Writer| x.use_bloom_policy(BloomConstructionPolicy::BitsPerKey(0.0))),
    )
}

#[test]
#[expect(
    clippy::unreadable_literal,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation
)]
#[cfg(not(feature = "metrics"))]
fn table_read_fuzz_1() -> crate::Result<()> {
    use crate::Slice;
    use crate::ValueType::{Tombstone, Value};

    let items = [
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            18340908174618760209,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            18054235897395861447,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([103]),
            17820711698989577060,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            17652351990810576660,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            17576667967203573449,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([30]),
            16889403751796995588,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([186]),
            15595956295177086731,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            15512796775024989213,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([188, 156, 59, 85, 13]),
            15149465603839159843,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([174, 71]),
            15102256701513339307,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([35, 148]),
            15091160407760527013,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14675333203365509622,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([245]),
            14571905818510788533,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14541113699969547298,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14486387191240337417,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            14112006182482717758,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([159]),
            13992512869528291746,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            13915106262991388976,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            13597506620670366065,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            13064400463180401957,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            12969967266897711474,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            12508372658468564628,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([138]),
            11795269606598686255,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([18]),
            10730214428751858128,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([236]),
            10124645034840293700,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([216, 81]),
            9559308046784608794,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([79]),
            8607115510826103394,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7963767336149785641,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7882646634183551394,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7719307175583565930,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([111]),
            7522791039398476411,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([227, 164, 129]),
            7410771579448817672,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            7003757491682295965,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5723101273557106371,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5581364419922287132,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([119, 29]),
            5541782075650463683,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5136199042703471864,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            5051972816573966850,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([162]),
            5020119417385108821,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([69]),
            4325966282181409009,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            4238714774310338082,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            4200824275757201410,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([92, 145, 251, 240, 133]),
            3894954012280195585,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([14]),
            3814525464013269105,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            3766663710061910506,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([129]),
            3749655073597306832,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([231]),
            3319226033273656005,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            3274394613296787928,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            2045761581956846404,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([78]),
            1704041985603476880,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([]),
            1441130125005023946,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([164, 136]),
            1225420702887300153,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([55]),
            974698856173325051,
            Value,
        ),
        InternalValue::from_components(
            Slice::from([0]),
            Slice::from([238, 237]),
            47340610649818236,
            Value,
        ),
        InternalValue::from_components(Slice::from([0]), Slice::from([]), 0, Value),
        InternalValue::from_components(
            Slice::from([0, 161]),
            Slice::from([]),
            17872519117933825384,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([0, 161]),
            Slice::from([]),
            4494664966150999400,
            Tombstone,
        ),
        InternalValue::from_components(
            Slice::from([1]),
            Slice::from([]),
            15373275907316083975,
            Value,
        ),
    ];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let data_block_size = 97;

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0)
        .unwrap()
        .use_data_block_size(data_block_size);

    for item in items.iter().cloned() {
        writer.write(item).unwrap();
    }

    let _trailer = writer.finish().unwrap();

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        None,
    )
    .unwrap();

    let item_count_usize = table.metadata.item_count as usize;
    assert_eq!(item_count_usize, items.len());

    assert_eq!(items.len(), item_count_usize);
    let items = items.into_iter().collect::<Vec<_>>();

    assert_eq!(items, table.iter().collect::<Result<Vec<_>, _>>().unwrap());
    assert_eq!(
        items.iter().rev().cloned().collect::<Vec<_>>(),
        table.iter().rev().collect::<Result<Vec<_>, _>>().unwrap(),
    );

    {
        let lo = 0;
        let hi = 54;

        let lo_key = &items[lo].key.user_key;
        let hi_key = &items[hi].key.user_key;

        assert_eq!(lo_key, hi_key);

        let expected_range: Vec<_> = items[lo..=hi].to_vec();

        let iter = table.range(lo_key..=hi_key);

        assert_eq!(expected_range, iter.collect::<Result<Vec<_>, _>>().unwrap());
    }

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_partitioned_index() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a", "a7", 7, Value),
        InternalValue::from_components("a", "a6", 6, Value),
        InternalValue::from_components("a", "a5", 5, Value),
        InternalValue::from_components("a", "a4", 4, Value),
        InternalValue::from_components("a", "a3", 3, Value),
        InternalValue::from_components("b", "b5", 5, Value),
        InternalValue::from_components("c", "c8", 8, Value),
        InternalValue::from_components("d", "d10", 10, Value),
    ];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0)
        .unwrap()
        .use_partitioned_index()
        .use_data_block_size(5)
        .use_meta_partition_size(3);

    for item in items.iter().cloned() {
        writer.write(item).unwrap();
    }

    let _trailer = writer.finish().unwrap();

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        None,
        #[cfg(feature = "metrics")]
        Default::default(),
    )
    .unwrap();

    assert!(
        table.regions.index.is_some(),
        "2nd-level index should exist",
    );

    assert!(
        table.metadata.index_block_count > 1,
        "should use partitioned index",
    );

    assert_eq!(
        b"a7",
        &*table
            .get(b"a", 8, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a6",
        &*table
            .get(b"a", 7, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a5",
        &*table
            .get(b"a", 6, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a4",
        &*table
            .get(b"a", 5, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"a3",
        &*table
            .get(b"a", 4, BloomBuilder::get_hash(b"a"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"b5",
        &*table
            .get(b"b", 6, BloomBuilder::get_hash(b"b"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"c8",
        &*table
            .get(b"c", 9, BloomBuilder::get_hash(b"c"))?
            .unwrap()
            .value,
    );
    assert_eq!(
        b"d10",
        &*table
            .get(b"d", 11, BloomBuilder::get_hash(b"d"))?
            .unwrap()
            .value,
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_global_seqno() -> crate::Result<()> {
    use crate::ValueType::Value;

    let items = [
        InternalValue::from_components("a0", "a0", 0, Value),
        InternalValue::from_components("a1", "a1", 1, Value),
        InternalValue::from_components("b", "b", 8, Value),
    ];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0)?
        .use_partitioned_filter()
        .use_data_block_size(1)
        .use_meta_partition_size(1);

    for item in items {
        writer.write(item)?;
    }

    let _trailer = writer.finish()?;

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        7,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        None,
        #[cfg(feature = "metrics")]
        Default::default(),
    )?;

    // global seqno is 7, so a1 is = 8 -> can not be read by snapshot=8
    assert!(table
        .get(b"a1", 8, BloomBuilder::get_hash(b"a1"))?
        .is_none());

    assert_eq!(
        b"a0",
        &*table
            .get(b"a0", 8, BloomBuilder::get_hash(b"a0"))?
            .unwrap()
            .value,
    );

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_return_global_seqno() -> crate::Result<()> {
    use crate::ValueType::Value;

    const SEQNO: SeqNo = 15;

    let items = [InternalValue::from_components("abc", "abc", 0, Value)];

    let dir = tempfile::tempdir()?;
    let file = dir.path().join("table_fuzz");

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0)?;

    for item in items {
        writer.write(item)?;
    }

    let _trailer = writer.finish()?;

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        SEQNO,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        None,
        #[cfg(feature = "metrics")]
        Default::default(),
    )?;

    assert_eq!(
        InternalValue::from_components("abc", "abc", SEQNO, Value),
        table
            .get(b"abc", 2 * SEQNO, BloomBuilder::get_hash(b"abc"))?
            .unwrap(),
    );

    Ok(())
}

/// Writes `keys` (each at seqno 0) into a table with a 3-byte prefix filter
/// and recovers it with `configured` as the configured extractor.
#[expect(clippy::unwrap_used)]
fn prefix_table(
    dir: &std::path::Path,
    keys: &[&[u8]],
    written_with: crate::prefix::SharedPrefixExtractor,
    configured: Option<&crate::prefix::SharedPrefixExtractor>,
    whole_key_filtering: bool,
    partition_size: Option<u32>,
    global_seqno: SeqNo,
) -> crate::Result<Table> {
    let file = dir.join("table");

    let mut writer = Writer::new(file.clone(), 0, 0)?
        .use_bloom_policy(BloomConstructionPolicy::BitsPerKey(50.0))
        .use_prefix_extractor(Some(written_with))
        .use_whole_key_filtering(whole_key_filtering);

    if let Some(size) = partition_size {
        writer = writer
            .use_partitioned_filter()
            .use_data_block_size(1)
            .use_meta_partition_size(size);
    }

    for (seqno, key) in keys.iter().enumerate() {
        writer.write(InternalValue::from_components(
            *key,
            *key,
            seqno as SeqNo,
            crate::ValueType::Value,
        ))?;
    }

    let (_, checksum) = writer.finish()?.unwrap();

    Table::recover(
        file,
        checksum,
        global_seqno,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        configured,
        #[cfg(feature = "metrics")]
        Arc::default(),
    )
}

fn fixed3() -> crate::prefix::SharedPrefixExtractor {
    Arc::new(crate::prefix::FixedPrefixExtractor::new(3))
}

fn range_skipped(
    table: &Table,
    start: std::ops::Bound<&[u8]>,
    end: std::ops::Bound<&[u8]>,
) -> bool {
    #[expect(clippy::unwrap_used)]
    table
        .skips_range(&mut crate::prefix::RangeProbe::new(start, end))
        .unwrap()
}

/// A table without whole-key hashes must apply `global_seqno` when it reads
/// past its prefix filter, like the whole-key path does.
///
/// With `global_seqno = 7`, "aa1" (seqno 1) has logical seqno 8, so a read at
/// seqno 8 must not see it, while "aa0" (logical 7) is visible.
#[test]
#[expect(clippy::unwrap_used)]
fn table_prefix_only_get_applies_global_seqno() -> crate::Result<()> {
    let dir = tempdir()?;
    let table = prefix_table(
        dir.path(),
        &[b"aa0", b"aa1"],
        Arc::new(crate::prefix::FixedPrefixExtractor::new(2)),
        None,
        false,
        None,
        7,
    )?;

    let get = |key: &[u8], seqno| table.get(key, seqno, BloomBuilder::get_hash(key));

    assert!(get(b"aa1", 8)?.is_none());
    assert_eq!(b"aa0", &*get(b"aa0", 8)?.unwrap().value);
    assert_eq!(b"aa1", &*get(b"aa1", 9)?.unwrap().value);

    Ok(())
}

/// A partitioned prefix-only filter splits keys of one prefix across
/// partitions; each partition must still answer for its own keys.
#[test]
#[expect(clippy::unwrap_used)]
fn table_prefix_only_partitioned_filter_reads_every_key() -> crate::Result<()> {
    let dir = tempdir()?;

    let keys: Vec<Vec<u8>> = [b"aaa", b"bbb", b"ccc", b"ddd"]
        .iter()
        .flat_map(|p| (0..20u32).map(move |i| [&p[..], format!("{i:04}").as_bytes()].concat()))
        .collect();
    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

    let table = prefix_table(dir.path(), &keys, fixed3(), None, false, Some(1), 0)?;

    let partitions = table.pinned_filter_index.as_ref().unwrap().iter().count();
    assert!(
        partitions > 1,
        "expected several partitions, got {partitions}"
    );

    for key in &keys {
        assert!(
            table
                .get(key, SeqNo::MAX, BloomBuilder::get_hash(key))?
                .is_some(),
            "lost {key:?}",
        );
    }

    #[cfg(feature = "metrics")]
    let skipped_before = table.metrics.io_skipped_by_prefix_filter();

    assert!(table
        .get(b"zzz0000", SeqNo::MAX, BloomBuilder::get_hash(b"zzz0000"))?
        .is_none());

    #[cfg(feature = "metrics")]
    assert_eq!(
        skipped_before + 1,
        table.metrics.io_skipped_by_prefix_filter()
    );

    Ok(())
}

#[test]
fn table_skips_range_by_prefix_filter() -> crate::Result<()> {
    use std::ops::Bound::{Excluded, Included};

    let dir = tempdir()?;
    let table = prefix_table(
        dir.path(),
        &[b"aaa0", b"aaa1", b"bbb0"],
        fixed3(),
        None,
        true,
        None,
        0,
    )?;

    // prefix("zzz00") and prefix("zzz")
    assert!(range_skipped(
        &table,
        Included(b"zzz00"),
        Excluded(b"zzz01")
    ));
    assert!(range_skipped(&table, Included(b"zzz"), Excluded(b"zz{")));
    // A range between the table's prefixes
    assert!(range_skipped(&table, Included(b"mmm"), Excluded(b"mmn")));

    assert!(!range_skipped(&table, Included(b"aaa"), Excluded(b"aab")));
    assert!(!range_skipped(&table, Included(b"aaa1"), Included(b"aaa5")));

    // "aaa9999..=ccc0000" holds "bbb0" though "aaa9999" shares its prefix
    // with the table's first key
    assert!(!range_skipped(
        &table,
        Included(b"aaa9999"),
        Included(b"ccc0000")
    ));

    // Too short to have a prefix
    assert!(!range_skipped(&table, Included(b"zz"), Excluded(b"z{")));

    Ok(())
}

/// A table keeps the extractor that wrote it: a built-in one is rebuilt from
/// its name, and an unknown one disables the prefix filter.
#[test]
fn table_resolves_its_own_extractor() -> crate::Result<()> {
    use crate::prefix::PrefixExtractor;
    use std::ops::Bound::{Excluded, Included};

    struct Custom;

    impl PrefixExtractor for Custom {
        fn extract(&self, key: &[u8], emit: &mut dyn FnMut(usize)) {
            crate::prefix::FixedPrefixExtractor::new(3).extract(key, emit);
        }

        fn same_result_when_appended(&self, prefix: &[u8]) -> bool {
            prefix.len() == 3
        }

        fn name(&self) -> &'static str {
            "test::Custom"
        }
    }

    let configured: crate::prefix::SharedPrefixExtractor =
        Arc::new(crate::prefix::FixedPrefixExtractor::new(8));

    {
        let dir = tempdir()?;
        let table = prefix_table(
            dir.path(),
            &[b"aaa0"],
            fixed3(),
            Some(&configured),
            true,
            None,
            0,
        )?;
        assert!(range_skipped(&table, Included(b"zzz"), Excluded(b"zz{")));
    }

    {
        let dir = tempdir()?;
        let custom: crate::prefix::SharedPrefixExtractor = Arc::new(Custom);

        let table = prefix_table(
            dir.path(),
            &[b"aaa0"],
            custom.clone(),
            Some(&configured),
            true,
            None,
            0,
        )?;
        assert!(!range_skipped(&table, Included(b"zzz"), Excluded(b"zz{")));

        let table = prefix_table(
            &{
                let path = dir.path().join("again");
                std::fs::create_dir(&path)?;
                path
            },
            &[b"aaa0"],
            custom.clone(),
            Some(&custom),
            true,
            None,
            0,
        )?;
        assert!(range_skipped(&table, Included(b"zzz"), Excluded(b"zz{")));
    }

    Ok(())
}

/// Reads the version a table file carries in its `table_version` section and
/// in its meta block.
#[expect(clippy::unwrap_used)]
fn written_table_versions(table: &Table) -> crate::Result<(Vec<u8>, Vec<u8>)> {
    use std::io::{Read, Seek};

    let mut file = std::fs::File::open(&*table.path)?;
    let trailer = sfa::Reader::from_reader(&mut file)?;
    let section = trailer.toc().section(b"table_version").unwrap();

    let mut section_bytes = vec![0; usize::try_from(section.len()).unwrap()];
    file.seek(std::io::SeekFrom::Start(section.pos()))?;
    file.read_exact(&mut section_bytes)?;

    let block = crate::table::block::Block::from_file(
        &file,
        table.regions.metadata,
        crate::CompressionType::None,
    )?;
    let meta_bytes = DataBlock::new(block)
        .point_read(b"table_version", SeqNo::MAX)
        .unwrap()
        .value;

    Ok((section_bytes, meta_bytes.to_vec()))
}

/// A filter without whole-key hashes gets table version 4, which readers that
/// predate `whole_key_filtering` refuse to open instead of skipping the table
/// on every point read; every other table keeps version 3.
#[test]
fn table_version_marks_prefix_only_filter() -> crate::Result<()> {
    let dir = tempdir()?;
    let prefix_only = prefix_table(dir.path(), &[b"aaa0"], fixed3(), None, false, None, 0)?;
    assert_eq!((vec![4], vec![4]), written_table_versions(&prefix_only)?);

    let dir = tempdir()?;
    let prefix_and_key = prefix_table(dir.path(), &[b"aaa0"], fixed3(), None, true, None, 0)?;
    assert_eq!((vec![3], vec![3]), written_table_versions(&prefix_and_key)?);

    // Without an extractor the filter holds whole keys whatever the flag says
    let dir = tempdir()?;
    let file = dir.path().join("table");
    let mut writer = Writer::new(file.clone(), 0, 0)?.use_whole_key_filtering(false);
    writer.write(InternalValue::from_components(
        b"aaa0",
        b"aaa0",
        0,
        crate::ValueType::Value,
    ))?;
    #[expect(clippy::unwrap_used)]
    let (_, checksum) = writer.finish()?.unwrap();
    let key_only = Table::recover(
        file,
        checksum,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
        None,
        true,
        true,
        None,
        #[cfg(feature = "metrics")]
        Arc::default(),
    )?;
    assert_eq!((vec![3], vec![3]), written_table_versions(&key_only)?);

    Ok(())
}
