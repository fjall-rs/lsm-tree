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

    let mut writer = crate::table::Writer::new(file.clone(), 0, 0)
        .unwrap()
        .use_partitioned_filter()
        .use_data_block_size(1)
        .use_meta_partition_size(1);

    for item in items.iter().cloned() {
        writer.write(item).unwrap();
    }

    let _trailer = writer.finish().unwrap();

    let table = crate::Table::recover(
        file,
        crate::Checksum::from_raw(0),
        7,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(0)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        #[cfg(feature = "metrics")]
        Default::default(),
    )
    .unwrap();

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

/// Exercises the partition spill inside `PartitionedFilterWriter::register_bytes`.
/// With a prefix extractor, prefix hashes are registered via `register_bytes`
/// rather than `register_key`. Using a tiny partition size (1 byte) forces the
/// filter to spill after every prefix hash.
#[test]
#[expect(clippy::unwrap_used)]
fn table_partitioned_prefix_filter_spills_during_register_bytes() -> crate::Result<()> {
    use crate::prefix::FixedLengthExtractor;

    let dir = tempdir()?;
    let file = dir.path().join("table_partitioned_prefix_spill");
    let ex: crate::prefix::SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));

    let mut writer = Writer::new(file.clone(), 0, 0)?;
    writer = writer
        .use_bloom_policy(BloomConstructionPolicy::BitsPerKey(50.0))
        .use_prefix_extractor(Some(ex.clone()))
        .use_partitioned_filter()
        .use_meta_partition_size(1); // Force spills on every prefix hash

    for p in [b"aaa", b"bbb", b"ccc", b"ddd"] {
        for i in 0..20u32 {
            let mut k = p.to_vec();
            k.extend_from_slice(format!("{i:04}").as_bytes());
            writer.write(InternalValue::from_components(
                &k,
                &[],
                0,
                crate::ValueType::Value,
            ))?;
        }
    }
    let (_, checksum) = writer.finish()?.unwrap();

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::Metrics::default());

    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        #[cfg(feature = "metrics")]
        metrics,
    )?;

    // Verify the filter was built as a partitioned filter (has a top-level index)
    assert!(
        table.pinned_filter_index.is_some(),
        "expected partitioned filter with top-level index",
    );

    // Verify data is still readable through the prefix filter.
    // With a prefix extractor, the filter contains prefix hashes (not full-key
    // hashes), so we probe via maybe_contains_prefix rather than get().
    assert_eq!(
        Some(true),
        table.maybe_contains_prefix(b"aaa0000", ex.as_ref())?,
    );
    assert_eq!(
        Some(true),
        table.maybe_contains_prefix(b"ddd0019", ex.as_ref())?,
    );
    // Prefix "zzz" was never written — the filter should reject it
    assert_eq!(
        Some(false),
        table.maybe_contains_prefix(b"zzz0000", ex.as_ref())?,
    );

    // Also verify actual data reads bypass the filter successfully
    assert!(table.point_read(b"aaa0000", SeqNo::MAX)?.is_some());
    assert!(table.point_read(b"ddd0019", SeqNo::MAX)?.is_some());
    assert!(table.point_read(b"zzz0000", SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn table_should_skip_range_by_prefix_filter() -> crate::Result<()> {
    use crate::prefix::FixedLengthExtractor;
    use crate::range::prefix_upper_range;
    use std::ops::Bound;

    let dir = tempdir()?;
    let file = dir.path().join("table");
    let ex: crate::prefix::SharedPrefixExtractor = Arc::new(FixedLengthExtractor::new(3));

    // Write a table containing keys with prefixes "aaa" and "bbb" only
    let mut writer = Writer::new(file.clone(), 0, 0)?;
    writer = writer
        .use_bloom_policy(BloomConstructionPolicy::BitsPerKey(50.0))
        .use_prefix_extractor(Some(ex.clone()));

    for p in [b"aaa", b"bbb"] {
        for i in 0..20u32 {
            let mut k = p.to_vec();
            k.extend_from_slice(format!("{i:04}").as_bytes());
            writer.write(InternalValue::from_components(
                &k,
                &[],
                0,
                crate::ValueType::Value,
            ))?;
        }
    }
    let (_, checksum) = writer.finish()?.unwrap();

    #[cfg(feature = "metrics")]
    let metrics = Arc::new(crate::Metrics::default());

    let table = Table::recover(
        file,
        checksum,
        0,
        0,
        Arc::new(crate::Cache::with_capacity_bytes(1_000_000)),
        Some(Arc::new(crate::DescriptorTable::new(10))),
        true,
        true,
        #[cfg(feature = "metrics")]
        metrics,
    )?;

    // Absent prefix "zzz": filter should say skip
    let prefix = b"zzz00".to_vec();
    let start = Bound::Included(crate::UserKey::from(prefix.clone()));
    let end = prefix_upper_range(&prefix);
    assert!(
        table.should_skip_range_by_prefix_filter(&(start, end), ex.as_ref()),
        "should skip: table does not contain prefix zzz"
    );

    // Present prefix "aaa": filter should NOT say skip
    let prefix = b"aaa00".to_vec();
    let start = Bound::Included(crate::UserKey::from(prefix.clone()));
    let end = prefix_upper_range(&prefix);
    assert!(
        !table.should_skip_range_by_prefix_filter(&(start, end), ex.as_ref()),
        "should NOT skip: table contains prefix aaa"
    );

    // Incompatible extractor name: should not skip (conservative)
    let other_ex: crate::prefix::SharedPrefixExtractor =
        Arc::new(crate::prefix::FixedPrefixExtractor::new(3));
    let prefix = b"zzz00".to_vec();
    let start = Bound::Included(crate::UserKey::from(prefix.clone()));
    let end = prefix_upper_range(&prefix);
    assert!(
        !table.should_skip_range_by_prefix_filter(&(start, end), other_ex.as_ref()),
        "should NOT skip: extractor name mismatch"
    );

    Ok(())
}
