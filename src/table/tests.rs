use super::*;
use crate::table::filter::standard_bloom::Builder as BloomBuilder;
use tempfile::tempdir;
use test_log::test;

#[allow(
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
        let mut writer = Writer::new(file.clone(), 0)?;

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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
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

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert_eq!(0, table.pinned_block_index_size(), "should not pin index");
            // assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
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

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_none(), "should use full index");
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(table)?;
        }
    }

    std::fs::remove_file(&file)?;

    {
        let mut writer = Writer::new(file.clone(), 0)?.use_partitioned_index();

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
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert_eq!(0, table.pinned_filter_size(), "should not pin filter");

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            // assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file.clone(),
                checksum,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

            f(table)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let table = Table::recover(
                file,
                checksum,
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, table.id());
            assert_eq!(items.len(), table.metadata.item_count as usize);
            assert!(table.regions.index.is_some(), "should use two-level index",);
            assert!(table.pinned_block_index_size() > 0, "should pin index");
            // assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(table)?;
        }
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
#[allow(clippy::unwrap_used)]
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
