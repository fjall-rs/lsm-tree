use super::*;
use crate::segment::filter::standard_bloom::Builder as BloomBuilder;
use tempfile::tempdir;
use test_log::test;

#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::cast_possible_truncation
)]
fn test_with_table(
    items: &[InternalValue],
    f: impl Fn(Segment) -> crate::Result<()>,
    rotate_every: Option<usize>,
    data_block_size: Option<u32>,
) -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 0)?;

        if let Some(data_block_size) = data_block_size {
            writer = writer.use_data_block_size(data_block_size);
        }

        for (idx, item) in items.iter().enumerate() {
            if let Some(rotate) = rotate_every {
                if idx % rotate == 0 {
                    writer.spill_block()?;
                }
            }
            writer.write(item.clone())?;
        }
        let _trailer = writer.finish()?;

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(segment.regions.index.is_none(), "should use full index");
            assert_eq!(0, segment.pinned_block_index_size(), "should not pin index");
            assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

            f(segment)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(segment.regions.index.is_none(), "should use full index");
            assert_eq!(0, segment.pinned_block_index_size(), "should not pin index");
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(segment)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(segment.regions.index.is_none(), "should use full index");
            assert!(segment.pinned_block_index_size() > 0, "should pin index");
            assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

            f(segment)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(segment.regions.index.is_none(), "should use full index");
            assert!(segment.pinned_block_index_size() > 0, "should pin index");
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(segment)?;
        }
    }

    std::fs::remove_file(&file)?;

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 0)?.use_partitioned_index();

        if let Some(data_block_size) = data_block_size {
            writer = writer.use_data_block_size(data_block_size);
        }

        for (idx, item) in items.iter().enumerate() {
            if let Some(rotate) = rotate_every {
                if idx % rotate == 0 {
                    writer.spill_block()?;
                }
            }
            writer.write(item.clone())?;
        }
        let _trailer = writer.finish()?;

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(
                segment.regions.index.is_some(),
                "should use two-level index",
            );
            assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

            f(segment)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                false,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(
                segment.regions.index.is_some(),
                "should use two-level index",
            );
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(segment)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                false,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(
                segment.regions.index.is_some(),
                "should use two-level index",
            );
            assert!(segment.pinned_block_index_size() > 0, "should pin index");
            assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

            f(segment)?;
        }

        {
            #[cfg(feature = "metrics")]
            let metrics = Arc::new(Metrics::default());

            let segment = Segment::recover(
                file.clone(),
                0,
                Arc::new(Cache::with_capacity_bytes(1_000_000)),
                Arc::new(DescriptorTable::new(10)),
                true,
                true,
                #[cfg(feature = "metrics")]
                metrics,
            )?;

            assert_eq!(0, segment.id());
            assert_eq!(items.len(), segment.metadata.item_count as usize);
            assert!(
                segment.regions.index.is_some(),
                "should use two-level index",
            );
            assert!(segment.pinned_block_index_size() > 0, "should pin index");
            assert!(segment.pinned_filter_size() > 0, "should pin filter");

            f(segment)?;
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
        None,
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
        None,
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
        None,
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
        None,
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
        None,
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
        None,
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
        Some(1),
    )
}
