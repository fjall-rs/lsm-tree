use super::*;
use tempfile::tempdir;
use test_log::test;

#[test]
#[allow(clippy::unwrap_used)]
fn table_recover() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;
        writer.write(crate::InternalValue::from_components(
            b"abc",
            b"asdasdasd",
            3,
            crate::ValueType::Value,
        ))?;
        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(1, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            b"abc",
            &*segment
                .get(
                    b"abc",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                )?
                .unwrap()
                .key
                .user_key,
        );
        assert_eq!(
            None,
            segment.get(
                b"def",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
            )?
        );
        assert_eq!(
            None,
            segment.get(
                b"____",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
            )?
        );

        assert_eq!(
            segment.metadata.key_range,
            crate::KeyRange::new((b"abc".into(), b"abc".into())),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_volatile_index_point_read() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

        writer.write(crate::InternalValue::from_components(
            b"abc",
            b"asdasdasd",
            3,
            crate::ValueType::Value,
        ))?;

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            false,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(1, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 2 because we use a full index
        assert!(segment.regions.index.is_none(), "should use full index");
        assert_eq!(
            0,
            segment.pinned_block_index_size(),
            "should not pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            b"abc",
            &*segment
                .get(
                    b"abc",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                )?
                .unwrap()
                .key
                .user_key,
        );
        assert_eq!(
            None,
            segment.get(
                b"def",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
            )?
        );
        assert_eq!(
            None,
            segment.get(
                b"____",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
            )?
        );

        assert_eq!(
            segment.metadata.key_range,
            crate::KeyRange::new((b"abc".into(), b"abc".into())),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_partitioned_index_point_read() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_partitioned_index();

        writer.write(crate::InternalValue::from_components(
            b"abc",
            b"asdasdasd",
            3,
            crate::ValueType::Value,
        ))?;

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(1, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(2, segment.metadata.index_block_count); // 2 because we use a full index, + 1 2nd level index block
        assert!(segment.regions.index.is_some(), "should use 2-tier index");
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin TLI block",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            b"abc",
            &*segment
                .get(
                    b"abc",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                )?
                .unwrap()
                .key
                .user_key,
        );
        assert_eq!(
            None,
            segment.get(
                b"def",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
            )?
        );
        assert_eq!(
            None,
            segment.get(
                b"____",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"____")
            )?
        );

        assert_eq!(
            segment.metadata.key_range,
            crate::KeyRange::new((b"abc".into(), b"abc".into())),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_partitioned_index_point_read_mvcc_block_boundary() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_partitioned_index();

        writer.write(crate::InternalValue::from_components(
            b"a",
            b"5",
            5,
            crate::ValueType::Value,
        ))?;
        writer.write(crate::InternalValue::from_components(
            b"a",
            b"4",
            4,
            crate::ValueType::Value,
        ))?;
        writer.write(crate::InternalValue::from_components(
            b"a",
            b"3",
            3,
            crate::ValueType::Value,
        ))?;
        writer.spill_block()?;
        writer.write(crate::InternalValue::from_components(
            b"a",
            b"2",
            2,
            crate::ValueType::Value,
        ))?;
        writer.write(crate::InternalValue::from_components(
            b"a",
            b"1",
            1,
            crate::ValueType::Value,
        ))?;

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(5, segment.metadata.item_count);
        assert_eq!(2, segment.metadata.data_block_count);
        assert_eq!(2, segment.metadata.index_block_count); // 2 because we use a full index, + 1 2nd level index block
        assert!(segment.regions.index.is_some(), "should use 2-tier index");
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin TLI block",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        let key_hash = crate::segment::filter::standard_bloom::Builder::get_hash(b"a");

        assert_eq!(
            b"5",
            &*segment.get(b"a", SeqNo::MAX, key_hash)?.unwrap().value
        );
        assert_eq!(b"4", &*segment.get(b"a", 5, key_hash)?.unwrap().value);
        assert_eq!(b"3", &*segment.get(b"a", 4, key_hash)?.unwrap().value);
        assert_eq!(b"2", &*segment.get(b"a", 3, key_hash)?.unwrap().value);
        assert_eq!(b"1", &*segment.get(b"a", 2, key_hash)?.unwrap().value);
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_scan() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(3, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(items, &*segment.scan()?.flatten().collect::<Vec<_>>());

        assert_eq!(
            segment.metadata.key_range,
            crate::KeyRange::new((b"abc".into(), b"xyz".into())),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_iter_simple() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(3, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(items, &*segment.iter().flatten().collect::<Vec<_>>());
        assert_eq!(
            items.iter().rev().cloned().collect::<Vec<_>>(),
            &*segment.iter().rev().flatten().collect::<Vec<_>>(),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_range_simple() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(3, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            items.iter().skip(1).cloned().collect::<Vec<_>>(),
            &*segment
                .range(UserKey::from("b")..)
                .flatten()
                .collect::<Vec<_>>()
        );

        assert_eq!(
            items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
            &*segment
                .range(UserKey::from("b")..)
                .rev()
                .flatten()
                .collect::<Vec<_>>(),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_range_simple_volatile_index() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = [
        crate::InternalValue::from_components(b"abc", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"def", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"xyz", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            false,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(3, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert_eq!(
            0,
            segment.pinned_block_index_size(),
            "should not pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            items.iter().skip(1).cloned().collect::<Vec<_>>(),
            &*segment
                .range(UserKey::from("b")..)
                .flatten()
                .collect::<Vec<_>>()
        );

        assert_eq!(
            items.iter().skip(1).rev().cloned().collect::<Vec<_>>(),
            &*segment
                .range(UserKey::from("b")..)
                .rev()
                .flatten()
                .collect::<Vec<_>>(),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_range_ping_pong() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = (0u64..10)
        .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, crate::ValueType::Value))
        .collect::<Vec<_>>();

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(10, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        let mut iter =
            segment.range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()));

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
#[allow(clippy::unwrap_used)]
fn table_range_ping_pong_partitioned() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = (0u64..10)
        .map(|i| InternalValue::from_components(i.to_be_bytes(), "", 0, crate::ValueType::Value))
        .collect::<Vec<_>>();

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_partitioned_index();

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(10, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(2, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_some(),
            "should use full index, so 2nd level index blocks should exist",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin top-level index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        let mut iter =
            segment.range(UserKey::from(5u64.to_be_bytes())..UserKey::from(10u64.to_be_bytes()));

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
#[allow(clippy::unwrap_used)]
fn table_range_multiple_data_blocks() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = [
        crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?.use_data_block_size(1);

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(5, segment.metadata.item_count);
        assert_eq!(5, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
            &*segment
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
            &*segment
                .range(UserKey::from("b")..=UserKey::from("d"))
                .rev()
                .flatten()
                .collect::<Vec<_>>(),
        );
    }

    Ok(())
}

#[test]
#[allow(clippy::unwrap_used)]
fn table_range_multiple_data_blocks_partitioned_index() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    let items = [
        crate::InternalValue::from_components(b"a", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"b", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"c", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"d", b"asdasdasd", 3, crate::ValueType::Value),
        crate::InternalValue::from_components(b"e", b"asdasdasd", 3, crate::ValueType::Value),
    ];

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?
            .use_data_block_size(1)
            .use_partitioned_index();

        for item in items.iter().cloned() {
            writer.write(item)?;
        }

        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            true,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(5, segment.metadata.item_count);
        assert_eq!(5, segment.metadata.data_block_count);
        assert_eq!(2, segment.metadata.index_block_count); // 1 because we use a full index and a 2nd level index block
        assert!(
            segment.regions.index.is_some(),
            "should use partitioned index, so 2nd level index blocks should exist",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin top-level index",
        );
        assert!(segment.pinned_filter_size() > 0, "should pin filter");

        assert_eq!(
            items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
            &*segment
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
            &*segment
                .range(UserKey::from("b")..=UserKey::from("d"))
                .rev()
                .flatten()
                .collect::<Vec<_>>(),
        );
    }

    Ok(())
}

// TODO: when using stats cfg feature: check filter hits += 1
#[test]
#[allow(clippy::unwrap_used)]
fn table_unpinned_filter() -> crate::Result<()> {
    let dir = tempdir()?;
    let file = dir.path().join("table");

    {
        let mut writer = crate::segment::Writer::new(file.clone(), 5)?;
        writer.write(crate::InternalValue::from_components(
            b"abc",
            b"asdasdasd",
            3,
            crate::ValueType::Value,
        ))?;
        let _trailer = writer.finish()?;
    }

    {
        #[cfg(feature = "metrics")]
        let metrics = Arc::new(Metrics::default());

        let segment = Segment::recover(
            file,
            0,
            Arc::new(Cache::with_capacity_bytes(1_000_000)),
            Arc::new(DescriptorTable::new(10)),
            false,
            true,
            #[cfg(feature = "metrics")]
            metrics,
        )?;

        assert_eq!(5, segment.id());
        assert_eq!(1, segment.metadata.item_count);
        assert_eq!(1, segment.metadata.data_block_count);
        assert_eq!(1, segment.metadata.index_block_count); // 1 because we use a full index
        assert!(
            segment.regions.index.is_none(),
            "should use full index, so only TLI exists",
        );
        assert!(
            segment.pinned_block_index_size() > 0,
            "should pin block index",
        );
        assert_eq!(0, segment.pinned_filter_size(), "should not pin filter");

        assert_eq!(
            b"abc",
            &*segment
                .get(
                    b"abc",
                    SeqNo::MAX,
                    crate::segment::filter::standard_bloom::Builder::get_hash(b"abc")
                )?
                .unwrap()
                .key
                .user_key,
        );
        assert_eq!(
            None,
            segment.get(
                b"def",
                SeqNo::MAX,
                crate::segment::filter::standard_bloom::Builder::get_hash(b"def")
            )?
        );

        assert_eq!(
            segment.metadata.key_range,
            crate::KeyRange::new((b"abc".into(), b"abc".into())),
        );
    }

    Ok(())
}
