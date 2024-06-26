use lsm_tree::Config;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn segment_point_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        assert!(tree.contains_key(key)?, "{key:?} not found");
    }

    Ok(())
}

#[test]
fn segment_point_reads_mvcc() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "0", 0);
        tree.insert(key, "1", 1);
        tree.insert(key, "2", 2);
    }
    tree.flush_active_memtable()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get_internal_entry(key, true, None)?.unwrap();
        assert_eq!(item.seqno, 2);
        assert_eq!(&*item.value, b"2");

        let snapshot = tree.snapshot(3);
        let item = snapshot.get(key)?.unwrap();
        assert_eq!(&*item, b"2");

        let snapshot = tree.snapshot(2);
        let item = snapshot.get(key)?.unwrap();
        assert_eq!(&*item, b"1");

        let snapshot = tree.snapshot(1);
        let item = snapshot.get(key)?.unwrap();
        assert_eq!(&*item, b"0");
    }

    Ok(())
}

#[test]
fn segment_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.into_path();

    let tree = Config::new(folder).block_size(1_024).open()?;

    let keys = [0, 1, 2]
        .into_iter()
        .map(u64::to_be_bytes)
        .collect::<Vec<_>>();

    for key in &keys {
        for seqno in 0..ITEM_COUNT as u64 {
            tree.insert(key, ITEM_COUNT.to_string(), seqno);
        }
    }
    tree.flush_active_memtable()?;

    for key in &keys {
        let item = tree.get_internal_entry(key, true, None)?.unwrap();
        assert_eq!(item.seqno, ITEM_COUNT as u64 - 1);
    }

    for key in &keys {
        // NOTE: Need to start at seqno=1
        for seqno in 1..ITEM_COUNT as u64 {
            let snapshot = tree.snapshot(seqno);
            let item = snapshot.get_internal_entry(key)?.unwrap();

            // NOTE: When snapshot is =1, it will read any items with
            // seqno less than 1
            assert_eq!(item.seqno, seqno - 1);
        }
    }

    Ok(())
}
