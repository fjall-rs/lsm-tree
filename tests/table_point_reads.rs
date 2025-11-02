use lsm_tree::{
    config::BlockSizePolicy, AbstractTree, Config, KvSeparationOptions, SeqNo,
    SequenceNumberCounter,
};
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn table_point_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        assert!(tree.contains_key(key, SeqNo::MAX)?, "{key:?} not found");
    }

    Ok(())
}

#[test]
fn table_point_reads_mvcc() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "0", 0);
        tree.insert(key, "1", 1);
        tree.insert(key, "2", 2);
    }
    tree.flush_active_memtable(0)?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get_internal_entry(&key, SeqNo::MAX)?.unwrap();
        assert_eq!(item.key.seqno, 2);
        assert_eq!(&*item.value, b"2");

        let item = tree.get_internal_entry(&key, 3)?.unwrap();
        assert_eq!(&*item.value, b"2");

        let item = tree.get_internal_entry(&key, 2)?.unwrap();
        assert_eq!(&*item.value, b"1");

        let item = tree.get_internal_entry(&key, 1)?.unwrap();
        assert_eq!(&*item.value, b"0");
    }

    Ok(())
}

#[test]
fn table_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let keys = [0, 1, 2]
        .into_iter()
        .map(u64::to_be_bytes)
        .collect::<Vec<_>>();

    for key in &keys {
        for seqno in 0..ITEM_COUNT as u64 {
            tree.insert(key, seqno.to_string(), seqno);
        }
    }
    tree.flush_active_memtable(0)?;

    for key in &keys {
        let item = tree.get_internal_entry(key, SeqNo::MAX)?.unwrap();
        assert_eq!(item.key.seqno, ITEM_COUNT as u64 - 1);
    }

    for key in &keys {
        // NOTE: Need to start at seqno=1
        for seqno in 1..ITEM_COUNT as u64 {
            let item = tree.get_internal_entry(key, seqno)?.unwrap();

            // NOTE: When snapshot is =1, it will read any items with
            // seqno less than 1
            assert_eq!(
                String::from_utf8_lossy(&item.value)
                    .parse::<SeqNo>()
                    .unwrap(),
                seqno - 1
            );
        }
    }

    Ok(())
}

#[test]
fn blob_tree_table_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let keys = [0, 1, 2]
        .into_iter()
        .map(u64::to_be_bytes)
        .collect::<Vec<_>>();

    for key in &keys {
        for seqno in 0..ITEM_COUNT as u64 {
            tree.insert(key, seqno.to_string(), seqno);
        }
    }
    tree.flush_active_memtable(0)?;

    for key in &keys {
        let item = tree.get(key, SeqNo::MAX)?.unwrap();
        assert_eq!(
            String::from_utf8_lossy(&item).parse::<SeqNo>().unwrap(),
            ITEM_COUNT as u64 - 1
        );
    }

    for key in &keys {
        // NOTE: Need to start at seqno=1
        for seqno in 1..ITEM_COUNT as u64 {
            let value = tree.get(key, seqno)?.unwrap();

            // NOTE: When snapshot is =1, it will read any items with
            // seqno less than 1
            assert_eq!(
                String::from_utf8_lossy(&value).parse::<SeqNo>().unwrap(),
                seqno - 1
            );
        }
    }

    Ok(())
}
