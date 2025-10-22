use lsm_tree::{config::BlockSizePolicy, AbstractTree, Config, SeqNo};
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn segment_point_reads() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
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
fn segment_point_reads_mvcc() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
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

        // TODO: 3.0.0 test snapshot reads

        // let snapshot = tree.snapshot(3);
        // let item = snapshot.get(key)?.unwrap();
        // assert_eq!(&*item, b"2");

        // let snapshot = tree.snapshot(2);
        // let item = snapshot.get(key)?.unwrap();
        // assert_eq!(&*item, b"1");

        // let snapshot = tree.snapshot(1);
        // let item = snapshot.get(key)?.unwrap();
        // assert_eq!(&*item, b"0");
    }

    Ok(())
}

#[test]
fn segment_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
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

    // TODO: 3.0.0 test snapshot reads

    // for key in &keys {
    //     // NOTE: Need to start at seqno=1
    //     for seqno in 1..ITEM_COUNT as u64 {
    //         let snapshot = tree.snapshot(seqno);
    //         let item = snapshot.get(key)?.unwrap();

    //         // NOTE: When snapshot is =1, it will read any items with
    //         // seqno less than 1
    //         assert_eq!(
    //             String::from_utf8_lossy(&item).parse::<SeqNo>().unwrap(),
    //             seqno - 1
    //         );
    //     }
    // }

    Ok(())
}

#[test]
#[ignore]
fn blob_tree_segment_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(Default::default()))
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

    // TODO: 3.0.0 test snapshot reads

    // for key in &keys {
    //     // NOTE: Need to start at seqno=1
    //     for seqno in 1..ITEM_COUNT as u64 {
    //         let snapshot = tree.snapshot(seqno);
    //         let item = snapshot.get(key)?.unwrap();

    //         // NOTE: When snapshot is =1, it will read any items with
    //         // seqno less than 1
    //         assert_eq!(
    //             String::from_utf8_lossy(&item).parse::<SeqNo>().unwrap(),
    //             seqno - 1
    //         );
    //     }
    // }

    Ok(())
}
