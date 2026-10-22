use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, KvSeparationOptions,
    MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

const ITEM_COUNT: usize = 1_000;

#[test]
fn table_point_reads() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let batch_seqno = seqno.next();
    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), batch_seqno);
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        assert!(tree.contains_key(key, SeqNo::MAX)?, "{key:?} not found");
    }

    Ok(())
}

#[test]
fn table_point_reads_mvcc() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let batch_seqno_0 = seqno.next();
    let batch_seqno_1 = seqno.next();
    let batch_seqno_2 = seqno.next();

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "0", batch_seqno_0);
        tree.insert(key, "1", batch_seqno_1);
        tree.insert(key, "2", batch_seqno_2);
    }

    let snapshot_seqno = seqno.get();
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get_internal_entry(&key, SeqNo::MAX)?.unwrap();
        assert_eq!(item.key.seqno, 2);
        assert_eq!(&*item.value, b"2");

        let item = tree.get_internal_entry(&key, snapshot_seqno)?.unwrap();
        assert_eq!(&*item.value, b"2");

        let item = tree.get_internal_entry(&key, batch_seqno_2)?.unwrap();
        assert_eq!(&*item.value, b"1");

        let item = tree.get_internal_entry(&key, batch_seqno_1)?.unwrap();
        assert_eq!(&*item.value, b"0");
    }

    Ok(())
}

#[test]
fn table_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let keys = [0, 1, 2]
        .into_iter()
        .map(u64::to_be_bytes)
        .collect::<Vec<_>>();

    let batch_seqnos = (0..ITEM_COUNT as u64)
        .map(|seqno_value| (seqno_value, seqno.next()))
        .collect::<Vec<_>>();

    for key in &keys {
        for (seqno_value, batch_seqno) in &batch_seqnos {
            tree.insert(key, seqno_value.to_string(), *batch_seqno);
        }
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    for key in &keys {
        let item = tree.get_internal_entry(key, SeqNo::MAX)?.unwrap();
        assert_eq!(item.key.seqno, ITEM_COUNT as u64 - 1);
    }

    for key in &keys {
        // NOTE: Need to start at the second batch
        for (seqno_value, snapshot_seqno) in batch_seqnos.iter().skip(1) {
            let item = tree.get_internal_entry(key, *snapshot_seqno)?.unwrap();

            // NOTE: A snapshot at a batch sequence number reads the preceding
            // item version.
            assert_eq!(
                String::from_utf8_lossy(&item.value)
                    .parse::<SeqNo>()
                    .unwrap(),
                *seqno_value - 1
            );
        }
    }

    Ok(())
}

#[test]
fn blob_tree_table_point_reads_mvcc_slab() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let keys = [0, 1, 2]
        .into_iter()
        .map(u64::to_be_bytes)
        .collect::<Vec<_>>();

    let batch_seqnos = (0..ITEM_COUNT as u64)
        .map(|seqno_value| (seqno_value, seqno.next()))
        .collect::<Vec<_>>();

    for key in &keys {
        for (seqno_value, batch_seqno) in &batch_seqnos {
            tree.insert(key, seqno_value.to_string(), *batch_seqno);
        }
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    for key in &keys {
        let item = tree.get(key, SeqNo::MAX)?.unwrap();
        assert_eq!(
            String::from_utf8_lossy(&item).parse::<SeqNo>().unwrap(),
            ITEM_COUNT as u64 - 1
        );
    }

    for key in &keys {
        // NOTE: Need to start at the second batch
        for (seqno_value, snapshot_seqno) in batch_seqnos.iter().skip(1) {
            let value = tree.get(key, *snapshot_seqno)?.unwrap();

            // NOTE: A snapshot at a batch sequence number reads the preceding
            // item version.
            assert_eq!(
                String::from_utf8_lossy(&value).parse::<SeqNo>().unwrap(),
                *seqno_value - 1
            );
        }
    }

    Ok(())
}
