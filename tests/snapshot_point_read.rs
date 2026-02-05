use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter,
};
use test_log::test;

#[test]
#[ignore]
fn snapshot_404() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    // .index_block_size_policy(BlockSizePolicy::all(1_024))
    .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("a2", "a2", batch_seqno);
    tree.insert("c", "c", batch_seqno);

    tree.flush_active_memtable(0)?;

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());
    assert_eq!(b"a2", &*tree.get("a2", snapshot_seqno)?.unwrap());
    assert!(tree.get("b", snapshot_seqno)?.is_none());
    assert_eq!(b"c", &*tree.get("c", snapshot_seqno)?.unwrap());

    assert!(tree.get("a", 0)?.is_none());
    assert!(tree.get("a2", 0)?.is_none());
    assert!(tree.get("b", 0)?.is_none());
    assert!(tree.get("c", 0)?.is_none());

    assert_eq!(b"a2", &*tree.get("a2", SeqNo::MAX)?.unwrap());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert_eq!(b"c", &*tree.get("c", SeqNo::MAX)?.unwrap());

    Ok(())
}

#[test]
fn snapshot_lots_of_versions() -> lsm_tree::Result<()> {
    let version_count = 600;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    // .index_block_size_policy(BlockSizePolicy::all(1_024))
    .open()?;

    let key = "abc";

    for seqno in 0u64..version_count {
        tree.insert(key, format!("abc{version_count}").as_bytes(), seqno);
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);

    for seqno in 1..version_count {
        let item = tree
            .get_internal_entry(key.as_bytes(), seqno)?
            .expect("should exist");

        assert_eq!(format!("abc{}", version_count).as_bytes(), &*item.value);

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!(format!("abc{}", version_count).as_bytes(), &*item);
    }

    Ok(())
}

const ITEM_COUNT: usize = 1;
const BATCHES: usize = 10;

#[test]
fn snapshot_disk_point_reads() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    // .index_block_size_policy(BlockSizePolicy::all(1_024))
    .open()?;

    for batch in 0..BATCHES {
        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("abc{batch}").as_bytes(), seqno.next());
        }
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);
    }

    let snapshot_seqno = seqno.get();

    assert_eq!(tree.len(SeqNo::MAX, None)?, tree.len(snapshot_seqno, None)?);

    // This batch will be too new for snapshot (invisible)
    for batch in 0..BATCHES {
        let batch_seqno = seqno.next();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("def{batch}").as_bytes(), batch_seqno);
        }
    }
    tree.flush_active_memtable(0)?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key, snapshot_seqno)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!("def9".as_bytes(), &*item);
    }

    Ok(())
}

#[test]
fn snapshot_disk_and_memtable_reads() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .data_block_size_policy(BlockSizePolicy::all(1_024))
    // .index_block_size_policy(BlockSizePolicy::all(1_024))
    .open()?;

    for batch in 0..BATCHES {
        let batch_seqno = seqno.next();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("abc{batch}").as_bytes(), batch_seqno);
        }
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);

    let snapshot_seqno = seqno.get();

    assert_eq!(tree.len(SeqNo::MAX, None)?, tree.len(snapshot_seqno, None)?);

    // This batch will be in memtable and too new for snapshot (invisible)
    for batch in 0..BATCHES {
        let batch_seqno = seqno.next();

        for x in 0..ITEM_COUNT as u64 {
            let key = x.to_be_bytes();
            tree.insert(key, format!("def{batch}").as_bytes(), batch_seqno);
        }
    }

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();

        let item = tree.get(key, snapshot_seqno)?.expect("should exist");
        assert_eq!("abc9".as_bytes(), &*item);

        let item = tree.get(key, SeqNo::MAX)?.expect("should exist");
        assert_eq!("def9".as_bytes(), &*item);
    }

    Ok(())
}
