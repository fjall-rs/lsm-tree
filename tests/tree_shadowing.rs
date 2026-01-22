use lsm_tree::{get_tmp_folder, AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_shadowing_upsert() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let key = "1".as_bytes();
    let value = "oldvalue".as_bytes();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    tree.insert(key, value, 0);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    let value = "newvalue".as_bytes();

    tree.insert(key, value, 1);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    Ok(())
}

#[test]
fn tree_shadowing_upsert_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()?;

    let key = "1".as_bytes();
    let value = "oldvalue".as_bytes();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    tree.insert(key, value, 0);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    let value = "newvalue".as_bytes();

    tree.insert(key, value, 1);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    Ok(())
}

#[test]
fn tree_shadowing_delete() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();

    let key = "1".as_bytes();
    let value = "oldvalue".as_bytes();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    tree.insert(key, value, 0);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.remove(key, 1);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.get(key, SeqNo::MAX)?.is_none());

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.get(key, SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
fn tree_shadowing_delete_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()?;

    let key = "1".as_bytes();
    let value = "oldvalue".as_bytes();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    tree.insert(key, value, 0);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert_eq!(tree.get(key, SeqNo::MAX)?, Some(value.into()));

    tree.remove(key, 1);
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.get(key, SeqNo::MAX)?.is_none());

    tree.flush_active_memtable(0)?;
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.get(key, SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
fn tree_shadowing_range() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "old".as_bytes();
        tree.insert(key, value, seqno.next());
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "old".as_bytes()));

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "new".as_bytes();
        tree.insert(key, value, seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    Ok(())
}

#[test]
fn tree_shadowing_range_blob() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "old".as_bytes();
        tree.insert(key, value, seqno.next());
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "old".as_bytes()));

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        let value = "new".as_bytes();
        tree.insert(key, value, seqno.next());
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT);
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    Ok(())
}

#[test]
fn tree_shadowing_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default()).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("pre:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("prefix:{x}").as_bytes(), value, batch_seqno);
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "old".as_bytes()));

    for x in 0..ITEM_COUNT as u64 {
        let value = "new".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("pre:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("prefix:{x}").as_bytes(), value, batch_seqno);
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    Ok(())
}

#[test]
fn tree_shadowing_prefix_blob() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(&folder, seqno.clone(), SequenceNumberCounter::default())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("pre:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("prefix:{x}").as_bytes(), value, batch_seqno);
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "old".as_bytes()));

    for x in 0..ITEM_COUNT as u64 {
        let value = "new".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("pre:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("prefix:{x}").as_bytes(), value, batch_seqno);
    }

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, ITEM_COUNT * 2);
    assert_eq!(
        tree.prefix("pre".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT * 2
    );
    assert_eq!(
        tree.prefix("prefix".as_bytes(), SeqNo::MAX, None).count(),
        ITEM_COUNT
    );
    assert!(tree
        .iter(SeqNo::MAX, None)
        .all(|x| &*x.value().unwrap() == "new".as_bytes()));

    Ok(())
}
