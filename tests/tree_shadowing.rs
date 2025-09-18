use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_shadowing_upsert() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open()?;

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
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open_as_blob_tree()?;

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
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open().unwrap();

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
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open_as_blob_tree().unwrap();

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

    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open()?;

    let seqno = SequenceNumberCounter::default();

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
#[ignore]
fn tree_shadowing_range_blob() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open_as_blob_tree()?;

    let seqno = SequenceNumberCounter::default();

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

    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open()?;

    let seqno = SequenceNumberCounter::default();

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
#[ignore]
fn tree_shadowing_prefix_blob() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder).open_as_blob_tree()?;

    let seqno = SequenceNumberCounter::default();

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
