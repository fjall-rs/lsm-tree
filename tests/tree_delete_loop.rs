mod common;

use common::TestPrefixExtractor;
use std::sync::Arc;

use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_delete_by_prefix() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("a:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("b:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("c:{x}").as_bytes(), value, batch_seqno);
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len()?, ITEM_COUNT * 3);
    assert_eq!(tree.prefix("a:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("b:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("c:".as_bytes()).count(), ITEM_COUNT);

    for item in tree.prefix("b:".as_bytes()) {
        let (key, _) = item?;
        tree.remove(key, seqno.next());
    }

    assert_eq!(tree.len()?, ITEM_COUNT * 2);
    assert_eq!(tree.prefix("a:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("b:".as_bytes()).count(), 0);
    assert_eq!(tree.prefix("c:".as_bytes()).count(), ITEM_COUNT);

    Ok(())
}

#[test]
fn tree_delete_by_prefix_with_extractor() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 10_000;

    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder)
        .prefix_extractor(Arc::new(TestPrefixExtractor::new(3)))
        .open()?;

    let seqno = SequenceNumberCounter::default();

    for x in 0..ITEM_COUNT as u64 {
        let value = "old".as_bytes();
        let batch_seqno = seqno.next();

        tree.insert(format!("aa:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("b:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("bb:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("c:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("cd:{x}").as_bytes(), value, batch_seqno);
        tree.insert(format!("cdd:{x}").as_bytes(), value, batch_seqno);
    }

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len()?, ITEM_COUNT * 6);
    assert_eq!(tree.prefix("aa:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("b:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("bb:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("c:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("cd:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("cdd:".as_bytes()).count(), ITEM_COUNT);

    for item in tree.prefix("b:".as_bytes()) {
        let (key, _) = item?;
        tree.remove(key, seqno.next());
    }

    assert_eq!(tree.len()?, ITEM_COUNT * 5);
    assert_eq!(tree.prefix("aa:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("b:".as_bytes()).count(), 0);
    assert_eq!(tree.prefix("bb:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("c:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("cd:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("cdd:".as_bytes()).count(), ITEM_COUNT);

    // NOTE: delete by prefix in domain
    for item in tree.prefix("cd:".as_bytes()) {
        let (key, _) = item?;
        tree.remove(key, seqno.next());
    }

    assert_eq!(tree.len()?, ITEM_COUNT * 4);
    assert_eq!(tree.prefix("aa:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("b:".as_bytes()).count(), 0);
    assert_eq!(tree.prefix("bb:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("c:".as_bytes()).count(), ITEM_COUNT);
    assert_eq!(tree.prefix("cd:".as_bytes()).count(), 0);
    assert_eq!(tree.prefix("cdd:".as_bytes()).count(), ITEM_COUNT);

    Ok(())
}

#[test]
fn tree_delete_by_range() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(folder).open()?;

    let value = "old".as_bytes();
    tree.insert("a".as_bytes(), value, 0);
    tree.insert("b".as_bytes(), value, 0);
    tree.insert("c".as_bytes(), value, 0);
    tree.insert("d".as_bytes(), value, 0);
    tree.insert("e".as_bytes(), value, 0);
    tree.insert("f".as_bytes(), value, 0);

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len()?, 6);

    for item in tree.range("c"..="e") {
        let (key, _) = item?;
        tree.remove(key, 1);
    }

    assert_eq!(tree.len()?, 3);

    Ok(())
}
