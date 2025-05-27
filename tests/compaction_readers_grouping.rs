use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn compaction_readers_grouping() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = Config::new(path).open()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a".as_bytes(), "abc", seqno.next());
    tree.insert("b".as_bytes(), "abc", seqno.next());
    tree.insert("c".as_bytes(), "abc", seqno.next());
    tree.flush_active_memtable(0)?;
    assert_eq!(3, tree.len(None, None)?);

    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 2)), 0)?;

    tree.insert("d".as_bytes(), "abc", seqno.next());
    tree.insert("e".as_bytes(), "abc", seqno.next());
    tree.insert("f".as_bytes(), "abc", seqno.next());
    tree.flush_active_memtable(0)?;
    assert_eq!(6, tree.len(None, None)?);

    tree.insert("g".as_bytes(), "abc", seqno.next());
    tree.insert("h".as_bytes(), "abc", seqno.next());
    tree.insert("i".as_bytes(), "abc", seqno.next());
    tree.flush_active_memtable(0)?;
    assert_eq!(9, tree.len(None, None)?);

    // NOTE: Previously, create_compaction_stream would short circuit
    // breaking this
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(2, 3)), 0)?;

    assert!(!tree
        .manifest
        .read()
        .expect("asdasd")
        .current_version()
        .level(0)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .manifest
        .read()
        .expect("asdasd")
        .current_version()
        .level(1)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .manifest
        .read()
        .expect("asdasd")
        .current_version()
        .level(2)
        .expect("level should exist")
        .is_empty());

    assert!(!tree
        .manifest
        .read()
        .expect("asdasd")
        .current_version()
        .level(3)
        .expect("level should exist")
        .is_empty());

    Ok(())
}
