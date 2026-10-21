use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

/// NOTE: Fix: https://github.com/fjall-rs/lsm-tree/commit/66a974ae6748646a40df475c291e04cf1dfbaece
#[test]
fn compaction_readers_grouping() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(path, version_set_provider.clone()).open()?;

    tree.insert("a".as_bytes(), "abc", seqno.next());
    tree.insert("b".as_bytes(), "abc", seqno.next());
    tree.insert("c".as_bytes(), "abc", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(3, tree.len(SeqNo::MAX, None)?);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::PullDown(0, 2)),
        seqno.next(),
        0,
    )?;

    tree.insert("d".as_bytes(), "abc", seqno.next());
    tree.insert("e".as_bytes(), "abc", seqno.next());
    tree.insert("f".as_bytes(), "abc", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(6, tree.len(SeqNo::MAX, None)?);

    tree.insert("g".as_bytes(), "abc", seqno.next());
    tree.insert("h".as_bytes(), "abc", seqno.next());
    tree.insert("i".as_bytes(), "abc", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(9, tree.len(SeqNo::MAX, None)?);

    // NOTE: Previously, create_compaction_stream would short circuit
    // breaking this
    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::PullDown(2, 3)),
        seqno.next(),
        0,
    )?;

    assert!(!tree
        .current_version()
        .level(0)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .current_version()
        .level(1)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .current_version()
        .level(2)
        .expect("level should exist")
        .is_empty());

    assert!(!tree
        .current_version()
        .level(3)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .current_version()
        .level(4)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .current_version()
        .level(5)
        .expect("level should exist")
        .is_empty());

    assert!(tree
        .current_version()
        .level(6)
        .expect("level should exist")
        .is_empty());

    Ok(())
}
