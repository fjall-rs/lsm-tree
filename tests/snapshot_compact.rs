use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_snapshot_after_compaction_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    tree.insert("a", "b", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(b"b", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        0,
    )?;

    assert_eq!(b"b", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}

#[test]
fn tree_snapshot_after_compaction_iters() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 100;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    let snapshot_seqno = seqno.get();
    assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        0,
    )?;

    assert_eq!(tree.len(seqno.get(), None)?, ITEM_COUNT);

    assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);

    Ok(())
}
