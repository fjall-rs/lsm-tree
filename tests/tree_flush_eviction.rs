use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_flush_eviction_1() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should not evict weak tombstone
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_flush_eviction_2() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should evict old value, thus weak tombstone too
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 1)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
#[ignore = "compact"]
fn tree_flush_eviction_3() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.remove("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should evict old value, but tombstone should stay until last level
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 1)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should evict tombstone because last level
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), seqno.next())?;
    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
#[ignore = "compact"]
fn tree_flush_eviction_4() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.remove("a", seqno.next());
    tree.insert("a", "a", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    // NOTE: Tombstone should stay because of seqno threshold
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 1)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert_eq!(
        1,
        tree.current_version()
            .level(0)
            .expect("should exist")
            .first()
            .expect("should have at least 1 run")
            .first()
            .expect("should have one table")
            .metadata
            .tombstone_count
    );

    // NOTE: Should evict tombstone because last level
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), seqno.next())?;

    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert_eq!(
        0,
        tree.current_version()
            .level(6)
            .expect("should exist")
            .first()
            .expect("should have at least 1 run")
            .first()
            .expect("should have one table")
            .metadata
            .tombstone_count
    );

    Ok(())
}
