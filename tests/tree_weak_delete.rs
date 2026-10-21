use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_weak_delete_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "old", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.insert("a", "new", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "old", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_semi_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "old", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());

    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "old", seqno.next());
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.remove_weak("a", seqno.next());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_weak_delete_semi_flush_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "old", seqno.next());
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());

    tree.remove_weak("a", seqno.next());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_weak_delete_resurrection() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "old", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.insert("a", "new", seqno.next());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.remove_weak("a", seqno.next());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
