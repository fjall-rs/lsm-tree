use lsm_tree::{get_tmp_folder, AbstractTree, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_weak_delete_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.insert("a", "new", 2);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.remove_weak("a", 3);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_semi_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "old", 0);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.remove_weak("a", 1);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_weak_delete_semi_flush_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "old", 0);
    assert!(tree.contains_key("a", SeqNo::MAX)?);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());

    tree.remove_weak("a", 1);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_weak_delete_resurrection() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.insert("a", "new", 1);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    tree.remove_weak("a", 2);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
