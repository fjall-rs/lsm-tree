use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn tree_weak_delete_simple() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(None, None)?);
    assert!(tree.contains_key("a", None)?);

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(None, None)?);
    assert!(!tree.contains_key("a", None)?);

    tree.insert("a", "new", 2);
    assert_eq!(1, tree.len(None, None)?);
    assert!(tree.contains_key("a", None)?);

    tree.remove_weak("a", 3);
    assert_eq!(0, tree.len(None, None)?);
    assert!(!tree.contains_key("a", None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(None, None)?);

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(None, None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(0, tree.len(None, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_semi_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(None, None)?);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(None, None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.segment_count());
    assert_eq!(0, tree.len(None, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert!(tree.contains_key("a", None)?);

    tree.remove_weak("a", 1);
    assert!(!tree.contains_key("a", None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());
    assert!(!tree.contains_key("a", None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_semi_flush_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert!(tree.contains_key("a", None)?);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());

    tree.remove_weak("a", 1);
    assert!(!tree.contains_key("a", None)?);

    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.segment_count());
    assert!(!tree.contains_key("a", None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_resurrection() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len(None, None)?);

    tree.insert("a", "new", 1);
    assert_eq!(1, tree.len(None, None)?);

    tree.remove_weak("a", 2);
    assert_eq!(0, tree.len(None, None)?);

    Ok(())
}
