use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn tree_weak_delete_simple() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len()?);

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len()?);

    tree.insert("a", "new", 2);
    assert_eq!(1, tree.len()?);
    assert_eq!(b"new", &*tree.get("a")?.unwrap());

    tree.remove_weak("a", 3);
    assert_eq!(0, tree.len()?);

    Ok(())
}

#[test]
fn tree_weak_delete_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len()?);

    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len()?);

    tree.flush_active_memtable(0)?;

    assert_eq!(1, tree.segment_count());
    assert_eq!(0, tree.len()?);

    Ok(())
}

#[test]
fn tree_weak_delete_resurrection() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "old", 0);
    assert_eq!(1, tree.len()?);

    tree.insert("a", "new", 1);
    assert_eq!(1, tree.len()?);

    tree.remove_weak("a", 2);
    assert_eq!(1, tree.len()?);
    assert_eq!(b"old", &*tree.get("a")?.unwrap());

    Ok(())
}
