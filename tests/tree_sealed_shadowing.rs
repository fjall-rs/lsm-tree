use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_sealed_memtable_tombstone_shadowing() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = Config::new(path).open()?;

    tree.insert("a", "123", 0);
    assert!(tree.contains_key("a")?);

    tree.flush_active_memtable()?;

    tree.remove("a", 1);
    assert!(!tree.contains_key("a")?);

    let (id, memtable) = tree.rotate_memtable().unwrap();
    assert!(!tree.contains_key("a")?);

    let segment = tree.flush_memtable(id, &memtable)?;
    tree.register_segments(&[segment])?;

    assert!(!tree.contains_key("a")?);

    tree.major_compact(u64::MAX)?;

    assert!(!tree.contains_key("a")?);

    Ok(())
}
