use lsm_tree::{AbstractTree, Config, SeqNo};
use test_log::test;

#[test]
fn tree_sealed_memtable_tombstone_shadowing() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = Config::new(path).open()?;

    tree.insert("a", "123", 0);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.flush_active_memtable(0)?;

    tree.remove("a", 1);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    let (id, memtable) = tree.rotate_memtable().unwrap();
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    let (segment, _) = tree.flush_memtable(id, &memtable, 0)?.unwrap();
    tree.register_segments(&[segment], None, 0)?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.major_compact(u64::MAX, 2)?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}
