use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_sealed_memtable_tombstone_shadowing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "123", 0);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.flush_active_memtable(0)?;

    tree.remove("a", 1);
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.rotate_memtable().unwrap();

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    {
        let flush_lock = tree.get_flush_lock();
        assert!(tree.flush(&flush_lock, 0)?.unwrap() > 0);
    }

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.major_compact(u64::MAX, 2)?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}
