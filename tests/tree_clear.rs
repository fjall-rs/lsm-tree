use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_clear() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, SequenceNumberCounter::default()).open()?;

    tree.insert("a", "a", 0);
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.clear()?;
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0)?;
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    tree.clear()?;
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}
