use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_clear() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    tree.insert("a", "a", seqno.next());
    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    version_set_provider.clear(&tree, seqno.next())?;
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    tree.insert("a", "a", seqno.next());
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    version_set_provider.clear(&tree, seqno.next())?;
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
