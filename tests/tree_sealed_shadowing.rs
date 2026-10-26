use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_sealed_memtable_tombstone_shadowing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(path, version_set_provider.clone()).open()?;

    tree.insert("a", "123", seqno.next());
    assert!(tree.contains_key("a", SeqNo::MAX)?);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.remove("a", seqno.next());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    version_set_provider.rotate_active_memtable(&tree);

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    let flush_result = tree.new_flush(0)?.expect("should flush to table files");
    version_set_provider.register_flush(flush_result, seqno.next());

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        0,
    )?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);

    Ok(())
}
