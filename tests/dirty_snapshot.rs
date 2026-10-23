use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn dirty_snapshot_after_drop_range() -> lsm_tree::Result<()> {
    let dir = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(dir.path(), version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    tree.insert("b", "b", seqno.next());
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    version_set_provider.drop_range(&tree, "a"..="a", seqno.next(), 0)?;
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}
