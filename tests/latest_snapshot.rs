use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_latest_snapshot() -> lsm_tree::Result<()> {
    let dir = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = crate::Config::new(&dir, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.insert("b", "b", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.insert("c", "c", seqno.next());
    tree.insert("d", "d", seqno.next());

    let snapshot_seqno = seqno.get();
    assert_eq!(None, tree.get("e", snapshot_seqno)?);

    Ok(())
}
