use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_clear_snapshot() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    for c in ["a", "b", "c"] {
        tree.insert(c, c, seqno.next());
    }
    assert_eq!(3, tree.len(SeqNo::MAX, None)?);

    let snapshot = seqno.get();

    version_set_provider.clear(&tree, seqno.next())?;
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    assert_eq!(3, tree.len(snapshot, None)?);

    Ok(())
}
