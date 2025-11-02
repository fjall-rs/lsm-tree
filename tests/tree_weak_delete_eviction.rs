use lsm_tree::{AbstractTree, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_weak_remove_flush_eviction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default()).open()?;

    for (idx, c) in ('a'..='z').map(|x| (x as u8).to_be_bytes()).enumerate() {
        tree.insert(c, c, idx as SeqNo);
    }

    for (idx, c) in ('a'..='z').map(|x| (x as u8).to_be_bytes()).enumerate() {
        tree.remove_weak(c, idx as SeqNo + 1000);
    }

    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    tree.flush_active_memtable(1_100)?;

    Ok(())
}
