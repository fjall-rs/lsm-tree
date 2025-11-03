use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn latest_snapshot() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();

    let tree = Config::new(dir.path(), seqno.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", seqno.next());
    tree.flush_active_memtable(0)?;

    tree.insert("c", "c", seqno.next());
    tree.insert("d", "d", seqno.next());

    let snapshot_seqno = seqno.get();
    assert_eq!(None, tree.get("e", snapshot_seqno)?);

    Ok(())
}
