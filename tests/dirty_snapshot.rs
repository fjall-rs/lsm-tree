use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn dirty_snapshot_after_drop_range() -> lsm_tree::Result<()> {
    let dir = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        dir.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", seqno.next());
    tree.flush_active_memtable(0)?;
    tree.insert("b", "b", seqno.next());
    tree.flush_active_memtable(0)?;

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    tree.drop_range("a"..="a")?;
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}
