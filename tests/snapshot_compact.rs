use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn snapshot_after_compaction_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "a", seqno.next());

    let snapshot_seqno = seqno.get();
    assert_eq!(b"a", &*tree.get("a", u64::MAX)?.unwrap());
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    tree.insert("a", "b", seqno.next());
    tree.flush_active_memtable(0)?;
    assert_eq!(b"b", &*tree.get("a", u64::MAX)?.unwrap());
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    tree.major_compact(u64::MAX, 0)?;
    assert_eq!(b"b", &*tree.get("a", u64::MAX)?.unwrap());
    assert_eq!(b"a", &*tree.get("a", snapshot_seqno)?.unwrap());

    Ok(())
}

#[test]
fn snapshot_after_compaction_iters() -> lsm_tree::Result<()> {
    const ITEM_COUNT: usize = 100;

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    let snapshot_seqno = seqno.get();
    assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);

    for x in 0..ITEM_COUNT as u64 {
        let key = x.to_be_bytes();
        tree.insert(key, "abc".as_bytes(), seqno.next());
    }

    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    assert_eq!(tree.len(seqno.get(), None)?, ITEM_COUNT);

    assert_eq!(ITEM_COUNT, tree.len(snapshot_seqno, None)?);

    Ok(())
}
