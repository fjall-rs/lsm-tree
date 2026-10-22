use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_read_mvcc() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a0", batch_seqno);
    tree.insert("b", "b0", batch_seqno);

    let snapshot_seqno_1 = seqno.get();
    let batch_seqno = seqno.next();
    tree.insert("a", "a1", batch_seqno);
    tree.insert("b", "b1", batch_seqno);

    let snapshot_seqno_2 = seqno.get();
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.insert("b", "b2", seqno.next());
    let snapshot_seqno_4 = seqno.get();
    tree.insert("b", "b3", seqno.next());
    let snapshot_seqno_5 = seqno.get();
    tree.insert("c", "c4", seqno.next());
    let snapshot_seqno_6 = seqno.get();

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.insert("a", "a5", seqno.next());
    let snapshot_seqno_8 = seqno.get();
    let latest_snapshot_seqno = seqno.get();

    assert_eq!(&*tree.get("a", SeqNo::MAX)?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b", SeqNo::MAX)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", SeqNo::MAX)?.unwrap(), b"c4");

    assert_eq!(&*tree.get("a", snapshot_seqno_1)?.unwrap(), b"a0");
    assert_eq!(&*tree.get("b", snapshot_seqno_1)?.unwrap(), b"b0");
    assert!(tree.get("c", snapshot_seqno_1)?.is_none());

    assert_eq!(&*tree.get("a", snapshot_seqno_2)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", snapshot_seqno_2)?.unwrap(), b"b1");
    assert!(tree.get("c", snapshot_seqno_2)?.is_none());

    assert_eq!(&*tree.get("a", snapshot_seqno_4)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", snapshot_seqno_4)?.unwrap(), b"b2");
    assert!(tree.get("c", snapshot_seqno_4)?.is_none());

    assert_eq!(&*tree.get("a", snapshot_seqno_5)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", snapshot_seqno_5)?.unwrap(), b"b3");
    assert!(tree.get("c", snapshot_seqno_5)?.is_none());

    assert_eq!(&*tree.get("a", snapshot_seqno_6)?.unwrap(), b"a1");
    assert_eq!(&*tree.get("b", snapshot_seqno_6)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", snapshot_seqno_6)?.unwrap(), b"c4");

    assert_eq!(&*tree.get("a", snapshot_seqno_8)?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b", snapshot_seqno_8)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", snapshot_seqno_8)?.unwrap(), b"c4");

    assert_eq!(&*tree.get("a", latest_snapshot_seqno)?.unwrap(), b"a5");
    assert_eq!(&*tree.get("b", latest_snapshot_seqno)?.unwrap(), b"b3");
    assert_eq!(&*tree.get("c", latest_snapshot_seqno)?.unwrap(), b"c4");

    let mut iter = tree.iter(SeqNo::MAX, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a5");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(snapshot_seqno_1, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a0");
    assert_eq!(&*iter.next().unwrap().value()?, b"b0");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(snapshot_seqno_2, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b1");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(snapshot_seqno_4, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b2");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(snapshot_seqno_5, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(snapshot_seqno_6, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a1");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(snapshot_seqno_8, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a5");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    let mut iter = tree.iter(latest_snapshot_seqno, None);

    assert_eq!(&*iter.next().unwrap().value()?, b"a5");
    assert_eq!(&*iter.next().unwrap().value()?, b"b3");
    assert_eq!(&*iter.next().unwrap().value()?, b"c4");
    assert!(iter.next().is_none());

    Ok(())
}
