use lsm_tree::{
    get_tmp_folder, AbstractTree, Guard, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_l0_range_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let batch_seqno = seqno.next();
    tree.insert("a", "A", batch_seqno);
    tree.insert("b", "B", batch_seqno);
    tree.insert("c", "C", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let batch_seqno = seqno.next();
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);
    tree.insert("f", "f", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    tree.insert("g", "g".repeat(10_000), seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let mut range = tree.range("c"..="e", SeqNo::MAX, None);
    assert_eq!(b"C", &*range.next().unwrap().value()?);
    assert_eq!(b"d", &*range.next().unwrap().value()?);
    assert_eq!(b"e", &*range.next().unwrap().value()?);
    assert!(range.next().is_none());

    let mut range = tree.range("f"..="g", SeqNo::MAX, None).rev();
    assert_eq!(b"g".repeat(10_000), &*range.next().unwrap().value()?);
    assert_eq!(b"f", &*range.next().unwrap().value()?);
    assert!(range.next().is_none());

    Ok(())
}
