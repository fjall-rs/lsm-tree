use lsm_tree::{
    get_tmp_folder, AbstractTree, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_l0_point_read() -> lsm_tree::Result<()> {
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

    tree.insert("g", "g", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(b"A", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert_eq!(b"B", &*tree.get("b", SeqNo::MAX)?.unwrap());
    assert_eq!(b"C", &*tree.get("c", SeqNo::MAX)?.unwrap());
    assert_eq!(b"d", &*tree.get("d", SeqNo::MAX)?.unwrap());
    assert_eq!(b"e", &*tree.get("e", SeqNo::MAX)?.unwrap());
    assert_eq!(b"f", &*tree.get("f", SeqNo::MAX)?.unwrap());
    assert_eq!(b"g", &*tree.get("g", SeqNo::MAX)?.unwrap());

    Ok(())
}
