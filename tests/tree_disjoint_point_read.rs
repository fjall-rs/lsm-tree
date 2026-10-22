use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let batch_seqno = seqno.next();
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);
    tree.insert("f", "f", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key, SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(Default::default()))
        .open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let batch_seqno = seqno.next();
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);
    tree.insert("f", "f", batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key, SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}
