use lsm_tree::{config::BlockSizePolicy, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    tree.flush_active_memtable(0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key, SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read_blob() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder, SequenceNumberCounter::default())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    tree.flush_active_memtable(0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    tree.flush_active_memtable(0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key, SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}
