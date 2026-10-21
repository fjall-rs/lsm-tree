use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key, SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}

#[test]
fn tree_disjoint_point_read_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("b", "b", 0);
    tree.insert("c", "c", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("d", "d", 0);
    tree.insert("e", "e", 0);
    tree.insert("f", "f", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
        let value = tree.get(key, SeqNo::MAX).unwrap().unwrap();
        assert_eq!(&*value, key)
    }

    Ok(())
}
