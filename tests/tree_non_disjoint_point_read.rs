use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_non_disjoint_point_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("c", "c", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 0);
    tree.insert("d", "d", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 0);
    tree.insert("e", "e", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 0);
    tree.insert("f", "f", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 0);
    tree.insert("g", "g", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 0);
    tree.insert("h", "h", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.insert("a", "a", 0);
    tree.insert("z", "z", 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    tree.get("c", SeqNo::MAX).unwrap().unwrap();
    tree.get("d", SeqNo::MAX).unwrap().unwrap();
    tree.get("e", SeqNo::MAX).unwrap().unwrap();
    tree.get("f", SeqNo::MAX).unwrap().unwrap();
    tree.get("g", SeqNo::MAX).unwrap().unwrap();
    tree.get("h", SeqNo::MAX).unwrap().unwrap();

    Ok(())
}
