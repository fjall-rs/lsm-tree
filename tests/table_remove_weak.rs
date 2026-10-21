use lsm_tree::{
    config::BlockSizePolicy, get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn table_remove_weak_simple() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("a", "b", 1);
    tree.remove_weak("a", 2);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert!(tree.get("a", SeqNo::MAX)?.is_none());

    Ok(())
}
