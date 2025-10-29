use lsm_tree::{config::BlockSizePolicy, AbstractTree, Config, SeqNo};
use test_log::test;

#[test]
fn table_remove_weak_simple() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder)
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        // .index_block_size_policy(BlockSizePolicy::all(1_024))
        .open()?;

    tree.insert("a", "a", 0);
    tree.insert("a", "b", 1);
    tree.remove_weak("a", 2);

    tree.flush_active_memtable(0)?;

    assert!(tree.get("a", SeqNo::MAX)?.is_none());

    Ok(())
}
