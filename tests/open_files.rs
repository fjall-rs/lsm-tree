use lsm_tree::{AbstractTree, BlockCache, Config};
use std::sync::Arc;
use test_log::test;

#[test]
fn open_file_limit() -> lsm_tree::Result<()> {
    std::fs::create_dir_all(".test")?;
    let folder = tempfile::tempdir_in(".test")?;

    let tree = Config::new(folder)
        .block_size(1_024)
        .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
        .open()?;

    for _ in 0..2_048 {
        let key = 0u64.to_be_bytes();
        tree.insert(key, key, 0);
        tree.flush_active_memtable(0)?;
    }

    for _ in 0..5 {
        assert!(tree.first_key_value()?.is_some());
    }

    Ok(())
}
