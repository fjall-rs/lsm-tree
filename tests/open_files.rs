use lsm_tree::{AbstractTree, BlockCache, Config};
use std::sync::Arc;
use test_log::test;

#[test]
#[ignore = "this is a sanity check test, but the data it writes is impossible, so the range scan first_key_value is doing is crashing as of 2.1.1 lol"]
fn open_file_limit() -> lsm_tree::Result<()> {
    std::fs::create_dir_all(".test_open_files")?;
    let folder = tempfile::tempdir_in(".test_open_files")?;

    let tree = Config::new(folder)
        .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
        .open()?;

    for _ in 0..2_048 {
        let key = 0u64.to_be_bytes();
        tree.insert(key, key, 0);
        tree.flush_active_memtable(0)?;
    }

    for _ in 0..5 {
        assert!(tree.first_key_value(None, None)?.is_some());
    }

    Ok(())
}
