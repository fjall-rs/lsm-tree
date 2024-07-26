use lsm_tree::{BlockCache, Config};
use std::sync::Arc;
use test_log::test;

#[test]
fn open_file_limit() {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(folder)
        .block_size(1_024)
        .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
        .open()
        .unwrap();

    for _ in 0..2_048 {
        let key = 0u64.to_be_bytes();
        tree.insert(key, key, 0);
        tree.flush_active_memtable().unwrap();
    }

    for _ in 0..5 {
        assert!(tree.first_key_value().unwrap().is_some());
    }
}
