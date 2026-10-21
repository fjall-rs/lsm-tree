use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_fifo_limit() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Fifo::new(10, None));

    for i in 0..100 {
        tree.insert(nanoid::nanoid!(), "$", 0);

        version_set_provider.flush_active_memtable(&tree, 1, 0)?;

        version_set_provider.compact(&tree, compaction.clone(), 2 + i, 0)?;

        assert!((0..10).contains(&tree.blob_file_count()));
    }

    Ok(())
}
