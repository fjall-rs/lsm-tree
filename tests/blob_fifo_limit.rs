use lsm_tree::{AbstractTree, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_fifo_limit() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Fifo::new(10, None));

    for _ in 0..100 {
        tree.insert(nanoid::nanoid!(), "$", 0);
        tree.flush_active_memtable(0)?;
        tree.compact(compaction.clone(), 0)?;
        assert!((0..10).contains(&tree.blob_file_count()));
    }

    Ok(())
}
