use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_fifo_limit() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let tree = lsm_tree::Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    let compaction = Arc::new(lsm_tree::compaction::Fifo::new(10, None));

    // Run enough rounds with a small FIFO size limit to repeatedly trigger
    // FIFO drops and blob GC, so the invariant (blob_file_count < 10) is
    // exercised on every iteration.
    for _ in 0..20 {
        tree.insert(nanoid::nanoid!(), "$", 0);
        tree.flush_active_memtable(0)?;
        tree.compact(compaction.clone(), 0)?;
        assert!((0..10).contains(&tree.blob_file_count()));
    }

    Ok(())
}
