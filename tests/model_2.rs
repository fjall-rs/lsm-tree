// Found by model testing

use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, Result, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn model_2() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    let value = b"hellohello";

    tree.insert("a", value, 3);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    tree.insert("b", value, 4);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(2, tree.blob_file_count());

    tree.insert("a", value, 5);
    tree.flush_active_memtable(0)?;
    tree.compact(compaction.clone(), 0)?;
    assert_eq!(3, tree.table_count());
    assert_eq!(3, tree.blob_file_count());

    tree.drop_range::<&[u8], _>(..)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
