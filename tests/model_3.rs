// Found by model testing

use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, Result, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn model_3() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    let value = b"hellohello";

    tree.insert("a", value, 1);
    tree.insert("i", value, 1);
    tree.flush_active_memtable(0)?;

    tree.insert("a", value, 2);
    tree.insert("f", value, 2);
    tree.flush_active_memtable(0)?;

    tree.insert("a", value, 3);
    tree.insert("h", value, 3);
    tree.flush_active_memtable(0)?;

    tree.insert("a", value, 4);
    tree.insert("b", value, 4);
    tree.flush_active_memtable(0)?;

    tree.insert("c", value, 5);
    tree.insert("g", value, 5);
    tree.flush_active_memtable(0)?;

    tree.insert("b", value, 6);
    tree.insert("c", value, 6);
    tree.insert("d", value, 6);
    tree.insert("e", value, 6);
    tree.flush_active_memtable(15)?;
    tree.compact(compaction.clone(), 41)?;

    tree.insert("a", value, 7);
    tree.flush_active_memtable(16)?;

    tree.insert("a", value, 8);
    tree.flush_active_memtable(17)?;

    tree.insert("a", value, 9);
    tree.flush_active_memtable(18)?;

    tree.insert("a", value, 10);
    tree.flush_active_memtable(19)?;
    tree.compact(compaction.clone(), 19)?;

    tree.drop_range::<&[u8], _>(..)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
