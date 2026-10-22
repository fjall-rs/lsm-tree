// Found by model testing

use lsm_tree::{
    get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider, Result,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn model_2() -> Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(5)))
        .open()?;
    let compaction = Arc::new(lsm_tree::compaction::Leveled::default());

    let value = b"hellohello";

    tree.insert("a", value, seqno.next());
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    version_set_provider.compact(&tree, compaction.clone(), seqno.next(), 0)?;
    assert_eq!(1, tree.table_count());
    assert_eq!(1, tree.blob_file_count());

    tree.insert("b", value, seqno.next());
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    version_set_provider.compact(&tree, compaction.clone(), seqno.next(), 0)?;
    assert_eq!(2, tree.table_count());
    assert_eq!(2, tree.blob_file_count());

    tree.insert("a", value, seqno.next());
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    version_set_provider.compact(&tree, compaction.clone(), seqno.next(), 0)?;
    assert_eq!(3, tree.table_count());
    assert_eq!(3, tree.blob_file_count());

    version_set_provider.drop_range::<&[u8], _>(&tree, .., seqno.next(), 0)?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    Ok(())
}
