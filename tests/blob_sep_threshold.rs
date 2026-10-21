use lsm_tree::{
    get_tmp_folder, AbstractTree, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_separation_threshold() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(
            KvSeparationOptions::default().separation_threshold(1_024),
        ))
        .open()?;

    tree.insert("a", "a".repeat(1_023), 0);

    version_set_provider.flush_active_memtable(&tree, 1, 0)?;

    assert_eq!(0, tree.blob_file_count());

    tree.insert("b", "b".repeat(1_024), 2);

    version_set_provider.flush_active_memtable(&tree, 3, 0)?;

    assert_eq!(1, tree.blob_file_count());

    assert_eq!(2, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
