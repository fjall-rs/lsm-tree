use lsm_tree::{get_tmp_folder, AbstractTree, MemoryVersionSetProvider, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_flush_empty() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let medium_value = b"a".repeat(500);

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(path, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("med", &medium_value, seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    // Blob writer should have cleaned up blob file because it was empty
    let blob_file_count_on_disk = std::fs::read_dir(path.join("blobs"))?.flatten().count();
    assert_eq!(0, blob_file_count_on_disk);

    Ok(())
}
