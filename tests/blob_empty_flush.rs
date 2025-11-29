use lsm_tree::{get_tmp_folder, AbstractTree, SequenceNumberCounter};
use test_log::test;

#[test]
fn blob_tree_flush_empty() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let medium_value = b"a".repeat(500);

    let tree = lsm_tree::Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(Default::default()))
    .open()?;

    tree.insert("med", &medium_value, 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(1, tree.table_count());
    assert_eq!(0, tree.blob_file_count());

    // Blob writer should have cleaned up blob file because it was empty
    let blob_file_count_on_disk = std::fs::read_dir(path.join("blobs"))?.flatten().count();
    assert_eq!(0, blob_file_count_on_disk);

    Ok(())
}
