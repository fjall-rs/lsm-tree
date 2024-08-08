use lsm_tree::AbstractTree;
use test_log::test;

#[test]
fn blob_tree_separation_threshold() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path)
        .blob_file_separation_threshold(1_024)
        .open_as_blob_tree()?;

    tree.insert("a", "a".repeat(1_023), 0);
    tree.flush_active_memtable()?;
    assert_eq!(tree.blobs.segment_count(), 0);

    tree.insert("b", "b".repeat(1_024), 0);
    tree.flush_active_memtable()?;
    assert_eq!(tree.blobs.segment_count(), 1);

    assert_eq!(2, tree.len()?);

    Ok(())
}
