use lsm_tree::{AbstractTree, SeqNo};
use test_log::test;

#[test]
fn blob_tree_separation_threshold() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path)
        .blob_file_separation_threshold(1_024)
        .open_as_blob_tree()?;

    tree.insert("a", "a".repeat(1_023), 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(0, tree.blob_file_count());

    tree.insert("b", "b".repeat(1_024), 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blob_file_count());

    assert_eq!(2, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
