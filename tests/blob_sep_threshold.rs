use lsm_tree::{get_tmp_folder, AbstractTree, KvSeparationOptions, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn blob_tree_separation_threshold() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let tree = lsm_tree::Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        KvSeparationOptions::default().separation_threshold(1_024),
    ))
    .open()?;

    tree.insert("a", "a".repeat(1_023), 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(0, tree.blob_file_count());

    tree.insert("b", "b".repeat(1_024), 0);
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.blob_file_count());

    assert_eq!(2, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
