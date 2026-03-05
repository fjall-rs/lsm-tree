use lsm_tree::{get_tmp_folder, AbstractTree, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn blob_tree_recovery() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(4_096);

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(Default::default()))
        .open()?;

        tree.insert("big", &big_value, 0);
        tree.flush_active_memtable(0)?;

        tree.insert("big", &big_value, 0);
        tree.flush_active_memtable(0)?;

        tree.insert("big", &big_value, 0);
        tree.flush_active_memtable(0)?;

        tree.insert("big", &big_value, 0);
        tree.flush_active_memtable(0)?;

        tree.insert("big", &big_value, 0);
        tree.flush_active_memtable(0)?;
    }

    {
        let tree = lsm_tree::Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .with_kv_separation(Some(Default::default()))
        .open()?;

        assert!(!tree.is_empty(SeqNo::MAX, None)?);
    }

    Ok(())
}
