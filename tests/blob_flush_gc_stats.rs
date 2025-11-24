use lsm_tree::{get_tmp_folder, AbstractTree, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn blob_tree_flush_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.insert("big", &new_big_value, 1);

        tree.flush_active_memtable(1_000)?;

        // NOTE: The first big_value is dropped, so it never arrives in a blob file
        assert_eq!(2, tree.approximate_len());
    }

    Ok(())
}

#[test]
fn blob_tree_flush_gc_stats_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(128_000);

    {
        let tree = lsm_tree::Config::new(path, SequenceNumberCounter::default())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        tree.insert("big", &big_value, 0);
        tree.insert("smol", "small value", 0);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.remove("big", 1);

        tree.flush_active_memtable(1_000)?;

        // NOTE: The first big_value is dropped, so it never arrives in a blob file
        assert_eq!(2, tree.approximate_len());
    }

    Ok(())
}
