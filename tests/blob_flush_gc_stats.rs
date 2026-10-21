use lsm_tree::{
    get_tmp_folder, AbstractTree, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_flush_gc_stats() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(128_000);
    let new_big_value = b"winter!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        let batch_seqno = seqno.next();
        tree.insert("big", &big_value, batch_seqno);
        tree.insert("smol", "small value", batch_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.insert("big", &new_big_value, seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 1_000)?;

        // NOTE: The first big_value is dropped, so it never arrives in a blob file
        assert_eq!(2, tree.approximate_len());
    }

    Ok(())
}

#[test]
fn blob_tree_flush_gc_stats_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let seqno = SequenceNumberCounter::default();
    let big_value = b"neptune!".repeat(128_000);

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(tree.get("big", SeqNo::MAX)?.is_none());
        let batch_seqno = seqno.next();
        tree.insert("big", &big_value, batch_seqno);
        tree.insert("smol", "small value", batch_seqno);

        let value = tree.get("big", SeqNo::MAX)?.expect("should exist");
        assert_eq!(&*value, big_value);

        tree.remove("big", seqno.next());

        version_set_provider.flush_active_memtable(&tree, seqno.next(), 1_000)?;

        // NOTE: The first big_value is dropped, so it never arrives in a blob file
        assert_eq!(2, tree.approximate_len());
    }

    Ok(())
}
