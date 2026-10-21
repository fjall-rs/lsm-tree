use lsm_tree::{
    get_tmp_folder, AbstractTree, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn blob_tree_recovery() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    let big_value = b"neptune!".repeat(4_096);

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        tree.insert("big", &big_value, seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        tree.insert("big", &big_value, seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        tree.insert("big", &big_value, seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        tree.insert("big", &big_value, seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        tree.insert("big", &big_value, seqno.next());
        version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

        assert!(!tree.is_empty(SeqNo::MAX, None)?);
        assert_eq!(5, tree.blob_file_count());
    }

    {
        let tree = lsm_tree::Config::new(path, version_set_provider.clone())
            .with_kv_separation(Some(Default::default()))
            .open()?;

        assert!(!tree.is_empty(SeqNo::MAX, None)?);
        assert_eq!(5, tree.blob_file_count());
    }

    Ok(())
}
