use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn memory_provider_clear_appends_empty_version() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "a", seqno.next());
    let snapshot_seqno = seqno.get();
    let history_len = tree.version_set_len();

    version_set_provider.clear(&tree, seqno.next())?;

    assert_eq!(tree.current_version().id(), 1);
    assert_eq!(tree.version_set_len(), history_len + 1);
    assert_eq!(tree.sealed_memtable_count(), 0);
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert_eq!(&*tree.get("a", snapshot_seqno)?.unwrap(), b"a");

    Ok(())
}

#[test]
fn memory_provider_drop_range_registers_compaction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let history_len = tree.version_set_len();
    assert_eq!(tree.table_count(), 1);

    version_set_provider.drop_range::<&[u8], _>(&tree, .., seqno.next(), 0)?;

    assert_eq!(tree.current_version().id(), 2);
    assert_eq!(tree.version_set_len(), history_len + 1);
    assert_eq!(tree.table_count(), 0);
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());

    Ok(())
}
