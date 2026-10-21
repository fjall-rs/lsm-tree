use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_major_compaction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    let seqno = SequenceNumberCounter::default();

    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(path, version_set_provider.clone()).open()?;

    tree.insert("a".as_bytes(), "abc", seqno.next());
    tree.insert("b".as_bytes(), "abc", seqno.next());
    tree.insert("c".as_bytes(), "abc", seqno.next());

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;
    assert_eq!(1, tree.table_count());

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        1_000, // NOTE: Simulate some time passing
    )?;
    assert_eq!(1, tree.table_count());

    let item = tree.get_internal_entry(b"a", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "a".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 0);

    let item = tree.get_internal_entry(b"b", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "b".as_bytes());
    assert!(!item.is_tombstone());
    // assert_eq!(item.key.seqno, 0); // NOTE: Seqno is zeroed because below GC threshold // TODO:

    let item = tree.get_internal_entry(b"c", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "c".as_bytes());
    assert!(!item.is_tombstone());
    // assert_eq!(item.key.seqno, 0); // NOTE: Seqno is zeroed because below GC threshold // TODO:

    assert_eq!(1, tree.table_count());
    assert_eq!(3, tree.len(SeqNo::MAX, None)?);

    let batch_seqno = seqno.next();
    tree.remove("a".as_bytes(), batch_seqno);
    tree.remove("b".as_bytes(), batch_seqno);
    tree.remove("c".as_bytes(), batch_seqno);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.table_count());

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        10_000, // NOTE: Simulate some time passing
    )?;

    assert_eq!(0, tree.table_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}
