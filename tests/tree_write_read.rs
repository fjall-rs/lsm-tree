use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_write_and_read() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("b".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("c".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    let item = tree.get_internal_entry(b"a", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "a".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 0);

    let item = tree.get_internal_entry(b"b", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "b".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 1);

    let item = tree.get_internal_entry(b"c", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "c".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 2);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    drop(tree);

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let item = tree.get_internal_entry(b"a", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "a".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 0);

    let item = tree.get_internal_entry(b"b", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "b".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 1);

    let item = tree.get_internal_entry(b"c", SeqNo::MAX)?.unwrap();
    assert_eq!(&*item.key.user_key, "c".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 2);

    Ok(())
}
