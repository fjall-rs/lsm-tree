use lsm_tree::{AbstractTree, Config};
use test_log::test;

#[test]
fn tree_write_and_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?.keep();

    let tree = Config::new(folder.clone()).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), 0);
    tree.insert("b".as_bytes(), nanoid::nanoid!().as_bytes(), 1);
    tree.insert("c".as_bytes(), nanoid::nanoid!().as_bytes(), 2);

    let item = tree.get_internal_entry(b"a", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "a".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 0);

    let item = tree.get_internal_entry(b"b", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "b".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 1);

    let item = tree.get_internal_entry(b"c", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "c".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 2);

    tree.flush_active_memtable(0)?;

    let tree = Config::new(folder).open()?;

    let item = tree.get_internal_entry(b"a", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "a".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 0);

    let item = tree.get_internal_entry(b"b", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "b".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 1);

    let item = tree.get_internal_entry(b"c", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "c".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 2);

    Ok(())
}
