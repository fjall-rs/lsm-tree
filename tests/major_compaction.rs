use lsm_tree::{AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = Config::new(path).open()?;

    let seqno = SequenceNumberCounter::default();

    tree.insert("a".as_bytes(), "abc", seqno.next());
    tree.insert("b".as_bytes(), "abc", seqno.next());
    tree.insert("c".as_bytes(), "abc", seqno.next());

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());

    tree.major_compact(u64::MAX, 1_000 /* NOTE: Simulate some time passing */)?;
    assert_eq!(1, tree.segment_count());

    let item = tree.get_internal_entry("a", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "a".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 0);

    let item = tree.get_internal_entry("b", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "b".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 1);

    let item = tree.get_internal_entry("c", None)?.unwrap();
    assert_eq!(&*item.key.user_key, "c".as_bytes());
    assert!(!item.is_tombstone());
    assert_eq!(item.key.seqno, 2);

    assert_eq!(1, tree.segment_count());
    assert_eq!(3, tree.len()?);

    let batch_seqno = seqno.next();
    tree.remove("a".as_bytes(), batch_seqno);
    tree.remove("b".as_bytes(), batch_seqno);
    tree.remove("c".as_bytes(), batch_seqno);

    tree.flush_active_memtable(0)?;
    assert_eq!(2, tree.segment_count());

    tree.major_compact(u64::MAX, 1_000 /* NOTE: Simulate some time passing */)?;

    assert_eq!(0, tree.segment_count());
    assert_eq!(0, tree.len()?);

    Ok(())
}
