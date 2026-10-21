use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_range_count() -> lsm_tree::Result<()> {
    use std::ops::Bound::{self, Excluded, Unbounded};

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    assert_eq!(2, tree.range("a"..="f", SeqNo::MAX, None).count());
    assert_eq!(2, tree.range("f"..="g", SeqNo::MAX, None).count());

    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>(
            (Excluded("f".into()), Unbounded),
            SeqNo::MAX,
            None
        )
        .count()
    );

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.range("a"..="f", SeqNo::MAX, None).count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>(
            (Excluded("f".into()), Unbounded),
            SeqNo::MAX,
            None
        )
        .count()
    );

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    assert_eq!(2, tree.range("a"..="f", SeqNo::MAX, None).count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>(
            (Excluded("f".into()), Unbounded),
            SeqNo::MAX,
            None
        )
        .count()
    );

    Ok(())
}

#[test]
fn blob_tree_range_count() -> lsm_tree::Result<()> {
    use std::ops::Bound::{self, Excluded, Unbounded};

    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), b"neptune!".repeat(128_000), seqno.next());

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), b"neptune!".repeat(128_000), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    assert_eq!(2, tree.range("a"..="f", SeqNo::MAX, None).count());
    assert_eq!(2, tree.range("f"..="g", SeqNo::MAX, None).count());

    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>(
            (Excluded("f".into()), Unbounded),
            SeqNo::MAX,
            None
        )
        .count()
    );

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(2, tree.range("a"..="f", SeqNo::MAX, None).count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>(
            (Excluded("f".into()), Unbounded),
            SeqNo::MAX,
            None
        )
        .count()
    );

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    assert_eq!(2, tree.range("a"..="f", SeqNo::MAX, None).count());
    assert_eq!(
        1,
        tree.range::<Vec<u8>, (Bound<Vec<u8>>, Bound<Vec<u8>>)>(
            (Excluded("f".into()), Unbounded),
            SeqNo::MAX,
            None
        )
        .count()
    );

    Ok(())
}
