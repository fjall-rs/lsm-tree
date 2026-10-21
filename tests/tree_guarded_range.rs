use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_guarded_range() -> lsm_tree::Result<()> {
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

    assert_eq!(
        2,
        tree.range("a"..="f", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );
    assert_eq!(
        2,
        tree.range("f"..="g", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );

    Ok(())
}

#[test]
fn blob_tree_guarded_range() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    tree.insert("a".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("f".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());
    tree.insert("g".as_bytes(), nanoid::nanoid!().as_bytes(), seqno.next());

    assert_eq!(
        2,
        tree.range("a"..="f", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );
    assert_eq!(
        2,
        tree.range("f"..="g", SeqNo::MAX, None)
            .flat_map(Guard::key)
            .count(),
    );

    Ok(())
}
