use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_weak_delete_queue() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);
    assert_eq!(
        b"a",
        &*tree.first_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    let batch_seqno = seqno.next();
    tree.remove_weak("a", batch_seqno);
    assert_eq!(
        b"b",
        &*tree.first_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("b", batch_seqno);
    assert_eq!(
        b"c",
        &*tree.first_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("c", batch_seqno);
    assert_eq!(
        b"d",
        &*tree.first_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("d", batch_seqno);
    assert_eq!(
        b"e",
        &*tree.first_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("e", batch_seqno);
    assert!(tree.is_empty(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_weak_delete_queue_reverse() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let batch_seqno = seqno.next();
    tree.insert("a", "a", batch_seqno);
    tree.insert("b", "b", batch_seqno);
    tree.insert("c", "c", batch_seqno);
    tree.insert("d", "d", batch_seqno);
    tree.insert("e", "e", batch_seqno);
    assert_eq!(
        b"e",
        &*tree.last_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    let batch_seqno = seqno.next();
    tree.remove_weak("e", batch_seqno);
    assert_eq!(
        b"d",
        &*tree.last_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("d", batch_seqno);
    assert_eq!(
        b"c",
        &*tree.last_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("c", batch_seqno);
    assert_eq!(
        b"b",
        &*tree.last_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("b", batch_seqno);
    assert_eq!(
        b"a",
        &*tree.last_key_value(SeqNo::MAX, None).unwrap().key()?
    );

    tree.remove_weak("a", batch_seqno);
    assert!(tree.is_empty(SeqNo::MAX, None)?);

    Ok(())
}
