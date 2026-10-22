use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, MemoryVersionSetProvider, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_approx_len_sealed() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    let batch_seqno = seqno.next();
    tree.insert("a", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    version_set_provider.rotate_active_memtable();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    Ok(())
}

#[test]
fn tree_approx_len_sealed_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    let batch_seqno = seqno.next();
    tree.insert("a", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    version_set_provider.rotate_active_memtable();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    Ok(())
}

#[test]
fn tree_approx_len() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    let batch_seqno = seqno.next();
    tree.insert("a", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    tree.insert("a", "", seqno.next());

    // Approximate count diverges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 3);

    tree.remove("a", seqno.next());

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    tree.remove("b", seqno.next());

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        5,
    )?;

    // Approximate count converges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    Ok(())
}

#[test]
fn tree_approx_len_blob() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    let batch_seqno = seqno.next();
    tree.insert("a", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", batch_seqno);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    tree.insert("a", "", seqno.next());

    // Approximate count diverges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 3);

    tree.remove("a", seqno.next());

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    tree.remove("b", seqno.next());

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    version_set_provider.compact(
        &tree,
        Arc::new(lsm_tree::compaction::Major::default()),
        seqno.next(),
        5,
    )?;

    // Approximate count converges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    Ok(())
}
