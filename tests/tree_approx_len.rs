use lsm_tree::{AbstractTree, Config, SeqNo};
use tempfile::tempdir;
use test_log::test;

#[test]
fn tree_approx_len_sealed() -> lsm_tree::Result<()> {
    let folder = tempdir()?;

    let tree = Config::new(folder).open()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    tree.insert("a", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    let _ = tree.rotate_memtable().unwrap();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    Ok(())
}

#[test]
fn tree_approx_len_sealed_blob() -> lsm_tree::Result<()> {
    let folder = tempdir()?;

    let tree = Config::new(folder).open_as_blob_tree()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    tree.insert("a", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    let _ = tree.rotate_memtable().unwrap();

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    Ok(())
}

#[test]
fn tree_approx_len() -> lsm_tree::Result<()> {
    let folder = tempdir()?;

    let tree = Config::new(folder).open()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    tree.insert("a", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    tree.insert("a", "", 1);

    // Approximate count diverges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 3);

    tree.remove("a", 2);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    tree.remove("b", 4);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    tree.major_compact(u64::MAX, 5)?;

    // Approximate count converges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    Ok(())
}

#[test]
fn tree_approx_len_blob() -> lsm_tree::Result<()> {
    let folder = tempdir()?;

    let tree = Config::new(folder).open_as_blob_tree()?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    tree.insert("a", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 1);

    tree.insert("b", "", 0);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 2);

    tree.insert("a", "", 1);

    // Approximate count diverges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 2);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 3);

    tree.remove("a", 2);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 1);
    assert!(!tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 4);

    tree.remove("b", 4);

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    tree.flush_active_memtable(0)?;

    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 5);

    tree.index.major_compact(u64::MAX, 5)?;

    // Approximate count converges
    assert_eq!(tree.len(SeqNo::MAX, None)?, 0);
    assert!(tree.is_empty(SeqNo::MAX, None)?);
    assert_eq!(tree.approximate_len(), 0);

    Ok(())
}
