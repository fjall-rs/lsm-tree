use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};
use test_log::test;

#[test]
fn tree_contains_prefix_empty_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    assert!(!tree.contains_prefix("abc", SeqNo::MAX, None)?);
    assert!(!tree.contains_prefix("", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_contains_prefix_basic() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("abc:1", "value1", 0);
    tree.insert("abc:2", "value2", 1);
    tree.insert("def:1", "value3", 2);

    assert!(tree.contains_prefix("abc", 3, None)?);
    assert!(tree.contains_prefix("def", 3, None)?);
    assert!(!tree.contains_prefix("xyz", 3, None)?);
    assert!(!tree.contains_prefix("ab", 0, None)?);

    Ok(())
}

#[test]
fn tree_contains_prefix_no_match() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("abc", "value", 0);
    tree.insert("abd", "value", 1);

    assert!(!tree.contains_prefix("xyz", 2, None)?);
    assert!(!tree.contains_prefix("abe", 2, None)?);
    assert!(!tree.contains_prefix("abca", 2, None)?);

    Ok(())
}

#[test]
fn tree_contains_prefix_mvcc() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert at seqno 4
    tree.insert("abc:1", "value", 4);

    // Not visible at seqno 3 (seqno filter is item_seqno < query_seqno)
    assert!(!tree.contains_prefix("abc", 3, None)?);

    // Not visible at seqno 4 (strict less-than)
    assert!(!tree.contains_prefix("abc", 4, None)?);

    // Visible at seqno 5
    assert!(tree.contains_prefix("abc", 5, None)?);

    // Visible at MAX
    assert!(tree.contains_prefix("abc", SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_contains_prefix_after_delete() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("abc:1", "value", 0);
    tree.remove("abc:1", 1);

    // After deletion, prefix should not match
    assert!(!tree.contains_prefix("abc", 2, None)?);

    // But at seqno 1 (before delete), it should still be visible
    assert!(tree.contains_prefix("abc", 1, None)?);

    Ok(())
}

#[test]
fn tree_contains_prefix_after_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("abc:1", "value1", 0);
    tree.insert("abc:2", "value2", 1);
    tree.flush_active_memtable(0)?;

    assert!(tree.contains_prefix("abc", 2, None)?);
    assert!(!tree.contains_prefix("xyz", 2, None)?);

    Ok(())
}
