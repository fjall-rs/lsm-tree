use lsm_tree::{AbstractTree, Config, SeqNo, Tree};
use std::ops::Bound::{Excluded, Included, Unbounded};

fn populate_segments(tree: &Tree) -> lsm_tree::Result<()> {
    for key in 'a'..='e' {
        tree.insert([key as u8], "", 0);
        tree.flush_active_memtable(0)?;
    }
    Ok(())
}

#[test]
fn tree_drop_range_basic() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(5, tree.segment_count());

    tree.drop_range("a"..="c")?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(2, tree.segment_count());

    Ok(())
}

#[test]
fn tree_drop_range_partial_segment_overlap_kept() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    for key in ['a', 'b', 'c', 'd', 'e'] {
        tree.insert([key as u8], "", 0);
    }
    tree.flush_active_memtable(0)?;

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(1, tree.segment_count());

    tree.drop_range("b".."d")?;

    for key in ['a', 'b', 'c', 'd', 'e'] {
        assert!(tree.contains_key([key as u8], SeqNo::MAX)?);
    }

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(1, tree.segment_count());

    Ok(())
}

#[test]
fn tree_drop_range_upper_exclusive() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range("a".."d")?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_lower_exclusive() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range::<&str, _>((Excluded("a"), Included("c")))?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_lower_inclusive_upper() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range::<&str, _>((Unbounded, Included("c")))?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_lower_exclusive_upper() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range::<&str, _>((Unbounded, Excluded("d")))?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_exclusive_empty_interval() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range::<&str, _>((Excluded("b"), Excluded("b")))?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("b", SeqNo::MAX)?);
    assert!(tree.contains_key("c", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_empty_tree() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    tree.drop_range("a"..="c")?;

    assert_eq!(0, tree.l0_run_count());
    assert_eq!(0, tree.segment_count());

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_upper() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range("c"..)?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(!tree.contains_key("d", SeqNo::MAX)?);
    assert!(!tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_clear_all() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range::<&str, _>(..)?;

    assert_eq!(0, tree.l0_run_count());
    assert_eq!(0, tree.segment_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_inverted_bounds_is_noop() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = Config::new(&folder).open()?;

    populate_segments(&tree)?;

    tree.drop_range("c".."a")?;
    tree.drop_range("c"..="a")?;

    // All keys remain because the range is treated as empty.
    for key in 'a'..='e' {
        assert!(tree.contains_key([key as u8], SeqNo::MAX)?);
    }

    Ok(())
}
