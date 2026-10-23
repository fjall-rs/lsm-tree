use lsm_tree::{
    get_tmp_folder, AbstractTree, AnyTree, Config, MemoryVersionSetProvider, Result, SeqNo,
    SequenceNumberCounter,
};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

fn populate_tables(
    tree: &AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
) -> Result<()> {
    for key in 'a'..='e' {
        tree.insert([key as u8], "", seqno.next());
        version_set_provider.flush_active_memtable(tree, seqno.next(), 0)?;
    }
    Ok(())
}

fn drop_range<R: RangeBounds<&'static str>>(
    tree: &AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
    range: R,
) -> Result<()> {
    version_set_provider.drop_range(tree, range, seqno.next(), 0)
}

#[test]
fn tree_drop_range_basic() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(5, tree.table_count());

    drop_range(&tree, &version_set_provider, &seqno, "a"..="c")?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(2, tree.table_count());

    Ok(())
}

#[test]
fn tree_drop_range_partial_table_overlap_kept() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for key in ['a', 'b', 'c', 'd', 'e'] {
        tree.insert([key as u8], "", seqno.next());
    }
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(1, tree.table_count());

    drop_range(&tree, &version_set_provider, &seqno, "b".."d")?;

    for key in ['a', 'b', 'c', 'd', 'e'] {
        assert!(tree.contains_key([key as u8], SeqNo::MAX)?);
    }

    assert_eq!(1, tree.l0_run_count());
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn tree_drop_range_upper_exclusive() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(&tree, &version_set_provider, &seqno, "a".."d")?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_lower_exclusive() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(
        &tree,
        &version_set_provider,
        &seqno,
        (Excluded("a"), Included("c")),
    )?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_lower_inclusive_upper() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(
        &tree,
        &version_set_provider,
        &seqno,
        (Unbounded, Included("c")),
    )?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);
    assert!(tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_lower_exclusive_upper() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(
        &tree,
        &version_set_provider,
        &seqno,
        (Unbounded, Excluded("d")),
    )?;

    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(tree.contains_key("d", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_exclusive_empty_interval() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(
        &tree,
        &version_set_provider,
        &seqno,
        (Excluded("b"), Excluded("b")),
    )?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("b", SeqNo::MAX)?);
    assert!(tree.contains_key("c", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_empty_tree() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    drop_range(&tree, &version_set_provider, &seqno, "a"..="c")?;

    assert_eq!(0, tree.l0_run_count());
    assert_eq!(0, tree.table_count());

    Ok(())
}

#[test]
fn tree_drop_range_unbounded_upper() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(&tree, &version_set_provider, &seqno, "c"..)?;

    assert!(tree.contains_key("a", SeqNo::MAX)?);
    assert!(tree.contains_key("b", SeqNo::MAX)?);
    assert!(!tree.contains_key("c", SeqNo::MAX)?);
    assert!(!tree.contains_key("d", SeqNo::MAX)?);
    assert!(!tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_clear_all() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;
    drop_range(&tree, &version_set_provider, &seqno, ..)?;

    assert_eq!(0, tree.l0_run_count());
    assert_eq!(0, tree.table_count());
    assert!(!tree.contains_key("a", SeqNo::MAX)?);
    assert!(!tree.contains_key("e", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn tree_drop_range_inverted_bounds_is_noop() -> Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    populate_tables(&tree, &version_set_provider, &seqno)?;

    drop_range(&tree, &version_set_provider, &seqno, "c".."a")?;
    drop_range(&tree, &version_set_provider, &seqno, "c"..="a")?;

    // All keys remain because the range is treated as empty.
    for key in 'a'..='e' {
        assert!(tree.contains_key([key as u8], SeqNo::MAX)?);
    }

    Ok(())
}
