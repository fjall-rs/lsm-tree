use lsm_tree::AbstractTree;
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_flush_eviction_1() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(None, None)?);

    // NOTE: Should not evict weak tombstone
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(0, tree.len(None, None)?);

    Ok(())
}

#[test]
fn tree_flush_eviction_2() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(None, None)?);

    // NOTE: Should evict old value, thus weak tombstone too
    tree.flush_active_memtable(1)?;
    assert_eq!(0, tree.segment_count());
    assert_eq!(0, tree.len(None, None)?);

    Ok(())
}

#[test]
fn tree_flush_eviction_3() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove("a", 1);
    assert_eq!(0, tree.len(None, None)?);

    // NOTE: Should evict old value, but tombstone should stay until last level
    tree.flush_active_memtable(1)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(0, tree.len(None, None)?);

    // NOTE: Should evict tombstone because last level
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), 0)?;
    assert_eq!(0, tree.segment_count());
    assert_eq!(0, tree.len(None, None)?);

    Ok(())
}

#[test]
fn tree_flush_eviction_4() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove("a", 1);
    tree.insert("a", "a", 2);
    assert_eq!(1, tree.len(None, None)?);

    // NOTE: Tombstone should stay because of seqno threshold
    tree.flush_active_memtable(1)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(1, tree.len(None, None)?);
    assert_eq!(
        1,
        tree.levels
            .read()
            .unwrap()
            .levels
            .first()
            .unwrap()
            .first()
            .unwrap()
            .tombstone_count()
    );

    // NOTE: Should evict tombstone because last level
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), 0)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(1, tree.len(None, None)?);
    assert_eq!(
        0,
        tree.levels
            .read()
            .unwrap()
            .levels
            .last()
            .unwrap()
            .first()
            .unwrap()
            .tombstone_count()
    );

    Ok(())
}
