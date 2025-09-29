use lsm_tree::{AbstractTree, SeqNo};
use std::sync::Arc;
use test_log::test;

#[test]
fn tree_flush_eviction_1() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should not evict weak tombstone
    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn tree_flush_eviction_2() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove_weak("a", 1);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should evict old value, thus weak tombstone too
    tree.flush_active_memtable(1)?;
    assert_eq!(0, tree.segment_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
#[ignore]
fn tree_flush_eviction_3() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove("a", 1);
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should evict old value, but tombstone should stay until last level
    tree.flush_active_memtable(1)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    // NOTE: Should evict tombstone because last level
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), 0)?;
    assert_eq!(0, tree.segment_count());
    assert_eq!(0, tree.len(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
#[ignore]
fn tree_flush_eviction_4() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let path = folder.path();

    let tree = lsm_tree::Config::new(path).open()?;

    tree.insert("a", "a", 0);
    tree.remove("a", 1);
    tree.insert("a", "a", 2);
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);

    // NOTE: Tombstone should stay because of seqno threshold
    tree.flush_active_memtable(1)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert_eq!(
        1,
        tree.manifest()
            .read()
            .expect("lock is poisoned")
            .current_version()
            .level(0)
            .expect("should exist")
            .first()
            .expect("should have at least 1 run")
            .first()
            .expect("should have one segment")
            .metadata
            .tombstone_count
    );

    // NOTE: Should evict tombstone because last level
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), 0)?;
    assert_eq!(1, tree.segment_count());
    assert_eq!(1, tree.len(SeqNo::MAX, None)?);
    assert_eq!(
        0,
        tree.manifest()
            .read()
            .expect("lock is poisoned")
            .current_version()
            .level(6)
            .expect("should exist")
            .first()
            .expect("should have at least 1 run")
            .first()
            .expect("should have one segment")
            .metadata
            .tombstone_count
    );

    Ok(())
}
