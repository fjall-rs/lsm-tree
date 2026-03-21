// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Regression tests for issue #52: L0 MVCC stale reads caused by L0 run
//! reordering, with or without an active memtable.
//!
//! When optimize_runs merges disjoint SSTs from different temporal epochs into
//! one run, the point read path may find a stale entry from an older run before
//! reaching the newer entry in a merged run, depending on how L0 runs are
//! ordered and probed. These tests (both with and without active memtable
//! data) verify that get_internal_entry_from_tables always returns the entry
//! with the highest visible seqno across all L0 runs.

use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

/// Exact reproducer from issue #52.
///
/// 3 SSTs where key=0 is overwritten across SST-2 and SST-3, plus an active
/// memtable with data for a *different* key. The bug: point read for key=0
/// returns the stale SST-2 value instead of the newer SST-3 value.
#[test]
fn regression_overwrite_across_ssts() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // SST-1: key=2@1
    tree.insert(vec![2u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key=1@2, key=0@3, key=2@4
    tree.insert(vec![1u8], vec![0u8], 2);
    tree.insert(vec![0u8], vec![0u8], 3);
    tree.insert(vec![2u8], vec![0u8], 4);
    tree.flush_active_memtable(0)?;

    // SST-3: key=0@5 val=1 (newer version of key=0)
    tree.insert(vec![0u8], vec![1u8], 5);
    tree.flush_active_memtable(0)?;

    // Active memtable: key=1@6, key=1@7 (different key!)
    tree.insert(vec![1u8], vec![0u8], 6);
    tree.insert(vec![1u8], vec![0u8], 7);

    // Must return SST-3's value [1], not SST-2's stale value [0]
    let result = tree.get(&[0u8], 8)?;
    assert_eq!(
        result,
        Some(vec![1u8].into()),
        "key=0 should return newest value [1] from SST-3, got stale value from SST-2"
    );

    Ok(())
}

/// Same scenario with different data layout — 3 SSTs plus active memtable.
#[test]
fn regression_three_ssts_plus_memtable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // SST-1: key="a"@1
    tree.insert("a", "v1", 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key="a"@2, key="b"@3
    tree.insert("a", "v2", 2);
    tree.insert("b", "b1", 3);
    tree.flush_active_memtable(0)?;

    // SST-3: key="a"@4 (newest version)
    tree.insert("a", "v3", 4);
    tree.flush_active_memtable(0)?;

    // Active memtable: key="c"@5 (different key, triggers the bug)
    tree.insert("c", "c1", 5);

    assert_eq!(
        &*tree.get("a", 6)?.expect("key=a should exist at seqno 6"),
        b"v3",
        "key=a should return newest value v3 from SST-3"
    );

    Ok(())
}

/// Confirms that 2-SST case works (no bug with only 2 SSTs).
#[test]
fn regression_two_ssts_same_key() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // SST-1: key=0@1 val=0
    tree.insert(vec![0u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key=0@2 val=1 (newer)
    tree.insert(vec![0u8], vec![1u8], 2);
    tree.flush_active_memtable(0)?;

    // Active memtable: key=1@3
    tree.insert(vec![1u8], vec![0u8], 3);

    let result = tree.get(&[0u8], 4)?;
    assert_eq!(result, Some(vec![1u8].into()));

    Ok(())
}

/// Confirms that 3 SSTs without active memtable data works.
#[test]
fn regression_three_ssts_overwrite() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // SST-1: key=2@1
    tree.insert(vec![2u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key=1@2, key=0@3, key=2@4
    tree.insert(vec![1u8], vec![0u8], 2);
    tree.insert(vec![0u8], vec![0u8], 3);
    tree.insert(vec![2u8], vec![0u8], 4);
    tree.flush_active_memtable(0)?;

    // SST-3: key=0@5 val=1
    tree.insert(vec![0u8], vec![1u8], 5);
    tree.flush_active_memtable(0)?;

    // No active memtable data
    let result = tree.get(&[0u8], 6)?;
    assert_eq!(
        result,
        Some(vec![1u8].into()),
        "key=0 should return newest value [1] from SST-3"
    );

    Ok(())
}

/// Point tombstone in a newer SST must suppress a value in an older SST,
/// even when optimize_runs reorders them.
#[test]
fn regression_tombstone_across_reordered_runs() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // SST-1: key=2@1
    tree.insert(vec![2u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key=0@2 val=42, key=2@3
    tree.insert(vec![0u8], vec![42u8], 2);
    tree.insert(vec![2u8], vec![0u8], 3);
    tree.flush_active_memtable(0)?;

    // SST-3: delete key=0@4
    tree.remove(vec![0u8], 4);
    tree.flush_active_memtable(0)?;

    // Active memtable: key=1@5
    tree.insert(vec![1u8], vec![0u8], 5);

    // key=0 was deleted at seqno=4, must return None
    let result = tree.get(&[0u8], 6)?;
    assert_eq!(
        result, None,
        "key=0 should be deleted by tombstone from SST-3"
    );

    Ok(())
}

/// MVCC snapshot reads must also be correct across reordered L0 runs.
#[test]
fn regression_mvcc_snapshot_across_reordered_runs() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // SST-1: key=2@1
    tree.insert(vec![2u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key=0@2 val=0, key=1@3, key=2@4
    tree.insert(vec![0u8], vec![0u8], 2);
    tree.insert(vec![1u8], vec![0u8], 3);
    tree.insert(vec![2u8], vec![0u8], 4);
    tree.flush_active_memtable(0)?;

    // SST-3: key=0@5 val=1
    tree.insert(vec![0u8], vec![1u8], 5);
    tree.flush_active_memtable(0)?;

    // Active memtable: key=1@6
    tree.insert(vec![1u8], vec![0u8], 6);

    // Snapshot at seqno=3: should see key=0@2 val=0 (seqno 5 not visible)
    assert_eq!(
        tree.get(&[0u8], 3)?,
        Some(vec![0u8].into()),
        "snapshot@3 should see old value"
    );

    // Snapshot at seqno=6: should see key=0@5 val=1
    assert_eq!(
        tree.get(&[0u8], 6)?,
        Some(vec![1u8].into()),
        "snapshot@6 should see new value"
    );

    Ok(())
}
