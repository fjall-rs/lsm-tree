// Regression tests for a range/point tombstone bug: point tombstone invisible
// when range tombstone exists in a prior SST.
//
// When `optimize_runs` merges disjoint L0 tables from different flush epochs,
// a newer point tombstone can end up in a run iterated AFTER an older value,
// causing `get()` to return stale data instead of None.
//
// `optimize_runs` is called on every version creation (flush, compaction),
// so flushing multiple memtables is sufficient to trigger the reordering.

use lsm_tree::{get_tmp_folder, AbstractTree, AnyTree, Config, SequenceNumberCounter};
use test_log::test;

fn open_tree(path: &std::path::Path) -> AnyTree {
    Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .expect("should open")
}

/// Baseline: point tombstone across SSTs works without range tombstones.
/// This confirms the basic cross-SST tombstone mechanism is sound.
#[test]
fn baseline_point_tombstone_across_ssts() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // SST-1: insert key=2
    tree.insert(vec![2u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    assert!(
        tree.get(&[2u8], 2)?.is_some(),
        "value should be visible before the point tombstone"
    );

    // SST-2: point tombstone for key=2
    tree.remove(vec![2u8], 2);
    tree.flush_active_memtable(0)?;

    // Point tombstone in newer SST should shadow insert in older SST
    let result = tree.get(&[2u8], 3)?;
    assert_eq!(
        result, None,
        "point tombstone in newer SST should shadow insert"
    );

    Ok(())
}

/// Simpler RT scenario: RT + insert + tombstone with fewer SSTs.
/// RT and insert coexist in same SST, tombstone in next SST.
#[test]
fn regression_rt_same_sst_then_tombstone_in_next() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // SST-1: RT([0,3))@1 + insert key=2@2
    tree.remove_range(&[0u8], &[3u8], 1);
    tree.insert(vec![2u8], vec![0u8], 2);
    tree.flush_active_memtable(0)?;

    assert!(
        tree.get(&[2u8], 3)?.is_some(),
        "insert after the RT should be visible before the later point tombstone"
    );

    // SST-2: point tombstone key=2@3
    tree.remove(vec![2u8], 3);
    tree.flush_active_memtable(0)?;

    let result = tree.get(&[2u8], 4)?;
    assert_eq!(
        result, None,
        "point tombstone should shadow insert even with RT present"
    );

    Ok(())
}

/// Exact reproducer from issue #53.
///
/// When a range tombstone and an insert coexist in one SST, and a point
/// tombstone for the same key is flushed to a newer SST, the point tombstone
/// is not visible during reads. The tree incorrectly returns the inserted
/// value instead of None.
#[test]
fn regression_remove_range_then_insert_then_remove() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // SST-1: key=3@1
    tree.insert(vec![3u8], vec![0u8], 1);
    tree.flush_active_memtable(0)?;

    // SST-2: key=0@2,3,4 + RT([0,3))@5 + key=2@6
    tree.insert(vec![0u8], vec![0u8], 2);
    tree.insert(vec![0u8], vec![0u8], 3);
    tree.insert(vec![0u8], vec![0u8], 4);
    tree.remove_range(&[0u8], &[3u8], 5); // range tombstone
    tree.insert(vec![2u8], vec![0u8], 6); // insert AFTER RT
    tree.flush_active_memtable(0)?;

    assert!(
        tree.get(&[2u8], 7)?.is_some(),
        "insert after the RT should be visible before the final point tombstone"
    );

    // SST-3: point tombstone for key=2@7
    tree.remove(vec![2u8], 7);
    tree.flush_active_memtable(0)?;

    // The point tombstone at seqno 7 should shadow the insert at seqno 6
    let result = tree.get(&[2u8], 8)?;
    assert_eq!(
        result, None,
        "point tombstone at seqno 7 must remain visible and shadow insert at seqno 6, \
         even in the presence of the range tombstone in SST-2"
    );

    Ok(())
}
