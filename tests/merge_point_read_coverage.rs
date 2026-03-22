// Tests exercising create_range_point() branches for codecov coverage.
//
// create_range_point is the fast path used by resolve_merge_via_pipeline
// (called from tree.get() when entry is a MergeOperand). These tests
// ensure all major code paths are hit:
//   - Single-table runs with bloom-passing key
//   - Multi-table runs (surviving 0 / 1 / 2+ tables)
//   - Range tombstone collection from bloom-passing tables
//   - Post-merge RT suppression filter
//   - Sealed memtable path

use lsm_tree::{AbstractTree, Config, MergeOperator, SequenceNumberCounter, UserValue};
use std::sync::Arc;
use tempfile::tempdir;

struct CounterMerge;

impl MergeOperator for CounterMerge {
    fn merge(
        &self,
        _key: &[u8],
        base_value: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        let mut counter: i64 = match base_value {
            Some(bytes) if bytes.len() == 8 => {
                i64::from_le_bytes(bytes.try_into().expect("checked"))
            }
            _ => 0,
        };
        for op in operands {
            if op.len() == 8 {
                counter += i64::from_le_bytes((*op).try_into().expect("checked"));
            }
        }
        Ok(counter.to_le_bytes().to_vec().into())
    }
}

fn get_counter(tree: &lsm_tree::AnyTree, key: &str, seqno: u64) -> Option<i64> {
    tree.get(key, seqno)
        .unwrap()
        .map(|v| i64::from_le_bytes((*v).try_into().unwrap()))
}

fn tree_with_merge(folder: &tempfile::TempDir) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .unwrap()
}

/// Single-table run path: base on disk, merge operand in memtable.
/// Exercises the len==1 arm with bloom-passing table.
#[test]
fn point_read_merge_single_table() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand in memtable
    tree.merge("counter", 5_i64.to_le_bytes(), 1);

    assert_eq!(get_counter(&tree, "counter", 2), Some(105));
}

/// Multiple flushed tables with unrelated keys (bloom rejects them).
/// The target key's base + operand should merge correctly despite many tables.
#[test]
fn point_read_merge_bloom_filters_unrelated_tables() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 50_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Unrelated tables (bloom should reject for "counter")
    for i in 0..5 {
        let key = format!("other_{i}");
        tree.insert(key, vec![0u8; 8], i as u64 + 1);
        tree.flush_active_memtable(0).unwrap();
    }

    // Merge operand in memtable
    tree.merge("counter", 7_i64.to_le_bytes(), 10);

    assert_eq!(get_counter(&tree, "counter", 11), Some(57));
}

/// Sealed memtable path: merge operand in sealed (not yet flushed) memtable.
#[test]
fn point_read_merge_sealed_memtable() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 10_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // First merge operand — will be sealed via rotate
    tree.merge("counter", 3_i64.to_le_bytes(), 1);
    tree.rotate_memtable();

    // Second merge operand — in active memtable
    tree.merge("counter", 2_i64.to_le_bytes(), 2);

    assert_eq!(get_counter(&tree, "counter", 3), Some(15));
}

/// Range tombstone suppression: RT kills the base value, merge
/// operand should produce result with no base (pure merge).
#[test]
fn point_read_merge_with_range_tombstone_suppression() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Range tombstone [c, d) at seqno 2 — covers "counter"
    tree.remove_range("c", "d", 2);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand in memtable — base is suppressed by RT,
    // so merge runs with base=None
    tree.merge("counter", 42_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(42));
}

/// Multiple merge operands across disk and memtable.
#[test]
fn point_read_merge_multiple_operands_on_disk() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value
    tree.insert("counter", 10_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // Merge operands on disk
    tree.merge("counter", 1_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0).unwrap();
    tree.merge("counter", 2_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand in memtable
    tree.merge("counter", 3_i64.to_le_bytes(), 3);

    // 10 + 1 + 2 + 3 = 16
    assert_eq!(get_counter(&tree, "counter", 4), Some(16));
}

/// No base value — pure merge operands only.
#[test]
fn point_read_merge_no_base_value() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    tree.merge("counter", 5_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();
    tree.merge("counter", 3_i64.to_le_bytes(), 1);

    assert_eq!(get_counter(&tree, "counter", 2), Some(8));
}

/// Key not present — should return None, not panic.
#[test]
fn point_read_merge_nonexistent_key() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    tree.merge("counter", 5_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    assert_eq!(get_counter(&tree, "missing", 1), None);
}

/// Range tombstone in active memtable (not flushed) suppresses the
/// base value during merge resolution.
#[test]
fn point_read_merge_rt_in_active_memtable() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // RT in active memtable
    tree.remove_range("c", "d", 2);

    // Merge operand in active memtable
    tree.merge("counter", 42_i64.to_le_bytes(), 3);

    // RT suppresses base — result is pure merge from None
    assert_eq!(get_counter(&tree, "counter", 4), Some(42));
}

/// RT in sealed memtable exercises the sealed memtable RT collection
/// path in create_range_point.
#[test]
fn point_read_merge_rt_in_sealed_memtable() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Base value on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0).unwrap();

    // RT in sealed memtable
    tree.remove_range("c", "d", 2);
    tree.rotate_memtable();

    // Merge operand in active memtable
    tree.merge("counter", 42_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(42));
}

/// Tables whose key range does not overlap the target key are skipped
/// during RT collection (exercises the key-range continue path).
#[test]
fn point_read_merge_non_overlapping_tables_skipped() {
    let folder = tempdir().unwrap();
    let tree = tree_with_merge(&folder);

    // Tables with keys far from "counter" — key range won't overlap
    tree.insert("zzz_far_away", vec![0u8; 8], 0);
    tree.flush_active_memtable(0).unwrap();
    tree.insert("yyy_also_far", vec![0u8; 8], 1);
    tree.flush_active_memtable(0).unwrap();

    // Base value
    tree.insert("counter", 50_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0).unwrap();

    // Merge operand
    tree.merge("counter", 10_i64.to_le_bytes(), 3);

    assert_eq!(get_counter(&tree, "counter", 4), Some(60));
}
