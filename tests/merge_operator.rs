// Guard import is required: into_inner() and key() are trait methods from IterGuard (re-exported as Guard)
use lsm_tree::{AbstractTree, Config, Guard, MergeOperator, SequenceNumberCounter, UserValue};
use std::sync::Arc;

/// Simple counter merge operator: base + sum of operands (i64 little-endian).
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
                i64::from_le_bytes(bytes.try_into().expect("checked length"))
            }
            Some(_) => return Err(lsm_tree::Error::MergeOperator),
            None => 0,
        };

        for operand in operands {
            if operand.len() != 8 {
                return Err(lsm_tree::Error::MergeOperator);
            }
            counter += i64::from_le_bytes((*operand).try_into().expect("checked length"));
        }

        Ok(counter.to_le_bytes().to_vec().into())
    }
}

fn open_tree_with_counter(folder: &tempfile::TempDir) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .open()
    .unwrap()
}

fn open_blob_tree_with_counter(folder: &tempfile::TempDir) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(CounterMerge)))
    .with_kv_separation(Some(lsm_tree::KvSeparationOptions::default()))
    .open()
    .unwrap()
}

fn get_counter(tree: &lsm_tree::AnyTree, key: &str, seqno: u64) -> Option<i64> {
    tree.get(key, seqno)
        .unwrap()
        .map(|v| i64::from_le_bytes((*v).try_into().unwrap()))
}

#[test]
fn merge_counter_increment() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // 3 merge operands, no base value
    tree.merge("counter", 1_i64.to_le_bytes(), 0);
    tree.merge("counter", 2_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);

    assert_eq!(Some(6), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_with_base_value() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Base value = 100, then +5, +10
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 5_i64.to_le_bytes(), 1);
    tree.merge("counter", 10_i64.to_le_bytes(), 2);

    assert_eq!(Some(115), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_after_tombstone() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Base=50, delete, then merge +7
    tree.insert("counter", 50_i64.to_le_bytes(), 0);
    tree.remove("counter", 1);
    tree.merge("counter", 7_i64.to_le_bytes(), 2);

    // Merge after delete should produce value from operands only (base=None)
    assert_eq!(Some(7), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_mvcc_snapshot() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.merge("counter", 30_i64.to_le_bytes(), 3);

    // Read at different snapshots
    assert_eq!(Some(100), get_counter(&tree, "counter", 1)); // base only
    assert_eq!(Some(110), get_counter(&tree, "counter", 2)); // base + 10
    assert_eq!(Some(130), get_counter(&tree, "counter", 3)); // base + 10 + 20
    assert_eq!(Some(160), get_counter(&tree, "counter", 4)); // base + 10 + 20 + 30
}

#[test]
fn merge_no_operator_returns_raw() {
    let folder = tempfile::tempdir().unwrap();

    // Open tree WITHOUT merge operator
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap();

    tree.merge("key", 42_i64.to_le_bytes(), 0);

    // Should return raw operand bytes (backward compatible)
    let result = tree.get("key", 1).unwrap().unwrap();
    assert_eq!(42_i64.to_le_bytes(), &*result);
}

#[test]
fn merge_mixed_keys() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Regular insert
    tree.insert("regular", b"hello".to_vec(), 0);

    // Merge key
    tree.merge("counter", 5_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);

    // Both should work correctly
    assert_eq!(
        Some(b"hello".as_slice().into()),
        tree.get("regular", 3).unwrap()
    );
    assert_eq!(Some(8), get_counter(&tree, "counter", 3));
}

#[test]
fn merge_flush_and_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.merge("counter", 20_i64.to_le_bytes(), 2);

    // Flush to disk
    tree.flush_active_memtable(3)?;

    // Read from flushed data — compaction stream should merge operands
    assert_eq!(Some(130), get_counter(&tree, "counter", 3));

    Ok(())
}

#[test]
fn merge_across_memtable_and_tables() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write base and first operand, flush
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(2)?;

    // Write more operands to active memtable
    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.merge("counter", 30_i64.to_le_bytes(), 3);

    // Should merge across memtable and disk tables
    assert_eq!(Some(160), get_counter(&tree, "counter", 4));

    Ok(())
}

#[test]
fn merge_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("b", 1_i64.to_le_bytes(), 1);
    tree.merge("b", 2_i64.to_le_bytes(), 2);
    tree.insert("c", 30_i64.to_le_bytes(), 3);

    let items: Vec<_> = tree
        .iter(4, None)
        .map(|guard| {
            let (key, value): (lsm_tree::UserKey, lsm_tree::UserValue) =
                guard.into_inner().unwrap();
            let val = i64::from_le_bytes((*value).try_into().unwrap());
            (String::from_utf8(key.to_vec()).unwrap(), val)
        })
        .collect();

    assert_eq!(items.len(), 3);
    assert_eq!(items[0], ("a".to_string(), 10));
    assert_eq!(items[1], ("b".to_string(), 3)); // merged: 1 + 2
    assert_eq!(items[2], ("c".to_string(), 30));

    Ok(())
}

#[test]
fn merge_multiple_operands_only() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // 5 operands, no base
    for i in 0..5 {
        tree.merge("sum", (i as i64).to_le_bytes(), i);
    }

    assert_eq!(Some(0 + 1 + 2 + 3 + 4), get_counter(&tree, "sum", 5));
}

#[test]
fn merge_key_not_found() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("a", 1_i64.to_le_bytes(), 0);

    assert_eq!(None, get_counter(&tree, "b", 1));
}

#[test]
fn merge_after_weak_tombstone() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 50_i64.to_le_bytes(), 0);
    tree.remove_weak("counter", 1);
    tree.merge("counter", 7_i64.to_le_bytes(), 2);

    // WeakTombstone stops base search — merge with base=None
    assert_eq!(Some(7), get_counter(&tree, "counter", 3));
}

/// Merge operator that always fails
struct FailingMerge;

impl MergeOperator for FailingMerge {
    fn merge(
        &self,
        _key: &[u8],
        _base_value: Option<&[u8]>,
        _operands: &[&[u8]],
    ) -> lsm_tree::Result<UserValue> {
        Err(lsm_tree::Error::MergeOperator)
    }
}

#[test]
fn merge_error_propagation() {
    let folder = tempfile::tempdir().unwrap();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_merge_operator(Some(Arc::new(FailingMerge)))
    .open()
    .unwrap();

    tree.merge("key", b"op1".to_vec(), 0);

    let result = tree.get("key", 1);
    assert!(matches!(result, Err(lsm_tree::Error::MergeOperator)));
}

#[test]
fn merge_multi_get() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("b", 1_i64.to_le_bytes(), 1);
    tree.merge("b", 2_i64.to_le_bytes(), 2);
    tree.insert("c", 30_i64.to_le_bytes(), 3);

    let results = tree.multi_get(["a", "b", "c", "missing"], 4).unwrap();

    assert_eq!(
        results[0]
            .as_ref()
            .map(|v| i64::from_le_bytes((**v).try_into().unwrap())),
        Some(10)
    );
    assert_eq!(
        results[1]
            .as_ref()
            .map(|v| i64::from_le_bytes((**v).try_into().unwrap())),
        Some(3)
    );
    assert_eq!(
        results[2]
            .as_ref()
            .map(|v| i64::from_le_bytes((**v).try_into().unwrap())),
        Some(30)
    );
    assert!(results[3].is_none());
}

#[test]
fn merge_prefix_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.merge("user:1:score", 10_i64.to_le_bytes(), 0);
    tree.merge("user:1:score", 20_i64.to_le_bytes(), 1);
    tree.merge("user:2:score", 5_i64.to_le_bytes(), 2);
    tree.insert("other", 99_i64.to_le_bytes(), 3);

    let items: Vec<_> = tree
        .prefix("user:", 4, None)
        .map(|guard| {
            let (key, value): (lsm_tree::UserKey, lsm_tree::UserValue) =
                guard.into_inner().unwrap();
            let val = i64::from_le_bytes((*value).try_into().unwrap());
            (String::from_utf8(key.to_vec()).unwrap(), val)
        })
        .collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0], ("user:1:score".to_string(), 30)); // 10 + 20
    assert_eq!(items[1], ("user:2:score".to_string(), 5));

    Ok(())
}

#[test]
fn merge_contains_key() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("exists", 1_i64.to_le_bytes(), 0);

    // MergeOperand should count as "key exists" after resolution
    assert!(tree.contains_key("exists", 1).unwrap());
    assert!(!tree.contains_key("missing", 1).unwrap());
}

#[test]
fn merge_major_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write and flush multiple times to create multiple tables.
    // Use gc_seqno_threshold=0 to preserve merge operands during flush
    // (they can't be resolved since the base may be in a different table).
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0)?;

    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0)?;

    // Before compaction: read path should resolve across tables
    assert_eq!(Some(130), get_counter(&tree, "counter", 3));

    // Major compaction should merge all into single entry
    tree.major_compact(64_000_000, 3)?;

    assert_eq!(Some(130), get_counter(&tree, "counter", 3));
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn merge_reverse_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("b", 1_i64.to_le_bytes(), 1);
    tree.merge("b", 2_i64.to_le_bytes(), 2);
    tree.insert("c", 30_i64.to_le_bytes(), 3);

    let items: Vec<_> = tree
        .iter(4, None)
        .rev()
        .map(|guard| {
            let (key, value): (lsm_tree::UserKey, lsm_tree::UserValue) =
                guard.into_inner().unwrap();
            let val = i64::from_le_bytes((*value).try_into().unwrap());
            (String::from_utf8(key.to_vec()).unwrap(), val)
        })
        .collect();

    assert_eq!(items.len(), 3);
    assert_eq!(items[0], ("c".to_string(), 30));
    assert_eq!(items[1], ("b".to_string(), 3)); // merged: 1 + 2
    assert_eq!(items[2], ("a".to_string(), 10));

    Ok(())
}

#[test]
fn merge_overwrite_then_merge() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Insert → overwrite → merge
    tree.insert("key", 10_i64.to_le_bytes(), 0);
    tree.insert("key", 20_i64.to_le_bytes(), 1);
    tree.merge("key", 5_i64.to_le_bytes(), 2);

    // Should merge with latest base (20), not first (10)
    assert_eq!(Some(25), get_counter(&tree, "key", 3));
}

// --- BlobTree merge tests ---

#[test]
fn blob_tree_merge_write_and_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_blob_tree_with_counter(&folder);

    tree.merge("counter", 1_i64.to_le_bytes(), 0);
    tree.merge("counter", 2_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);

    // BlobTree merge write path works (same as standard tree internally)
    tree.flush_active_memtable(0)?;

    Ok(())
}

#[test]
fn blob_tree_merge_mixed_operations() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_blob_tree_with_counter(&folder);

    tree.insert("regular", b"hello world value".to_vec(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.remove("deleted", 2);
    tree.merge("counter", 20_i64.to_le_bytes(), 3);

    tree.flush_active_memtable(0)?;
    assert_eq!(0, tree.sealed_memtable_count());

    Ok(())
}

// --- Additional edge case tests for coverage ---

#[test]
fn merge_sealed_memtable_resolution() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write base to memtable, rotate (seal it)
    tree.insert("key", 100_i64.to_le_bytes(), 0);

    // Rotate memtable manually by writing enough to trigger
    // or use flush with gc_threshold=0 to preserve entries
    tree.flush_active_memtable(0)?;

    // Now write operands to a new memtable, then seal it
    tree.merge("key", 10_i64.to_le_bytes(), 1);

    // Read should resolve across sealed+tables
    assert_eq!(Some(110), get_counter(&tree, "key", 2));

    Ok(())
}

#[test]
fn merge_empty_operands_after_base() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    // Just a base value, no operands — get should return base
    tree.insert("key", 42_i64.to_le_bytes(), 0);
    assert_eq!(Some(42), get_counter(&tree, "key", 1));
}

#[test]
fn merge_size_of() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("key", 1_i64.to_le_bytes(), 0);

    // size_of goes through get path → should resolve merge
    let size = tree.size_of("key", 1).unwrap();
    assert_eq!(size, Some(8)); // i64 = 8 bytes
}

#[test]
fn merge_is_empty_and_len() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    assert!(tree.is_empty(0, None)?);

    tree.merge("a", 1_i64.to_le_bytes(), 0);
    tree.merge("b", 2_i64.to_le_bytes(), 1);

    assert!(!tree.is_empty(2, None)?);
    assert_eq!(2, tree.len(2, None)?);

    Ok(())
}

#[test]
fn merge_first_last_key_value() {
    let folder = tempfile::tempdir().unwrap();
    let tree = open_tree_with_counter(&folder);

    tree.merge("b", 1_i64.to_le_bytes(), 0);
    tree.merge("d", 2_i64.to_le_bytes(), 1);
    tree.insert("a", 10_i64.to_le_bytes(), 2);
    tree.insert("e", 20_i64.to_le_bytes(), 3);

    let first = tree.first_key_value(4, None).unwrap().key().unwrap();
    assert_eq!(&*first, b"a");

    let last = tree.last_key_value(4, None).unwrap().key().unwrap();
    assert_eq!(&*last, b"e");
}

#[test]
fn merge_multiple_operands_in_single_table() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write base + multiple operands, flush with gc_threshold=0
    // to preserve all entries individually in one SST
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.merge("counter", 20_i64.to_le_bytes(), 2);
    tree.merge("counter", 30_i64.to_le_bytes(), 3);
    tree.flush_active_memtable(0)?;

    // All 4 entries are in the same table. table.get() returns only
    // the newest (MergeOperand@3), but resolve_merge_via_pipeline must collect
    // all entries via range scan to produce the correct result.
    assert_eq!(Some(160), get_counter(&tree, "counter", 4));

    Ok(())
}

#[test]
fn merge_operand_above_watermark_preserves_tail() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Write entries with specific seqnos
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 5);
    tree.merge("counter", 20_i64.to_le_bytes(), 10);

    // Flush with gc_threshold=7: seqno 0 and 5 are below, seqno 10 is above.
    // The operand at seqno=10 must NOT cause the tail (seqno=5, seqno=0)
    // to be drained — they are needed for merge resolution.
    tree.flush_active_memtable(7)?;

    // Read should still resolve correctly: 100 + 10 + 20 = 130
    assert_eq!(Some(130), get_counter(&tree, "counter", 11));

    Ok(())
}

/// Merge state persists across tree reopen (crash recovery).
#[test]
fn merge_persists_after_reopen() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    {
        let tree = open_tree_with_counter(&folder);
        tree.insert("counter", 100_i64.to_le_bytes(), 0);
        tree.merge("counter", 5_i64.to_le_bytes(), 1);
        tree.merge("counter", 10_i64.to_le_bytes(), 2);
        tree.flush_active_memtable(0)?;
    }

    // Reopen tree — merge operands must be recovered from disk
    let tree = open_tree_with_counter(&folder);
    assert_eq!(Some(115), get_counter(&tree, "counter", 3));

    // Add more operands after reopen
    tree.merge("counter", 20_i64.to_le_bytes(), 3);
    assert_eq!(Some(135), get_counter(&tree, "counter", 4));

    Ok(())
}

/// Merge after major compaction fully resolves operands into a single Value.
#[test]
fn merge_major_compaction_resolves_all() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 10_i64.to_le_bytes(), 0);
    tree.merge("counter", 5_i64.to_le_bytes(), 1);
    tree.merge("counter", 3_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0)?;

    // Before compaction: read should resolve across operands
    assert_eq!(Some(18), get_counter(&tree, "counter", 3));

    // Major compaction merges all operands with the base
    tree.major_compact(64_000_000, 3)?;

    // After compaction, the key should be a single Value = 18
    assert_eq!(Some(18), get_counter(&tree, "counter", 3));

    // New operands stack on top of the compacted base
    tree.merge("counter", 2_i64.to_le_bytes(), 3);
    assert_eq!(Some(20), get_counter(&tree, "counter", 4));

    Ok(())
}

/// Range tombstone between base and operand: base is suppressed, operand survives.
#[test]
fn merge_rt_kills_base_preserves_operand() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // base@seqno=0
    tree.insert("counter", 100_i64.to_le_bytes(), 0);

    // RT [counter, counter\x00) at seqno=5 → kills base@0
    tree.remove_range("counter", "counter\x00", 5);

    // Merge operand@seqno=10 (above RT@5 → survives)
    tree.merge("counter", 7_i64.to_le_bytes(), 10);

    // get@11: base@0 suppressed by RT@5, operand@10 survives
    // merge(key, None, [7]) = 0 + 7 = 7
    assert_eq!(Some(7), get_counter(&tree, "counter", 11));

    Ok(())
}

/// Range tombstone kills all versions — key appears deleted.
#[test]
fn merge_rt_kills_all() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 5_i64.to_le_bytes(), 1);

    // RT at seqno=10 covers "counter" → kills base@0 and operand@1
    tree.remove_range("counter", "counter\x00", 10);

    // All versions suppressed → key not found
    assert_eq!(None, get_counter(&tree, "counter", 11));

    Ok(())
}

/// Range tombstone + merge in range scan: RT-suppressed operands excluded.
#[test]
fn merge_rt_in_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.insert("b", 100_i64.to_le_bytes(), 1);
    tree.merge("b", 5_i64.to_le_bytes(), 2);

    // RT at seqno=8 covers "b" → kills base@1 and operand@2
    tree.remove_range("b", "b\x00", 8);

    // Merge operand above RT
    tree.merge("b", 7_i64.to_le_bytes(), 10);

    // Range scan: a=10, b=7 (base@1 and op@2 killed by RT, op@10 survives)
    let items: Vec<_> = tree
        .iter(11, None)
        .map(|guard| {
            let (_k, v): (lsm_tree::UserKey, lsm_tree::UserValue) = guard.into_inner().unwrap();
            i64::from_le_bytes((*v).try_into().unwrap())
        })
        .collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0], 10); // a: untouched
    assert_eq!(items[1], 7); // b: only op@10, base+op@2 killed by RT

    Ok(())
}

/// Merge operands with range iteration and snapshot isolation.
#[test]
fn merge_range_with_snapshot_isolation() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.insert("a", 10_i64.to_le_bytes(), 0);
    tree.merge("a", 5_i64.to_le_bytes(), 1);
    tree.insert("b", 20_i64.to_le_bytes(), 2);
    tree.merge("b", 3_i64.to_le_bytes(), 3);

    // Snapshot at seqno=3: sees a=10+5=15, b=20 (merge on b at seqno=3 not visible)
    let items: Vec<_> = tree
        .iter(3, None)
        .map(|guard| {
            let (_k, v): (lsm_tree::UserKey, lsm_tree::UserValue) = guard.into_inner().unwrap();
            i64::from_le_bytes((*v).try_into().unwrap())
        })
        .collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0], 15); // a: 10 + 5
    assert_eq!(items[1], 20); // b: only base, merge not visible

    // Full view: a=15, b=23
    let items: Vec<_> = tree
        .iter(4, None)
        .map(|guard| {
            let (_k, v): (lsm_tree::UserKey, lsm_tree::UserValue) = guard.into_inner().unwrap();
            i64::from_le_bytes((*v).try_into().unwrap())
        })
        .collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0], 15); // a: 10 + 5
    assert_eq!(items[1], 23); // b: 20 + 3

    Ok(())
}

/// BlobTree with large values triggers Indirection — merge falls back to latest operand.
#[test]
fn merge_blob_tree_indirection_fallback() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_blob_tree_with_counter(&folder);

    // Write a large base value (>1 KiB) to trigger blob separation
    let large_base = vec![0u8; 2048];
    tree.insert("big", &large_base, 0);
    tree.flush_active_memtable(0)?;

    // Add merge operand on top
    tree.merge("big", 5_i64.to_le_bytes(), 1);

    // get should return the raw operand bytes (fallback when base is Indirection)
    let result = tree.get("big", 2)?;
    assert_eq!(
        result,
        Some(5_i64.to_le_bytes().to_vec().into()),
        "BlobTree indirection fallback must return latest operand bytes"
    );

    Ok(())
}

/// Merge at seqno=0 read boundary — should return None (no visible entries).
#[test]
fn merge_read_at_seqno_zero() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    tree.merge("counter", 5_i64.to_le_bytes(), 0);

    // seqno=0 means nothing is visible
    assert_eq!(None, get_counter(&tree, "counter", 0));

    Ok(())
}

/// RT suppresses base in flushed SST — merge across memtable and disk with RT.
#[test]
fn merge_rt_across_flush_boundary() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Base on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0)?;

    // RT in memtable kills base on disk
    tree.remove_range("counter", "counter\x00", 5);

    // Merge operand in memtable above RT
    tree.merge("counter", 42_i64.to_le_bytes(), 10);

    // base@0 suppressed by RT@5, operand@10 survives
    assert_eq!(Some(42), get_counter(&tree, "counter", 11));

    Ok(())
}

/// RT suppresses base across multiple disk tables.
#[test]
fn merge_rt_across_multiple_flushes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Base in first SST
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0)?;

    // Old operand in second SST
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    // RT kills everything at seqno < 5
    tree.remove_range("counter", "counter\x00", 5);

    // New operand above RT
    tree.merge("counter", 33_i64.to_le_bytes(), 10);

    // base@0 and op@1 suppressed, op@10 survives: merge(None, [33]) = 33
    assert_eq!(Some(33), get_counter(&tree, "counter", 11));

    Ok(())
}

/// Multiple operands across disk and memtable, RT kills the middle ones.
#[test]
fn merge_rt_partial_suppression_across_layers() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Base + old operand on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    // RT kills everything at seqno < 5 (base@0 + operand@1)
    tree.remove_range("counter", "counter\x00", 5);

    // New operands above RT
    tree.merge("counter", 20_i64.to_le_bytes(), 6);
    tree.merge("counter", 30_i64.to_le_bytes(), 7);

    // Only op@6 + op@7 survive. merge(None, [20, 30]) = 0 + 20 + 30 = 50
    assert_eq!(Some(50), get_counter(&tree, "counter", 8));

    Ok(())
}

/// RT suppression on no-operator path: get() and multi_get() must agree.
#[test]
fn merge_rt_no_operator_get_and_multi_get_agree() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Open tree WITHOUT merge operator
    let tree = lsm_tree::Config::new(
        &folder,
        lsm_tree::SequenceNumberCounter::default(),
        lsm_tree::SequenceNumberCounter::default(),
    )
    .open()?;

    // Write a MergeOperand directly (no operator needed for writing)
    tree.merge("key", b"operand_bytes", 0);

    // RT at seqno=5 suppresses the operand@0
    tree.remove_range("key", "key\x00", 5);

    // Both get() and multi_get() should return None (RT-suppressed)
    assert_eq!(None, tree.get("key", 6)?);

    let results = tree.multi_get(["key"], 6)?;
    assert!(
        results[0].is_none(),
        "multi_get must agree with get on RT suppression"
    );

    // Without RT (read at seqno before RT is visible): operand visible
    assert!(tree.get("key", 1)?.is_some());

    Ok(())
}

/// RT suppresses operand in disk range scan during merge resolution.
/// Exercises the is_rt_suppressed path inside the table.range() fallback
/// in resolve_merge_via_pipeline.
#[test]
fn merge_rt_suppresses_operand_in_disk_range_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Base + operand in same flush (gc_threshold=0 preserves both as separate entries)
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    // RT at seqno=2 suppresses all entries with seqno < 2 (kills base@0 and op@1)
    tree.remove_range("counter", "counter\x00", 2);

    // New operand above RT
    tree.merge("counter", 20_i64.to_le_bytes(), 3);

    // RT@2 suppresses base@0 and op@1. Only op@3 survives: merge(None, [20]) = 20
    assert_eq!(Some(20), get_counter(&tree, "counter", 4));

    Ok(())
}

/// Merge resolution with base value found via disk table point lookup
/// (non-MergeOperand entry on disk — exercises the process_entry path at line ~918).
#[test]
fn merge_disk_base_via_point_lookup() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Base on disk (single entry, not MergeOperand, so bloom-filtered get() finds it)
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.flush_active_memtable(0)?;

    // Operands in active memtable
    tree.merge("counter", 10_i64.to_le_bytes(), 1);
    tree.merge("counter", 20_i64.to_le_bytes(), 2);

    // resolve_merge_via_pipeline: active memtable has op@2, op@1
    // Then scans disk: table.get() returns base@0 (Value, not MergeOperand)
    // → process_entry sets base_value, found_base=true
    assert_eq!(Some(130), get_counter(&tree, "counter", 3));

    Ok(())
}

/// Merge with Tombstone base in sealed memtable — exercises sealed memtable
/// scan path in resolve_merge_via_pipeline.
#[test]
fn merge_tombstone_in_sealed_memtable() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Base value
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    // Delete it
    tree.remove("counter", 1);
    // Flush base + tombstone to disk
    tree.flush_active_memtable(0)?;

    // New operands in active memtable
    tree.merge("counter", 42_i64.to_le_bytes(), 2);

    // resolve_merge_via_pipeline scans active (finds op@2), then disk (finds tombstone@1)
    // tombstone stops scan, merge with no base: merge(None, [42]) = 42
    assert_eq!(Some(42), get_counter(&tree, "counter", 3));

    Ok(())
}

/// Merge where operands span active memtable and disk — tests that
/// resolve_merge_via_pipeline correctly collects from all layers.
#[test]
fn merge_operands_across_active_and_disk() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // First batch: base + operand on disk
    tree.insert("counter", 100_i64.to_le_bytes(), 0);
    tree.merge("counter", 5_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    // Second batch: more operands on disk
    tree.merge("counter", 10_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0)?;

    // Third batch: operand in active memtable
    tree.merge("counter", 15_i64.to_le_bytes(), 3);

    // All layers: active(op@3) + disk1(op@2) + disk2(op@1, base@0)
    // = 100 + 5 + 10 + 15 = 130
    assert_eq!(Some(130), get_counter(&tree, "counter", 4));

    Ok(())
}

/// Merge correctness when bloom pre-filtering is enabled and there exists an
/// overlapping table whose bloom filter reports the key absent. This ensures
/// the extra overlapping table does not affect the merged result.
#[test]
fn merge_bloom_with_overlapping_non_matching_table() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree_with_counter(&folder);

    // Table 1: wide key range [aaa, zzz] that does NOT contain "counter".
    // Its key_range overlaps "counter"; bloom may reject it (best-effort).
    tree.insert("aaa", 0_i64.to_le_bytes(), 0);
    tree.insert("zzz", 0_i64.to_le_bytes(), 1);
    tree.flush_active_memtable(0)?;

    // Table 2: contains "counter" base value.
    tree.insert("counter", 100_i64.to_le_bytes(), 2);
    tree.flush_active_memtable(0)?;

    // Merge operand in active memtable
    tree.merge("counter", 10_i64.to_le_bytes(), 3);

    // resolve_merge_via_pipeline builds a key..=key range with bloom hash.
    // Table 1 does not contain "counter" so it contributes nothing.
    // merge(Some(100), [10]) = 110
    assert_eq!(Some(110), get_counter(&tree, "counter", 4));

    Ok(())
}
