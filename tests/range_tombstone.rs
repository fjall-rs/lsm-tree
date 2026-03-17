// Guard: trait import required for .key() method on iterator items (IterGuard trait)
use lsm_tree::{get_tmp_folder, AbstractTree, AnyTree, Config, Guard, SequenceNumberCounter};
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

/// Helper to collect keys from a forward iterator.
/// Returns `Vec<Vec<u8>>` which compares correctly with `vec![b"a", b"b"]`
/// via Rust's `PartialEq` blanket impl for `Vec<T>` where `T: PartialEq<U>`.
fn collect_keys(tree: &AnyTree, seqno: u64) -> lsm_tree::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for item in tree.iter(seqno, None) {
        let k = item.key()?;
        keys.push(k.to_vec());
    }
    Ok(keys)
}

/// Helper to collect keys from a reverse iterator.
fn collect_keys_rev(tree: &AnyTree, seqno: u64) -> lsm_tree::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for item in tree.iter(seqno, None).rev() {
        let k = item.key()?;
        keys.push(k.to_vec());
    }
    Ok(keys)
}

// --- Test A: Point reads suppressed by memtable range tombstone ---
#[test]
fn range_tombstone_suppresses_point_read_in_memtable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);
    tree.insert("c", "val_c", 3);
    tree.insert("d", "val_d", 4);

    // Range tombstone [b, d) at seqno 10 suppresses b and c
    tree.remove_range("b", "d", 10);

    // a is outside range — visible
    assert_eq!(Some("val_a".as_bytes().into()), tree.get("a", 11)?);
    // b is inside range — suppressed
    assert_eq!(None, tree.get("b", 11)?);
    // c is inside range — suppressed
    assert_eq!(None, tree.get("c", 11)?);
    // d is at exclusive end — visible
    assert_eq!(Some("val_d".as_bytes().into()), tree.get("d", 11)?);

    Ok(())
}

// --- Test B: Range tombstone respects MVCC ---
#[test]
fn range_tombstone_mvcc_visibility() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);

    // Range tombstone at seqno 10
    tree.remove_range("a", "z", 10);

    // Reading at seqno 5 — tombstone not visible (seqno 10 > 5)
    assert_eq!(Some("val_a".as_bytes().into()), tree.get("a", 5)?);
    assert_eq!(Some("val_b".as_bytes().into()), tree.get("b", 5)?);

    // Reading at seqno 11 — tombstone visible, values suppressed
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);

    Ok(())
}

// --- Test C: Range tombstone does not suppress newer values ---
#[test]
fn range_tombstone_does_not_suppress_newer_values() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "old_a", 1);
    tree.remove_range("a", "z", 5);
    tree.insert("a", "new_a", 10);

    // new_a at seqno 10 is newer than tombstone at seqno 5
    assert_eq!(Some("new_a".as_bytes().into()), tree.get("a", 11)?);

    Ok(())
}

// --- Test D: Range iteration suppressed by range tombstone ---
#[test]
fn range_tombstone_suppresses_range_iteration() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.insert("d", "4", 4);
    tree.insert("e", "5", 5);

    // Delete [b, d) at seqno 10
    tree.remove_range("b", "d", 10);

    let keys = collect_keys(&tree, 11)?;
    assert_eq!(keys, vec![b"a", b"d", b"e"]);

    Ok(())
}

// --- Test E: Reverse iteration with range tombstone ---
#[test]
fn range_tombstone_suppresses_reverse_iteration() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.insert("d", "4", 4);
    tree.insert("e", "5", 5);

    tree.remove_range("b", "d", 10);

    let keys = collect_keys_rev(&tree, 11)?;
    assert_eq!(keys, vec![b"e", b"d", b"a"]);

    Ok(())
}

// --- Test F: Range tombstone in memtable suppresses SST data ---
#[test]
fn range_tombstone_suppresses_across_memtable_and_sst() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Insert data and flush to SST
    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);
    tree.insert("c", "val_c", 3);
    tree.flush_active_memtable(0)?;

    // Range tombstone in memtable suppresses SST data
    tree.remove_range("a", "d", 10);

    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(None, tree.get("c", 11)?);

    Ok(())
}

// --- Test G: Range tombstone in sealed memtable ---
#[test]
fn range_tombstone_in_sealed_memtable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Insert range tombstone then seal the memtable
    tree.remove_range("a", "z", 10);
    assert!(
        tree.rotate_memtable().is_some(),
        "memtable with RT should seal"
    );
    assert!(tree.sealed_memtable_count() > 0);

    // Insert new data in active memtable (lower seqno)
    tree.insert("b", "val_b", 5);

    // b@5 is suppressed by sealed tombstone@10
    assert_eq!(None, tree.get("b", 11)?);

    // Insert newer data
    tree.insert("b", "val_b_new", 15);
    // b@15 survives (newer than tombstone@10)
    assert_eq!(Some("val_b_new".as_bytes().into()), tree.get("b", 16)?);

    Ok(())
}

// --- Test H: remove_prefix ---
#[test]
fn remove_prefix_suppresses_matching_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("user:1", "alice", 1);
    tree.insert("user:2", "bob", 2);
    tree.insert("user:3", "carol", 3);
    tree.insert("order:1", "pizza", 4);

    // Delete all "user:" prefixed keys
    tree.remove_prefix("user:", 10);

    assert_eq!(None, tree.get("user:1", 11)?);
    assert_eq!(None, tree.get("user:2", 11)?);
    assert_eq!(None, tree.get("user:3", 11)?);
    // "order:" is not affected
    assert_eq!(Some("pizza".as_bytes().into()), tree.get("order:1", 11)?);

    Ok(())
}

// --- Test I: Overlapping range tombstones ---
#[test]
fn overlapping_range_tombstones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.insert("d", "4", 4);
    tree.insert("e", "5", 5);

    // Two overlapping tombstones
    tree.remove_range("a", "c", 10); // [a, c)
    tree.remove_range("b", "e", 15); // [b, e)

    // a: suppressed by [a,c)@10
    assert_eq!(None, tree.get("a", 20)?);
    // b: suppressed by both
    assert_eq!(None, tree.get("b", 20)?);
    // c: suppressed by [b,e)@15 only
    assert_eq!(None, tree.get("c", 20)?);
    // d: suppressed by [b,e)@15
    assert_eq!(None, tree.get("d", 20)?);
    // e: NOT suppressed (exclusive end of [b,e))
    assert_eq!(Some("5".as_bytes().into()), tree.get("e", 20)?);

    Ok(())
}

// --- Test J: Range iteration with sealed tombstone and SST data ---
#[test]
fn range_iteration_with_sealed_tombstone_and_sst_data() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Data in SST
    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.insert("d", "4", 4);
    tree.flush_active_memtable(0)?;

    // Range tombstone in sealed memtable
    tree.remove_range("b", "d", 10);
    tree.rotate_memtable();

    // New data in active memtable
    tree.insert("e", "5", 11);

    let keys = collect_keys(&tree, 12)?;
    assert_eq!(keys, vec![b"a", b"d", b"e"]);

    Ok(())
}

// --- Test K: Range tombstone persists through flush to SST ---
#[test]
fn range_tombstone_persists_through_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Insert data
    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);

    // Insert range tombstone in same memtable
    tree.remove_range("a", "c", 10);

    // Flush everything to SST (both data and range tombstone)
    tree.flush_active_memtable(0)?;

    // After flush: range tombstone should still suppress from SST
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(Some("3".as_bytes().into()), tree.get("c", 11)?); // c is at exclusive end

    // Verify via range iteration too
    let keys = collect_keys(&tree, 11)?;
    assert_eq!(keys, vec![b"c"]);

    Ok(())
}

// --- Test K2: Range tombstone survives compaction ---
#[test]
fn range_tombstone_survives_compaction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Batch 1: data + range tombstone in same memtable
    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.insert("d", "4", 4);
    tree.remove_range("b", "d", 10);
    tree.flush_active_memtable(0)?;

    // Batch 2: more data to force a second table
    tree.insert("e", "5", 11);
    tree.flush_active_memtable(0)?;

    // Both tables in L0 — compact them
    assert_eq!(2, tree.table_count());
    tree.major_compact(64_000_000, 0)?;

    // After compaction, range tombstone should still suppress
    assert_eq!(Some("1".as_bytes().into()), tree.get("a", 12)?);
    assert_eq!(None, tree.get("b", 12)?);
    assert_eq!(None, tree.get("c", 12)?);
    assert_eq!(Some("4".as_bytes().into()), tree.get("d", 12)?);
    assert_eq!(Some("5".as_bytes().into()), tree.get("e", 12)?);

    // Verify via iteration
    let keys = collect_keys(&tree, 12)?;
    assert_eq!(keys, vec![b"a", b"d", b"e"]);

    Ok(())
}

// --- Test L: Range tombstone persists through recovery ---
#[test]
fn range_tombstone_persists_through_recovery() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = open_tree(folder.path());
        tree.insert("a", "1", 1);
        tree.insert("b", "2", 2);
        tree.insert("c", "3", 3);
        tree.remove_range("a", "c", 10);
        tree.flush_active_memtable(0)?;
    }

    // Reopen the tree — range tombstones should be recovered from SST
    {
        let tree = open_tree(folder.path());
        assert_eq!(None, tree.get("a", 11)?);
        assert_eq!(None, tree.get("b", 11)?);
        assert_eq!(Some("3".as_bytes().into()), tree.get("c", 11)?);
    }

    Ok(())
}

// --- Test M: RT-only memtable flush creates a valid table ---
#[test]
fn range_tombstone_only_flush() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // First: insert data and flush
    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.flush_active_memtable(0)?;

    let tables_before = tree.table_count();

    // Second: insert only a range tombstone and flush
    // RT-only flush writes synthetic sentinel tombstones to create a valid SST
    tree.remove_range("a", "c", 10);
    tree.flush_active_memtable(0)?;

    assert!(
        tree.table_count() > tables_before,
        "RT-only flush should produce a table with sentinel tombstones"
    );

    // The range tombstone in the SST should suppress
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(Some("3".as_bytes().into()), tree.get("c", 11)?);

    Ok(())
}

// --- Test N: GC eviction at bottom level ---
#[test]
fn range_tombstone_gc_eviction_at_bottom_level() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.remove_range("a", "d", 10);
    tree.flush_active_memtable(0)?;

    // Before GC: range tombstone suppresses all
    assert_eq!(None, tree.get("a", 11)?);

    // Major compact with GC watermark ABOVE the tombstone seqno
    // This should evict the range tombstone at the bottom level
    tree.major_compact(64_000_000, 11)?;

    // After GC: both data and tombstone are evicted (all seqno < 11)
    // Insert new data — should be visible (no lingering tombstone)
    tree.insert("a", "new_a", 15);
    assert_eq!(Some("new_a".as_bytes().into()), tree.get("a", 16)?);

    Ok(())
}

// --- Test O: Prefix iteration with range tombstone in SST ---
#[test]
fn range_tombstone_prefix_iteration_with_sst() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("user:1", "alice", 1);
    tree.insert("user:2", "bob", 2);
    tree.insert("user:3", "carol", 3);
    tree.insert("order:1", "pizza", 4);
    tree.remove_prefix("user:", 10);
    tree.flush_active_memtable(0)?;

    // Prefix iteration over "user:" should yield nothing
    let mut user_keys = Vec::new();
    for item in tree.prefix("user:", 11, None) {
        let k = item.key()?;
        user_keys.push(k.to_vec());
    }
    assert!(user_keys.is_empty());

    // Prefix iteration over "order:" should yield "order:1"
    let mut order_keys = Vec::new();
    for item in tree.prefix("order:", 11, None) {
        let k = item.key()?;
        order_keys.push(k.to_vec());
    }
    assert_eq!(order_keys, vec![b"order:1"]);

    Ok(())
}

// --- Test P: Compaction with MultiWriter rotation preserves RTs across tables ---
#[test]
fn range_tombstone_survives_compaction_with_rotation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    // Use small target_size to force MultiWriter rotation during compaction
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert enough data to produce multiple tables on compaction
    for i in 0u8..20 {
        let key = format!("key_{i:03}");
        let val = "x".repeat(4000);
        tree.insert(key.as_bytes(), val.as_bytes(), u64::from(i));
    }
    // Range tombstone covering a subset
    tree.remove_range("key_005", "key_015", 50);
    tree.flush_active_memtable(0)?;

    // Force compaction with small target_size to trigger rotation
    tree.major_compact(1024, 0)?;

    // After compaction: keys inside [key_005, key_015) should be suppressed
    assert_eq!(None, tree.get("key_005", 51)?);
    assert_eq!(None, tree.get("key_010", 51)?);
    assert_eq!(None, tree.get("key_014", 51)?);

    // Keys outside range should survive
    assert!(tree.get("key_000", 51)?.is_some());
    assert!(tree.get("key_015", 51)?.is_some());
    assert!(tree.get("key_019", 51)?.is_some());

    Ok(())
}

// --- Test Q: Table-skip optimization triggers for fully-covered tables ---
#[test]
fn range_tombstone_table_skip_optimization() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Create a table with keys a-c
    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.flush_active_memtable(0)?;

    // Create a range tombstone that fully covers the table's key range
    // with higher seqno than any key in the table
    tree.remove_range("a", "d", 100);

    // The table [a,c] is fully covered by [a,d)@100 (100 > max_seqno=3)
    // Table-skip should allow skipping the entire table during iteration
    let keys = collect_keys(&tree, 101)?;
    assert!(keys.is_empty());

    // Reverse iteration should also skip
    let keys = collect_keys_rev(&tree, 101)?;
    assert!(keys.is_empty());

    Ok(())
}

// --- Test R: BlobTree range tombstone support ---
#[test]
fn range_tombstone_blob_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(
        lsm_tree::KvSeparationOptions::default()
            .separation_threshold(1)
            .compression(lsm_tree::CompressionType::None),
    ))
    .open()?;

    tree.insert("a", "value_a", 1);
    tree.insert("b", "value_b", 2);
    tree.insert("c", "value_c", 3);

    // Range tombstone in BlobTree
    tree.remove_range("a", "c", 10);

    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(Some("value_c".as_bytes().into()), tree.get("c", 11)?);

    // Flush and verify persistence
    tree.flush_active_memtable(0)?;

    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(Some("value_c".as_bytes().into()), tree.get("c", 11)?);

    Ok(())
}

// --- Test S: Invalid interval silently returns 0 ---
#[test]
fn range_tombstone_invalid_interval() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "1", 1);

    // start >= end — should be silently ignored
    let size = tree.remove_range("z", "a", 10);
    assert_eq!(0, size);

    // Equal start and end — also invalid
    let size = tree.remove_range("a", "a", 10);
    assert_eq!(0, size);

    // Data should still be visible
    assert_eq!(Some("1".as_bytes().into()), tree.get("a", 11)?);

    Ok(())
}

// --- Test T: Multiple compaction rounds preserve range tombstones ---
#[test]
fn range_tombstone_multiple_compaction_rounds() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Round 1: data + RT + flush + compact
    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.remove_range("a", "c", 10);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 0)?;

    // Round 2: add more data + flush + compact again
    tree.insert("d", "4", 11);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 0)?;

    // RT should survive both compaction rounds
    assert_eq!(None, tree.get("a", 12)?);
    assert_eq!(None, tree.get("b", 12)?);
    assert_eq!(Some("3".as_bytes().into()), tree.get("c", 12)?);
    assert_eq!(Some("4".as_bytes().into()), tree.get("d", 12)?);

    Ok(())
}

// --- Test: RT disjoint from memtable KV range persists through flush ---
// Regression test: delete_range targeting keys only in older SSTs must not be
// dropped during flush just because it doesn't overlap the memtable's KV range.
#[test]
fn range_tombstone_disjoint_from_flush_kv_range() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Write keys [x, y, z] and flush to SST (older data)
    tree.insert("x", "1", 1);
    tree.insert("y", "2", 2);
    tree.insert("z", "3", 3);
    tree.flush_active_memtable(0)?;

    // Now write keys [a, b] + delete_range("x", "zz") in a new memtable.
    // The RT is disjoint from the KV range [a, b] of this memtable.
    tree.insert("a", "4", 4);
    tree.insert("b", "5", 5);
    tree.remove_range("x", "zz", 10);
    tree.flush_active_memtable(0)?;

    // The RT must have survived flush and suppress [x, y, z] in the older SST
    assert_eq!(Some("4".as_bytes().into()), tree.get("a", 11)?);
    assert_eq!(Some("5".as_bytes().into()), tree.get("b", 11)?);
    assert_eq!(None, tree.get("x", 11)?);
    assert_eq!(None, tree.get("y", 11)?);
    assert_eq!(None, tree.get("z", 11)?);

    Ok(())
}

// --- Test: RT disjoint from KV range survives compaction ---
// After flush preserves the RT, compaction should merge it with the older SST
// and either suppress the keys or propagate the RT.
#[test]
fn range_tombstone_disjoint_survives_compaction() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Older data in SST
    tree.insert("x", "1", 1);
    tree.insert("y", "2", 2);
    tree.flush_active_memtable(0)?;

    // New memtable: KV in [a, b], RT covering [x, z) — disjoint from KV
    tree.insert("a", "3", 3);
    tree.insert("b", "4", 4);
    tree.remove_range("x", "z", 10);
    tree.flush_active_memtable(0)?;

    // Compact everything
    tree.major_compact(64_000_000, 0)?;

    // After compaction, [x, y] should still be suppressed
    assert_eq!(Some("3".as_bytes().into()), tree.get("a", 11)?);
    assert_eq!(Some("4".as_bytes().into()), tree.get("b", 11)?);
    assert_eq!(None, tree.get("x", 11)?);
    assert_eq!(None, tree.get("y", 11)?);

    Ok(())
}
