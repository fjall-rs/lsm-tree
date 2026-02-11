use lsm_tree::{AbstractTree, Config, Guard, SeqNo, SequenceNumberCounter};

fn open_tree(folder: &std::path::Path) -> lsm_tree::AnyTree {
    Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    .unwrap()
}

// --- Test A: Range tombstone suppresses point reads in memtable ---
#[test]
fn range_tombstone_memtable_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert some keys at seqno 1..5
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    // Insert range tombstone [b, e) at seqno 10
    tree.remove_range("b", "e", 10);

    // "a" should be visible (outside range)
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    // "b", "c", "d" should be suppressed (inside [b,e))
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());
    assert!(tree.get("d", SeqNo::MAX)?.is_none());
    // "e" should be visible (end is exclusive)
    assert!(tree.get("e", SeqNo::MAX)?.is_some());

    Ok(())
}

// --- Test B: Range tombstone suppresses in range iteration ---
#[test]
fn range_tombstone_memtable_range_iter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    tree.remove_range("b", "e", 10);

    let keys: Vec<_> = tree
        .range::<&str, _>(.., SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();

    // Only "a" and "e" should survive
    assert_eq!(2, keys.len());
    assert_eq!(b"a", &*keys[0]);
    assert_eq!(b"e", &*keys[1]);

    Ok(())
}

// --- Test C: Range tombstone survives flush to SST ---
#[test]
fn range_tombstone_survives_flush() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    tree.remove_range("b", "e", 10);

    tree.flush_active_memtable(0)?;
    assert_eq!(1, tree.table_count());

    // Point reads after flush
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());
    assert!(tree.get("d", SeqNo::MAX)?.is_none());
    assert!(tree.get("e", SeqNo::MAX)?.is_some());

    // Range iteration after flush
    let count = tree.range::<&str, _>(.., SeqNo::MAX, None).count();
    assert_eq!(2, count);

    Ok(())
}

// --- Test D: Range tombstone survives compaction ---
#[test]
fn range_tombstone_survives_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    tree.remove_range("b", "e", 10);

    tree.flush_active_memtable(0)?;
    tree.major_compact(u64::MAX, 0)?;

    // Point reads after compaction
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());
    assert!(tree.get("d", SeqNo::MAX)?.is_none());
    assert!(tree.get("e", SeqNo::MAX)?.is_some());

    Ok(())
}

// --- Test E: End-exclusive semantics ---
#[test]
fn range_tombstone_end_exclusive() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    tree.insert("l", "val", 1);
    tree.insert("m", "val", 2);

    // Range tombstone [a, m) at seqno 10
    tree.remove_range("a", "m", 10);

    // "l" is suppressed (inside [a,m))
    assert!(tree.get("l", SeqNo::MAX)?.is_none());
    // "m" is NOT suppressed (end exclusive)
    assert!(tree.get("m", SeqNo::MAX)?.is_some());

    Ok(())
}

// --- Test F: Reverse iteration with range tombstone ---
#[test]
fn range_tombstone_reverse_iteration() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    tree.remove_range("b", "e", 10);

    let keys: Vec<_> = tree
        .range::<&str, _>(.., SeqNo::MAX, None)
        .rev()
        .map(|g| g.key().unwrap())
        .collect();

    // Reverse: "e", "a"
    assert_eq!(2, keys.len());
    assert_eq!(b"e", &*keys[0]);
    assert_eq!(b"a", &*keys[1]);

    Ok(())
}

// --- Test G: MVCC visibility — tombstone only suppresses older versions ---
#[test]
fn range_tombstone_mvcc_visibility() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert key "b" at seqno 5
    tree.insert("b", "old", 5);

    // Range tombstone [a, c) at seqno 10
    tree.remove_range("a", "c", 10);

    // Insert key "b" at seqno 15 (newer than tombstone)
    tree.insert("b", "new", 15);

    // "b" should be visible because seqno 15 > tombstone seqno 10
    let val = tree.get("b", SeqNo::MAX)?;
    assert!(val.is_some());
    assert_eq!(b"new", &*val.unwrap());

    // At seqno 11: key "b"@15 not visible (15 >= 11), key "b"@5 suppressed by tombstone@10
    let val = tree.get("b", 11)?;
    assert!(val.is_none());

    Ok(())
}

// --- Test H: Multiple overlapping range tombstones ---
#[test]
fn range_tombstone_overlapping() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    for (i, key) in ["a", "b", "c", "d", "e", "f"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    tree.remove_range("a", "d", 10);
    tree.remove_range("c", "f", 10);

    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());
    assert!(tree.get("d", SeqNo::MAX)?.is_none());
    assert!(tree.get("e", SeqNo::MAX)?.is_none());
    // "f" is NOT suppressed (end exclusive)
    assert!(tree.get("f", SeqNo::MAX)?.is_some());

    Ok(())
}

// --- Test I: Range tombstone across memtable and SST ---
#[test]
fn range_tombstone_across_memtable_and_sst() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert keys and flush to SST
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }
    tree.flush_active_memtable(0)?;

    // Insert range tombstone in active memtable (newer seqno)
    tree.remove_range("b", "e", 10);

    // SST keys should be suppressed by memtable tombstone
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());
    assert!(tree.get("d", SeqNo::MAX)?.is_none());
    assert!(tree.get("e", SeqNo::MAX)?.is_some());

    // Range iteration should also work
    assert_eq!(2, tree.range::<&str, _>(.., SeqNo::MAX, None).count());

    Ok(())
}

// --- Test J: Range tombstone persists through compaction when GC threshold is not met ---
#[test]
fn range_tombstone_persists_below_gc_threshold() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    for (i, key) in ["a", "b", "c"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    tree.remove_range("a", "d", 10);

    tree.flush_active_memtable(0)?;

    // Compact with gc watermark BELOW the tombstone seqno — tombstone should survive
    tree.major_compact(u64::MAX, 5)?;

    // Data should still be suppressed because tombstone is preserved
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());

    Ok(())
}

// --- Test K: Range tombstone in SST also suppresses point reads ---
#[test]
fn range_tombstone_sst_point_read() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert data at low seqno
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    // Insert tombstone and flush everything together
    tree.remove_range("b", "e", 10);

    tree.flush_active_memtable(0)?;

    // Now insert new data at higher seqno and flush to a second SST
    tree.insert("b", "new_val", 20);
    tree.flush_active_memtable(0)?;

    // "b" should be visible (seqno 20 > tombstone seqno 10)
    assert!(tree.get("b", SeqNo::MAX)?.is_some());
    assert_eq!(b"new_val", &*tree.get("b", SeqNo::MAX)?.unwrap());

    // "c" should still be suppressed
    assert!(tree.get("c", SeqNo::MAX)?.is_none());

    Ok(())
}

// --- Test L: Table skip — tombstone in memtable covers entire SST ---
#[test]
fn range_tombstone_table_skip_in_iteration() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert data and flush to SST
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }
    tree.flush_active_memtable(0)?;

    // Now insert a range tombstone in the active memtable that covers the entire SST
    // with seqno higher than any KV in the SST
    tree.remove_range("a", "f", 100);

    // Insert some new data outside the tombstone range
    tree.insert("z", "val", 200);

    // Range iteration should skip the fully-covered SST and only return "z"
    let keys: Vec<_> = tree
        .range::<&str, _>(.., SeqNo::MAX, None)
        .map(|g| g.key().unwrap())
        .collect();

    assert_eq!(1, keys.len());
    assert_eq!(b"z", &*keys[0]);

    Ok(())
}

// --- Test M: Tombstone eviction at bottom level makes data visible again ---
#[test]
fn range_tombstone_eviction_makes_data_visible() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert data at low seqnos
    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);
    tree.insert("c", "val_c", 3);

    // Insert range tombstone [a, d) at seqno 10
    tree.remove_range("a", "d", 10);

    // Flush everything to SST
    tree.flush_active_memtable(0)?;

    // Verify data is suppressed before eviction
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());

    // Compact at last level with gc_watermark > tombstone seqno
    // This should evict the range tombstone
    tree.major_compact(u64::MAX, 20)?;

    // After eviction, data should be visible again because the values
    // are the only version of their keys and survive compaction
    assert!(tree.get("a", SeqNo::MAX)?.is_some());
    assert_eq!(b"val_a", &*tree.get("a", SeqNo::MAX)?.unwrap());
    assert!(tree.get("b", SeqNo::MAX)?.is_some());
    assert_eq!(b"val_b", &*tree.get("b", SeqNo::MAX)?.unwrap());
    assert!(tree.get("c", SeqNo::MAX)?.is_some());
    assert_eq!(b"val_c", &*tree.get("c", SeqNo::MAX)?.unwrap());

    Ok(())
}

// --- Test N: Fast path does not suppress data when no range tombstones exist ---
#[test]
fn range_tombstone_fast_path_no_tombstones() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert normal KVs at various seqnos — no range tombstones at all
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    // Point reads should all succeed (fast path must not accidentally suppress)
    for key in ["a", "b", "c", "d", "e"] {
        assert!(
            tree.get(key, SeqNo::MAX)?.is_some(),
            "key {key} missing in memtable"
        );
    }

    // Range iteration should return all 5 keys
    assert_eq!(5, tree.range::<&str, _>(.., SeqNo::MAX, None).count());

    // Flush to SST and verify again
    tree.flush_active_memtable(0)?;

    for key in ["a", "b", "c", "d", "e"] {
        assert!(
            tree.get(key, SeqNo::MAX)?.is_some(),
            "key {key} missing after flush"
        );
    }
    assert_eq!(5, tree.range::<&str, _>(.., SeqNo::MAX, None).count());

    // Compact and verify again
    tree.major_compact(u64::MAX, 0)?;

    for key in ["a", "b", "c", "d", "e"] {
        assert!(
            tree.get(key, SeqNo::MAX)?.is_some(),
            "key {key} missing after compaction"
        );
    }
    assert_eq!(5, tree.range::<&str, _>(.., SeqNo::MAX, None).count());

    Ok(())
}

// --- Test O: Compaction deduplicates redundant overlapping range tombstones ---
#[test]
fn range_tombstone_compaction_dedup() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = open_tree(folder.path());

    // Insert data
    for (i, key) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        tree.insert(*key, "val", i as SeqNo);
    }

    // Insert overlapping range tombstones:
    // [a, e) at seqno 10  — covers everything
    // [b, d) at seqno 8   — fully covered by the first (subset range, lower seqno)
    // [a, c) at seqno 10  — fully covered by the first (subset range, equal seqno)
    tree.remove_range("a", "e", 10);
    tree.remove_range("b", "d", 8);
    tree.remove_range("a", "c", 10);

    tree.flush_active_memtable(0)?;

    // Compact — dedup should remove the redundant tombstones
    tree.major_compact(u64::MAX, 0)?;

    // Data should still be correctly suppressed
    assert!(tree.get("a", SeqNo::MAX)?.is_none());
    assert!(tree.get("b", SeqNo::MAX)?.is_none());
    assert!(tree.get("c", SeqNo::MAX)?.is_none());
    assert!(tree.get("d", SeqNo::MAX)?.is_none());
    // "e" is outside [a, e) (end exclusive)
    assert!(tree.get("e", SeqNo::MAX)?.is_some());

    Ok(())
}
