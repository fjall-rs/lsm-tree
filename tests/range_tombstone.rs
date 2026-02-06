use lsm_tree::{
    AbstractTree, Config, Guard, RangeTombstone, SeqNo, SequenceNumberCounter,
};

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
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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

    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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

    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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

    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("a".into(), "m".into(), 10));

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

    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("a".into(), "c".into(), 10));

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

    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("a".into(), "d".into(), 10));
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("c".into(), "f".into(), 10));

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
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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

    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("a".into(), "d".into(), 10));

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
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("b".into(), "e".into(), 10));

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
    tree.active_memtable()
        .insert_range_tombstone(RangeTombstone::new("a".into(), "f".into(), 100));

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
