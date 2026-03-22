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

/// Helper to collect keys from a bounded range iterator.
fn collect_range_keys<R>(tree: &AnyTree, range: R, seqno: u64) -> lsm_tree::Result<Vec<Vec<u8>>>
where
    R: std::ops::RangeBounds<&'static str>,
{
    let mut keys = Vec::new();
    for item in tree.range(range, seqno, None) {
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

/// Helper to collect keys from a bounded reverse range iterator.
fn collect_range_keys_rev<R>(tree: &AnyTree, range: R, seqno: u64) -> lsm_tree::Result<Vec<Vec<u8>>>
where
    R: std::ops::RangeBounds<&'static str>,
{
    let mut keys = Vec::new();
    for item in tree.range(range, seqno, None).rev() {
        let k = item.key()?;
        keys.push(k.to_vec());
    }
    Ok(keys)
}

fn find_rt_table(tree: &AnyTree) -> lsm_tree::Table {
    tree.current_version()
        .iter_tables()
        .find(|table| table.regions.range_tombstones.is_some())
        .expect("expected RT-bearing table")
        .clone()
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

#[test]
fn remove_prefix_rejects_unbounded_prefix_without_partial_delete() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = open_tree(path);

        tree.insert(vec![0xFF], "ff", 1);
        tree.insert(vec![0xFF, 0x01], "ff01", 2);
        tree.insert("plain", "plain", 3);

        assert_eq!(0, tree.remove_prefix([], 10));
        assert_eq!(0, tree.remove_prefix(vec![0xFF], 11));

        assert_eq!(Some("ff".as_bytes().into()), tree.get(vec![0xFF], 12)?);
        assert_eq!(
            Some("ff01".as_bytes().into()),
            tree.get(vec![0xFF, 0x01], 12)?
        );
        assert_eq!(Some("plain".as_bytes().into()), tree.get("plain", 12)?);

        tree.flush_active_memtable(0)?;
    }

    {
        let tree = open_tree(path);

        assert_eq!(Some("ff".as_bytes().into()), tree.get(vec![0xFF], 12)?);
        assert_eq!(
            Some("ff01".as_bytes().into()),
            tree.get(vec![0xFF, 0x01], 12)?
        );
        assert_eq!(Some("plain".as_bytes().into()), tree.get("plain", 12)?);
    }

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
    assert!(
        tree.rotate_memtable().is_some(),
        "memtable with RT should seal"
    );
    assert!(tree.sealed_memtable_count() > 0);

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
    assert!(tree.table_count() <= 1, "major_compact should merge tables");

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

#[test]
fn range_tombstone_tampered_rt_block_fails_recovery() -> lsm_tree::Result<()> {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
    };

    let folder = get_tmp_folder();

    {
        let tree = open_tree(folder.path());
        tree.insert("a", "1", 1);
        tree.insert("b", "2", 2);
        tree.insert("c", "3", 3);
        tree.remove_range("a", "c", 10);
        tree.flush_active_memtable(0)?;

        let rt_table = find_rt_table(&tree);
        let rt_handle = rt_table
            .regions
            .range_tombstones
            .expect("expected range tombstone block");

        let mut file = OpenOptions::new().write(true).open(&*rt_table.path)?;
        let payload_pos = *rt_handle.offset()
            + u64::try_from(lsm_tree::table::block::Header::serialized_len())
                .expect("header size should fit in u64");
        file.seek(SeekFrom::Start(payload_pos))?;
        file.write_all(&u16::MAX.to_le_bytes())?;
        file.flush()?;
    }

    match Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()
    {
        Err(lsm_tree::Error::ChecksumMismatch { .. })
        | Err(lsm_tree::Error::Unrecoverable)
        | Err(lsm_tree::Error::RangeTombstoneDecode { .. }) => {}
        Err(other) => panic!(
            "expected ChecksumMismatch, Unrecoverable, or RangeTombstoneDecode, got: {other:?}"
        ),
        Ok(_) => panic!("tampered RT block must fail recovery, not reopen successfully"),
    }

    Ok(())
}

// --- Regression: RT-only table with sentinel survives recovery ---
// The sentinel WeakTombstone inflates item_count/tombstone_count metadata.
// Recovery must accept these tables without validation errors, and the
// RT must still suppress covered keys after reopen.
#[test]
fn rt_only_table_sentinel_survives_recovery() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = open_tree(path);
        tree.insert("a", "1", 1);
        tree.insert("b", "2", 2);
        tree.flush_active_memtable(0)?;

        // RT-only flush: sentinel written, counts inflated by +1
        tree.remove_range("a", "c", 10);
        tree.flush_active_memtable(0)?;
    }

    // Reopen — recovery must succeed despite sentinel in metadata
    let tree = open_tree(path);

    // RT must still suppress after recovery
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(Some("1".as_bytes().into()), tree.get("a", 5)?);

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

// --- Test N: Bottom-level compaction must keep RT-deleted keys hidden ---
#[test]
fn range_tombstone_bottom_level_compaction_keeps_deleted_keys_hidden() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "1", 1);
    tree.insert("b", "2", 2);
    tree.insert("c", "3", 3);
    tree.remove_range("a", "d", 10);
    tree.flush_active_memtable(0)?;

    // Before compaction: range tombstone suppresses all
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);

    // Major compact with GC watermark ABOVE the tombstone seqno.
    // We must not drop RTs yet because compaction does not physically remove
    // all covered KVs based on RT coverage alone.
    tree.major_compact(64_000_000, 11)?;

    // Deleted keys must stay hidden after bottom-level compaction.
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);

    // Newer writes must still win over the older RT.
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
    use lsm_tree::config::CompressionPolicy;
    use lsm_tree::CompressionType;

    let folder = get_tmp_folder();

    // Disable compression: repetitive payloads compress too well under lz4,
    // preventing MultiWriter rotation with the small 1 KiB target below.
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
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
    assert!(
        tree.table_count() > 1,
        "compaction with 1 KiB target should produce multiple output tables"
    );

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
    tree.flush_active_memtable(0)?;
    assert!(
        tree.table_count() >= 2,
        "table-skip regression should exercise SST-backed RT path"
    );

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

// --- Regression: RT-only table sentinel must not mask values in older SSTs ---
// When a memtable has only range tombstones (no KV data), flush produces an
// RT-only table with a synthetic sentinel at min(rt.start). If the sentinel
// seqno makes it visible before the RT's own seqno, point reads at intermediate
// snapshots incorrectly see a tombstone for that key, hiding real values in
// older tables.
#[test]
fn rt_only_sentinel_does_not_mask_older_values() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Older SST: real value at key "m" with seqno=5
    // Key "m" is chosen intentionally — it will be min(rt.start) for the RT below,
    // so the sentinel key collides with this real value.
    tree.insert("m", "real_value", 5);
    tree.flush_active_memtable(0)?;

    // RT-only memtable: delete_range [m, z) at seqno=20.
    // With a single RT, the sentinel uses that tombstone's start key "m".
    tree.remove_range("m", "z", 20);
    tree.flush_active_memtable(0)?;

    // Read at seqno=10: RT [m,z)@20 is NOT visible (20 > 10), so "m"@5
    // should be visible. The sentinel at ("m", sentinel_seqno) must NOT
    // act as a tombstone that hides the real value when the RT itself
    // is not yet visible.
    assert_eq!(
        Some("real_value".as_bytes().into()),
        tree.get("m", 10)?,
        "sentinel must not mask real value at key 'm' when RT is not yet visible"
    );

    // Read at seqno=21: RT [m,z)@20 IS visible → "m" suppressed by RT
    assert_eq!(None, tree.get("m", 21)?);

    Ok(())
}

// --- Regression: sentinel key/seqno must come from the same tombstone ---
// If the sentinel key comes from min(rt.start) but the seqno comes from a
// different tombstone with a lower seqno, the sentinel can become visible at a
// key that is not yet covered by any visible RT and mask older data.
#[test]
fn rt_only_sentinel_uses_lowest_seqno_tombstones_start_key() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "real_value", 5);
    tree.flush_active_memtable(0)?;

    // Two RTs in an RT-only flush:
    // - [m, z) @20 is the earliest visible tombstone
    // - [a, b) @30 provides the lexicographically smallest start key
    //
    // The sentinel must be written at "m"@20, not "a"@20.
    tree.remove_range("a", "b", 30);
    tree.remove_range("m", "z", 20);
    tree.flush_active_memtable(0)?;

    // At seqno 25 only [m, z) is visible, so key "a" must remain visible.
    assert_eq!(Some("real_value".as_bytes().into()), tree.get("a", 25)?);

    // Once [a, b) becomes visible, key "a" is suppressed as expected.
    assert_eq!(None, tree.get("a", 31)?);

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

// --- Regression: compaction clip must not drop RT covering gap between output tables ---
// major_compact merges all tables together so the gap scenario doesn't arise.
// Leveled compaction with overlap-based selection could produce a gap — tracked in #32.
#[test]
fn compaction_clip_preserves_rt_covering_gap_between_tables() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // L2: keys in the gap that the RT should suppress
    tree.insert("m", "old_m", 1);
    tree.insert("n", "old_n", 2);
    tree.insert("o", "old_o", 3);
    tree.flush_active_memtable(0)?;
    tree.major_compact(64_000_000, 0)?;

    // L1: keys on both sides of the gap, plus RT covering the gap
    tree.insert("a", "val_a", 10);
    tree.insert("l", "val_l", 11);
    tree.insert("q", "val_q", 12);
    tree.insert("z", "val_z", 13);
    tree.remove_range("m", "p", 20);
    tree.flush_active_memtable(0)?;

    // Compact L1 → should produce tables covering [a,l] and [q,z]
    // RT [m,p) must survive even though it falls in the gap
    tree.major_compact(64_000_000, 0)?;

    // Keys in the gap must still be suppressed by RT
    assert_eq!(
        None,
        tree.get("m", 21)?,
        "RT [m,p)@20 must suppress 'm' after compaction"
    );
    assert_eq!(
        None,
        tree.get("n", 21)?,
        "RT [m,p)@20 must suppress 'n' after compaction"
    );
    assert_eq!(
        None,
        tree.get("o", 21)?,
        "RT [m,p)@20 must suppress 'o' after compaction"
    );

    // Keys outside the gap must be fine
    assert_eq!(Some("val_a".as_bytes().into()), tree.get("a", 21)?);
    assert_eq!(Some("val_q".as_bytes().into()), tree.get("q", 21)?);

    Ok(())
}

// Regression: flush must finalize the last buffered KV block before widening
// table metadata for RT coverage. Otherwise Writer::finish would overwrite the
// widened key_range with the buffered block's last KV key and later point reads
// could not soundly reject unrelated SSTs by metadata.key_range.
#[test]
fn range_tombstone_disjoint_flush_key_range_tracks_rt_coverage() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("x", "1", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "2", 2);
    tree.insert("b", "3", 3);
    tree.remove_range("x", "zz", 10);
    tree.flush_active_memtable(0)?;

    let rt_table = find_rt_table(&tree);

    assert!(
        rt_table.metadata.key_range.contains_key(b"x"),
        "RT-bearing table metadata must conservatively include RT coverage"
    );
    assert_eq!(None, tree.get("x", 11)?);

    Ok(())
}

// --- Test: RT disjoint from KV range survives compaction ---
// Regression: disjoint RT (key range outside KV data) must survive
// multiple compaction rounds. Without key_range widening in flush mode,
// leveled compaction overlap selection would never pick up the table
// carrying the disjoint RT, leaving it permanently stuck.
#[test]
fn range_tombstone_disjoint_survives_multiple_compactions() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Older data in SST at low seqno
    tree.insert("x", "1", 1);
    tree.insert("y", "2", 2);
    tree.flush_active_memtable(0)?;

    // New memtable: KV in [a, b], RT covering [x, z) — disjoint from KV
    tree.insert("a", "3", 3);
    tree.insert("b", "4", 4);
    tree.remove_range("x", "z", 10);
    tree.flush_active_memtable(0)?;

    // Multiple compaction rounds — RT must propagate through all of them
    tree.major_compact(64_000_000, 0)?;
    tree.major_compact(64_000_000, 0)?;

    // After two compaction rounds, disjoint RT must still suppress [x, y]
    assert_eq!(Some("3".as_bytes().into()), tree.get("a", 11)?);
    assert_eq!(Some("4".as_bytes().into()), tree.get("b", 11)?);
    assert_eq!(None, tree.get("x", 11)?);
    assert_eq!(None, tree.get("y", 11)?);

    // Also verify via range iteration
    let keys = collect_keys(&tree, 11)?;
    assert_eq!(keys, vec![b"a", b"b"]);

    Ok(())
}

#[test]
#[ignore = "allocates ~68 MiB to force MultiWriter rotation — run with --ignored"]
fn range_tombstone_multi_table_flush_keeps_newer_values_reachable() -> lsm_tree::Result<()> {
    use lsm_tree::config::CompressionPolicy;
    use lsm_tree::CompressionType;

    let folder = get_tmp_folder();
    // Disable compression: large repetitive payloads compress to almost nothing
    // under lz4/zstd, preventing MultiWriter rotation and making the test flaky.
    let tree = Config::new(
        folder.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(CompressionType::None))
    .open()
    .expect("should open");

    // Standard-tree flush uses a fixed 64 MiB MultiWriter target. Multiple
    // large early values force rotation before the later "y" write, reproducing
    // the widened-key-range bug on the actual flush path instead of a synthetic
    // test-only configuration. Reuse one large buffer to keep the test lighter
    // on CI memory.
    let mi = 1_024 * 1_024;
    let large_value = "a".repeat(17 * mi);

    tree.insert("a0", &large_value, 1);
    tree.insert("a1", &large_value, 2);
    tree.insert("b0", &large_value, 3);
    tree.insert("b1", &large_value, 4);
    tree.remove_range("x", "zz", 10);
    tree.insert("y", "visible_newer", 20);
    tree.flush_active_memtable(0)?;

    assert!(tree.table_count() > 1, "test requires a rotated flush");
    assert_eq!(Some("visible_newer".as_bytes().into()), tree.get("y", 21)?);

    Ok(())
}

#[test]
fn range_tombstone_suppresses_bulk_ingested_values() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    {
        let mut ingestion = tree.ingestion()?;
        ingestion.write("k", "old")?;
        ingestion.finish()?;
    }

    tree.remove_range("k", "l", 10);
    tree.flush_active_memtable(0)?;

    assert_eq!(None, tree.get("k", 11)?);

    Ok(())
}

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

// Regression: range iteration should only carry RTs that overlap the requested
// range. Narrow scans over untouched keys must keep returning those keys, while
// overlapping scans still honor the persisted RT.
#[test]
fn range_tombstone_narrow_range_queries_respect_overlap() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("x", "1", 1);
    tree.insert("y", "2", 2);
    tree.flush_active_memtable(0)?;

    tree.insert("a", "3", 3);
    tree.insert("b", "4", 4);
    tree.remove_range("x", "z", 10);
    tree.flush_active_memtable(0)?;

    assert_eq!(collect_range_keys(&tree, "a"..="b", 11)?, vec![b"a", b"b"]);
    assert_eq!(
        collect_range_keys(&tree, "x"..="z", 11)?,
        Vec::<Vec<u8>>::new()
    );

    Ok(())
}

#[test]
fn range_tombstone_memtable_narrow_range_queries_ignore_disjoint_rt() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("x", "1", 1);
    tree.insert("y", "2", 2);
    tree.remove_range("x", "z", 10);

    tree.insert("a", "3", 3);
    tree.insert("b", "4", 4);

    assert_eq!(collect_range_keys(&tree, "a"..="b", 11)?, vec![b"a", b"b"]);
    assert_eq!(
        collect_range_keys(&tree, "x"..="z", 11)?,
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        collect_range_keys_rev(&tree, "a"..="b", 11)?,
        vec![b"b", b"a"]
    );
    assert_eq!(
        collect_range_keys_rev(&tree, "x"..="z", 11)?,
        Vec::<Vec<u8>>::new()
    );

    Ok(())
}

// --- Separate KV/RT seqno bounds ---

/// Tables that contain both KVs and range tombstones should track
/// separate `highest_kv_seqno`. This enables table-skip for a covering
/// RT stored in the same table: `rt.seqno > highest_kv_seqno` can be
/// true even when `rt.seqno <= highest_seqno`.
#[test]
fn kv_seqno_excludes_range_tombstone_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // KVs at seqno 1..4
    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);
    tree.insert("c", "val_c", 3);
    tree.insert("d", "val_d", 4);

    // RT at seqno 10 — higher than any KV
    tree.remove_range("a", "z", 10);

    // Flush everything into a single SST
    tree.flush_active_memtable(0)?;

    let table = find_rt_table(&tree);

    // highest_seqno includes RT seqno (10)
    assert_eq!(table.get_highest_seqno(), 10);
    // highest_kv_seqno excludes RT — only KVs (max is 4)
    assert_eq!(table.get_highest_kv_seqno(), 4);

    // Invariant: KV-only seqno must not exceed overall max
    assert!(table.get_highest_kv_seqno() <= table.get_highest_seqno());

    Ok(())
}

/// Without range tombstones, highest_kv_seqno equals highest_seqno
/// (all items are KV entries, none are RTs).
#[test]
fn kv_seqno_equals_overall_when_no_range_tombstones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);
    tree.insert("c", "val_c", 3);

    tree.flush_active_memtable(0)?;

    let table = tree
        .current_version()
        .iter_tables()
        .next()
        .expect("should have one table")
        .clone();

    assert_eq!(table.get_highest_seqno(), 3);
    assert_eq!(table.get_highest_kv_seqno(), 3);

    Ok(())
}

/// RT-only table: highest_kv_seqno is 0 because no KV items exist
/// (only the sentinel entry which has its seqno restored after write).
#[test]
fn kv_seqno_zero_for_rt_only_table() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // Only an RT, no KV inserts
    tree.remove_range("a", "z", 10);

    tree.flush_active_memtable(0)?;

    let table = find_rt_table(&tree);

    // Overall seqno includes the RT
    assert_eq!(table.get_highest_seqno(), 10);
    // KV-only seqno is 0 — sentinel seqno is restored to pre-write state
    assert_eq!(table.get_highest_kv_seqno(), 0);

    Ok(())
}

/// When a covering range tombstone and its covered KVs are colocated in the
/// same table, reads at a higher seqno should not observe those KVs.
/// This verifies that the colocated range tombstone correctly suppresses
/// the covered keys for range scans (forward and reverse) and point lookups.
#[test]
fn colocated_range_tombstone_suppresses_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    // KVs at seqno 1..3
    tree.insert("a", "val_a", 1);
    tree.insert("b", "val_b", 2);
    tree.insert("c", "val_c", 3);

    // Covering RT [a, z) at seqno 10 — in the same memtable
    tree.remove_range("a", "z", 10);

    // Flush: both KVs and RT go into one SST
    tree.flush_active_memtable(0)?;

    // Range scan at seqno 11 — all keys suppressed
    assert_eq!(collect_keys(&tree, 11)?, Vec::<Vec<u8>>::new());
    // Reverse scan too
    assert_eq!(collect_keys_rev(&tree, 11)?, Vec::<Vec<u8>>::new());

    // Point reads also suppressed
    assert_eq!(None, tree.get("a", 11)?);
    assert_eq!(None, tree.get("b", 11)?);
    assert_eq!(None, tree.get("c", 11)?);

    Ok(())
}

/// Binary search correctness: covering RT with start exactly at table min key.
#[test]
fn table_skip_rt_start_equals_table_min() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("m", "val_m", 1);
    tree.insert("n", "val_n", 2);
    tree.insert("o", "val_o", 3);

    // RT starts exactly at "m" (table min)
    tree.remove_range("m", "p", 10);
    tree.flush_active_memtable(0)?;

    assert_eq!(collect_keys(&tree, 11)?, Vec::<Vec<u8>>::new());
    assert_eq!(None, tree.get("m", 11)?);
    assert_eq!(None, tree.get("n", 11)?);
    assert_eq!(None, tree.get("o", 11)?);

    Ok(())
}

/// Point-read binary search: multiple RTs in a table, only one covers the key.
#[test]
fn point_read_binary_search_multiple_rts() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open_tree(folder.path());

    tree.insert("a", "val_a", 1);
    tree.insert("d", "val_d", 2);
    tree.insert("g", "val_g", 3);
    tree.insert("j", "val_j", 4);

    // Two disjoint RTs
    tree.remove_range("a", "c", 10); // covers "a"
    tree.remove_range("g", "i", 11); // covers "g"

    tree.flush_active_memtable(0)?;

    // "a" suppressed by first RT
    assert_eq!(None, tree.get("a", 12)?);
    // "d" not covered by any RT
    assert_eq!(Some("val_d".as_bytes().into()), tree.get("d", 12)?);
    // "g" suppressed by second RT
    assert_eq!(None, tree.get("g", 12)?);
    // "j" not covered
    assert_eq!(Some("val_j".as_bytes().into()), tree.get("j", 12)?);

    Ok(())
}

#[test]
fn range_tombstone_disjoint_survives_recovery_for_narrow_scans() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = open_tree(path);

        tree.insert("x", "1", 1);
        tree.insert("y", "2", 2);
        tree.flush_active_memtable(0)?;

        tree.insert("a", "3", 3);
        tree.insert("b", "4", 4);
        tree.remove_range("x", "z", 10);
        tree.flush_active_memtable(0)?;
    }

    let tree = open_tree(path);

    assert_eq!(Some("3".as_bytes().into()), tree.get("a", 11)?);
    assert_eq!(Some("4".as_bytes().into()), tree.get("b", 11)?);
    assert_eq!(None, tree.get("x", 11)?);
    assert_eq!(None, tree.get("y", 11)?);

    assert_eq!(collect_range_keys(&tree, "a"..="b", 11)?, vec![b"a", b"b"]);
    assert_eq!(
        collect_range_keys(&tree, "x"..="z", 11)?,
        Vec::<Vec<u8>>::new()
    );
    assert_eq!(
        collect_range_keys_rev(&tree, "a"..="b", 11)?,
        vec![b"b", b"a"]
    );
    assert_eq!(
        collect_range_keys_rev(&tree, "x"..="z", 11)?,
        Vec::<Vec<u8>>::new()
    );

    Ok(())
}
