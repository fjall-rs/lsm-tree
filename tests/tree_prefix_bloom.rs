use lsm_tree::{
    config::PinningPolicy, AbstractTree, Config, Guard, KvSeparationOptions, PrefixExtractor,
    SequenceNumberCounter, Tree,
};
use std::sync::Arc;

/// Extracts prefixes at each ':' separator boundary.
struct ColonSeparatedPrefix;

impl PrefixExtractor for ColonSeparatedPrefix {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b':')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

/// Asserts that L0 contains at least one run with `min_tables` tables.
///
/// Panics with a descriptive message if the largest L0 run is too small.
fn assert_l0_multi_table_run(tree: &Tree, min_tables: usize) {
    let version = tree.current_version();
    let l0 = version.level(0).expect("L0 should exist");
    let max_run_len = l0.iter().map(|r| r.len()).max().unwrap_or(0);
    assert!(
        max_run_len >= min_tables,
        "expected L0 run with >={min_tables} tables, \
         but largest run has {max_run_len} table(s)",
    );
}

fn tree_with_prefix_bloom(folder: &tempfile::TempDir) -> lsm_tree::Result<Tree> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ColonSeparatedPrefix))
    .open()?;

    match tree {
        lsm_tree::AnyTree::Standard(t) => Ok(t),
        _ => panic!("expected standard tree"),
    }
}

#[test]
fn prefix_bloom_basic_prefix_scan() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Insert keys with different prefixes
    tree.insert("user:1:name", "Alice", 0);
    tree.insert("user:1:email", "alice@example.com", 1);
    tree.insert("user:2:name", "Bob", 2);
    tree.insert("order:1:item", "widget", 3);
    tree.insert("order:2:item", "gadget", 4);

    // Flush to create SST with prefix bloom
    tree.flush_active_memtable(0)?;

    // Prefix scan should find matching keys
    let results: Vec<_> = tree
        .create_prefix("user:1:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"user:1:email");
    assert_eq!(results[1].0.as_ref(), b"user:1:name");

    // Different prefix
    let results: Vec<_> = tree
        .create_prefix("order:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    // Non-existent prefix
    let results: Vec<_> = tree
        .create_prefix("nonexist:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_skips_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create first segment with "user:" prefix keys
    tree.insert("user:1:name", "Alice", 0);
    tree.insert("user:2:name", "Bob", 1);
    tree.flush_active_memtable(0)?;

    // Create second segment with "order:" prefix keys
    tree.insert("order:1:item", "widget", 2);
    tree.insert("order:2:item", "gadget", 3);
    tree.flush_active_memtable(0)?;

    assert!(tree.table_count() >= 2, "expected at least 2 segments");

    // Prefix scan for "user:" should return correct results
    // and skip the "order:" segment via prefix bloom
    let results: Vec<_> = tree
        .create_prefix("user:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"user:1:name");
    assert_eq!(results[1].0.as_ref(), b"user:2:name");

    // Prefix scan for "order:" should return correct results
    // and skip the "user:" segment via prefix bloom
    let results: Vec<_> = tree
        .create_prefix("order:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    Ok(())
}

#[test]
fn prefix_bloom_after_compaction() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create data across multiple flushes
    tree.insert("a:1", "v1", 0);
    tree.insert("b:1", "v2", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("a:2", "v3", 2);
    tree.insert("c:1", "v4", 3);
    tree.flush_active_memtable(0)?;

    // Compact everything
    tree.major_compact(u64::MAX, 0)?;

    // Prefix scan still works after compaction
    let results: Vec<_> = tree
        .create_prefix("a:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"a:1");
    assert_eq!(results[1].0.as_ref(), b"a:2");

    Ok(())
}

#[test]
fn prefix_bloom_without_extractor_still_works() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Tree without prefix extractor — prefix scan still works, just no bloom skipping
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    match &tree {
        lsm_tree::AnyTree::Standard(t) => {
            t.insert("user:1:name", "Alice", 0);
            t.insert("user:2:name", "Bob", 1);
            t.flush_active_memtable(0)?;

            let results: Vec<_> = t
                .create_prefix("user:", 2, None)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(results.len(), 2);
        }
        _ => panic!("expected standard tree"),
    }

    Ok(())
}

#[test]
fn prefix_bloom_hierarchical_prefixes() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Insert keys with hierarchical prefixes
    tree.insert("adj:out:42:KNOWS", "target1", 0);
    tree.insert("adj:out:42:LIKES", "target2", 1);
    tree.insert("adj:out:99:KNOWS", "target3", 2);
    tree.insert("adj:in:42:KNOWS", "source1", 3);
    tree.insert("node:42", "properties", 4);
    tree.flush_active_memtable(0)?;

    // Scan at different prefix levels
    // "adj:" matches all adjacency keys
    let results: Vec<_> = tree
        .create_prefix("adj:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 4);

    // "adj:out:" matches outgoing adjacency
    let results: Vec<_> = tree
        .create_prefix("adj:out:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 3);

    // "adj:out:42:" matches specific node's outgoing edges
    let results: Vec<_> = tree
        .create_prefix("adj:out:42:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    // "node:" matches node properties
    let results: Vec<_> = tree
        .create_prefix("node:", 5, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);

    Ok(())
}

#[test]
fn prefix_bloom_with_memtable_and_disk() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Write some data to disk
    tree.insert("x:1", "disk_val", 0);
    tree.insert("y:1", "disk_val", 1);
    tree.flush_active_memtable(0)?;

    // Write more to memtable (not flushed)
    tree.insert("x:2", "mem_val", 2);
    tree.insert("z:1", "mem_val", 3);

    // Prefix scan should find both disk and memtable results
    let results: Vec<_> = tree
        .create_prefix("x:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"x:1");
    assert_eq!(results[1].0.as_ref(), b"x:2");

    Ok(())
}

#[test]
fn prefix_bloom_unpinned_filter() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create tree with unpinned filters to exercise the fallback load path
    // in Table::maybe_contains_prefix
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ColonSeparatedPrefix))
    .filter_block_pinning_policy(PinningPolicy::all(false))
    .filter_block_partitioning_policy(PinningPolicy::all(false))
    .open()?;

    let tree = match tree {
        lsm_tree::AnyTree::Standard(t) => t,
        _ => panic!("expected standard tree"),
    };

    tree.insert("a:1", "v1", 0);
    tree.insert("b:1", "v2", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("c:1", "v3", 2);
    tree.insert("d:1", "v4", 3);
    tree.flush_active_memtable(0)?;

    // Compact to L1 — prefix bloom check only applies to single-table runs.
    // With unpinned filters, maybe_contains_prefix hits the regions.filter path.
    tree.major_compact(u64::MAX, 0)?;

    // Prefix scan on unpinned filters exercises the load_block fallback
    let results: Vec<_> = tree
        .create_prefix("a:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.as_ref(), b"a:1");

    // Non-matching prefix should be skipped via unpinned bloom
    let results: Vec<_> = tree
        .create_prefix("z:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_blob_tree() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    // Create BlobTree with prefix extractor to exercise BlobTree::prefix path
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .prefix_extractor(Arc::new(ColonSeparatedPrefix))
    .with_kv_separation(Some(KvSeparationOptions::default()))
    .open()?;

    assert!(
        matches!(&tree, lsm_tree::AnyTree::Blob(_)),
        "expected BlobTree variant"
    );

    tree.insert("user:1:name", "Alice", 0);
    tree.insert("user:2:name", "Bob", 1);
    tree.insert("order:1:item", "widget", 2);
    tree.flush_active_memtable(0)?;

    // Prefix scan through BlobTree — collect results to surface I/O errors
    let results: Vec<_> = tree
        .prefix("user:", 3, None)
        .map(|g| g.key())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    let results: Vec<_> = tree
        .prefix("order:", 3, None)
        .map(|g| g.key())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 1);

    let results: Vec<_> = tree
        .prefix("nonexist:", 3, None)
        .map(|g| g.key())
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_many_disjoint_segments() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create 10 segments each with a unique prefix — exercises the bloom skip
    // path repeatedly across many single-table runs
    for i in 0u64..10 {
        let key = format!("ns{i}:key");
        tree.insert(key, "value", i);
        tree.flush_active_memtable(0)?;
    }

    assert!(tree.table_count() >= 10);

    // Each prefix scan should find exactly 1 result and skip 9 segments
    for i in 0u64..10 {
        let prefix = format!("ns{i}:");
        let results: Vec<_> = tree
            .create_prefix(&prefix, 10, None)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results.len(),
            1,
            "prefix {prefix} should match exactly 1 key"
        );
    }

    // A prefix that doesn't exist should match nothing
    let results: Vec<_> = tree
        .create_prefix("nonexist:", 10, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_skip_on_compacted_levels() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // To exercise the bloom-skip Ok(false) branch we need L1+ tables where:
    //   1. The on-disk key_range overlaps the scan prefix (so the table
    //      passes the key_range check and is considered a candidate), but
    //   2. The bloom filter for that table returns Ok(false) for the scan
    //      prefix (so we can safely skip I/O for that table).
    //
    // We create many tables in L1 that all contain keys under the same broad
    // "data:" namespace but with varying suffixes. The prefix extractor
    // (ColonSeparatedPrefix) inserts only the prefixes that actually occur in
    // the table into the bloom filter (e.g., "data:", "data:0:", ...). The
    // test then scans for a synthetically chosen prefix under "data:" that
    // lies within the tables' key_range but is not one of the prefixes that
    // ever appeared as a key prefix in any table. As a result, each table's
    // key_range overlaps the scan prefix, but its bloom filter correctly
    // rejects the lookup and returns Ok(false).

    let mut seqno = 0u64;

    // Flush 5 batches each with 100 keys under "data:" prefix, different
    // suffixes so compaction produces multiple L1 tables.
    let val = "x".repeat(64);
    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("data:{batch}:{i:04}");
            tree.insert(key, val.as_str(), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0)?;
    }

    // Compact with modest target_size so compaction produces multiple L1 tables
    // without rotating on nearly every key. Each table covers a sub-range of
    // "data:" keys, enabling bloom-skip for prefixes that fall in the key_range
    // but were never indexed.
    tree.major_compact(4 * 1024, 0)?;

    let table_count = tree.table_count();
    assert!(
        table_count >= 2,
        "compaction must produce >=2 tables for bloom skip to apply, got {table_count}",
    );

    // Scanning "data:" should find all 500 keys.
    let results: Vec<_> = tree
        .create_prefix("data:", seqno, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 500);

    // Scanning "data:2:" should find exactly 100 keys from batch 2.
    let results: Vec<_> = tree
        .create_prefix("data:2:", seqno, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 100);

    // "other:" has no keys and its key_range doesn't overlap — skipped by key_range check.
    let results: Vec<_> = tree
        .create_prefix("other:", seqno, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    // "data:3x:" falls WITHIN the table's key_range [data:0:*, data:4:*]
    // (lexicographically between "data:3:" and "data:4:") but was never
    // written — it is a valid extractor boundary that no key ever produced.
    // This exercises the Ok(false) bloom-skip branch: key_range says "yes"
    // but bloom correctly says "no".
    let results: Vec<_> = tree
        .create_prefix("data:3x:", seqno, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[test]
fn prefix_bloom_non_boundary_prefix_no_false_negative() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // The ColonSeparatedPrefix extractor indexes "adj:", "adj:out:", etc.
    // A scan for "adj" (no trailing colon) is NOT an extractor boundary,
    // so prefix bloom must NOT be used (it would cause false negatives).
    // The scan must still return correct results via key-range filtering.
    tree.insert("adj:out:1", "v1", 0);
    tree.insert("adj:in:1", "v2", 1);
    tree.insert("other:1", "v3", 2);
    tree.flush_active_memtable(0)?;

    // "adj" is not a colon-terminated boundary — bloom skip must be disabled
    let results: Vec<_> = tree
        .create_prefix("adj", 3, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        results.len(),
        2,
        "must find keys even though 'adj' is not an extractor boundary"
    );

    // "adj:" IS a valid boundary — bloom skip can be used safely
    let results: Vec<_> = tree
        .create_prefix("adj:", 3, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    Ok(())
}

#[test]
fn prefix_bloom_negative_lookup_in_key_range_gap() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create a single table with keys spanning a wide range but only two
    // prefix groups: "aaa:" and "zzz:". After flush, the table's key_range
    // is [aaa:0, zzz:19]. Scanning "mmm:" overlaps the key_range but "mmm:"
    // was never indexed in the bloom → Ok(false) branch fires.
    for i in 0..10 {
        tree.insert(format!("aaa:{i}"), "v", i);
    }
    for i in 10..20 {
        tree.insert(format!("zzz:{i}"), "v", i);
    }
    tree.flush_active_memtable(0)?;

    // Compact to L1 for single-table runs (bloom check only applies there).
    tree.major_compact(u64::MAX, 0)?;

    assert!(
        tree.table_count() >= 1,
        "expected at least 1 table after compaction, got {}",
        tree.table_count(),
    );

    // "mmm:" falls in the key_range [aaa:0, zzz:19] but was never written.
    // This test asserts that a prefix scan for "mmm:" returns no keys.
    // With 20 keys at 10 bpk, the bloom is ~200 bits, so in practice it will
    // usually reject such random prefixes, but false positives are still possible.
    let results: Vec<_> = tree
        .create_prefix("mmm:", 20, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    // Verify the real prefixes still work
    let results: Vec<_> = tree
        .create_prefix("aaa:", 20, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 10);

    let results: Vec<_> = tree
        .create_prefix("zzz:", 20, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 10);

    Ok(())
}

/// Multi-table runs (typically L0) now support per-table prefix bloom
/// skipping. This test creates multiple flushes WITHOUT compaction so
/// the tables remain in a single multi-table L0 run, then verifies
/// that prefix scans still return correct results (tables whose bloom
/// reports no match are skipped transparently).
#[test]
fn prefix_bloom_multi_table_run_skipping() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Flush 4 batches — each batch has a distinct prefix group.
    // Without compaction these stay in L0 as a multi-table run.
    tree.insert("alpha:1", "v1", 0);
    tree.insert("alpha:2", "v2", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("beta:1", "v3", 2);
    tree.insert("beta:2", "v4", 3);
    tree.flush_active_memtable(0)?;

    tree.insert("gamma:1", "v5", 4);
    tree.insert("gamma:2", "v6", 5);
    tree.flush_active_memtable(0)?;

    tree.insert("delta:1", "v7", 6);
    tree.insert("delta:2", "v8", 7);
    tree.flush_active_memtable(0)?;

    // Verify L0 contains a fused multi-table run (4 disjoint flushes).
    assert_l0_multi_table_run(&tree, 4);

    // Each prefix scan should find exactly 2 keys — the bloom filter
    // skips tables that definitely don't contain the queried prefix.
    for prefix in &["alpha:", "beta:", "gamma:", "delta:"] {
        let results: Vec<_> = tree
            .create_prefix(prefix, 8, None)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results.len(),
            2,
            "prefix '{prefix}' should match exactly 2 keys",
        );
    }

    // Non-existent prefix returns nothing.
    let results: Vec<_> = tree
        .create_prefix("omega:", 8, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    Ok(())
}

/// Exercises the bloom Ok(false) path within a multi-table run.
///
/// When a table's key range overlaps the prefix scan bounds but its
/// bloom filter correctly reports the prefix as absent, the table must
/// be skipped. This is distinct from the key-range guard (which is a
/// cheaper metadata-only check) and requires the bloom to be consulted.
#[test]
fn prefix_bloom_multi_table_run_bloom_rejection() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Create two disjoint tables that will be fused into one multi-table
    // run by optimize_runs. Each table spans a wide key range so that
    // a prefix scan for a non-existent prefix overlaps the table's
    // key_range but gets rejected by the bloom.
    //
    // Table 1: keys "a:*" and "c:*"
    //   → key_range = [a:1, c:9]
    // Table 2: keys "d:*" and "f:*"
    //   → key_range = [d:1, f:9]
    //
    // These are lexicographically disjoint (c:9 < d:1) so optimize_runs
    // fuses them into a single multi-table run.
    //
    // Scanning "b:" has bounds [b:, b;). Table 1 overlaps (a:1 < b: < c:9),
    // but "b:" was never written → bloom returns Ok(false).
    //
    // False-positive note: each table has 18 keys + 9 prefixes = 27 hashes
    // at the default 10 bits-per-key, giving a bloom with ~270 bits. The
    // probability that a single random probe returns a false positive is
    // ≈0.8% — negligible for a deterministic test with fixed keys.
    for i in 1..=9 {
        tree.insert(format!("a:{i}"), "v", i - 1);
        tree.insert(format!("c:{i}"), "v", 9 + i - 1);
    }
    tree.flush_active_memtable(0)?;

    for i in 1..=9 {
        tree.insert(format!("d:{i}"), "v", 18 + i - 1);
        tree.insert(format!("f:{i}"), "v", 27 + i - 1);
    }
    tree.flush_active_memtable(0)?;

    // Verify L0 has a multi-table run (disjoint tables fused).
    assert_l0_multi_table_run(&tree, 2);

    // "b:" overlaps table 1's key range [a:1, c:9] but isn't in its bloom.
    // This exercises the Ok(false) bloom rejection path in the multi-table
    // run filter (not just the key-range guard).
    let results: Vec<_> = tree
        .create_prefix("b:", 36, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    // "e:" overlaps table 2's key range [d:1, f:9] but isn't in its bloom.
    let results: Vec<_> = tree
        .create_prefix("e:", 36, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 0);

    // Real prefixes still work through the multi-table run.
    let results: Vec<_> = tree
        .create_prefix("a:", 36, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 9);

    let results: Vec<_> = tree
        .create_prefix("d:", 36, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 9);

    Ok(())
}

/// Exercises the multi-table run path where 2+ tables survive both
/// the key-range guard and bloom check (the `_ =>` match arm that
/// constructs a new Run from survivors).
///
/// Tables share a common broad prefix ("ns:") but have disjoint
/// sub-prefixes ("ns:a:", "ns:b:", "ns:c:"). Scanning "ns:" matches
/// all tables' blooms, keeping 3 survivors in the multi-table path.
#[test]
fn prefix_bloom_multi_table_run_multiple_survivors() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // 3 disjoint flushes that share the broad prefix "ns:".
    tree.insert("ns:a:1", "v1", 0);
    tree.insert("ns:a:2", "v2", 1);
    tree.flush_active_memtable(0)?;

    tree.insert("ns:b:1", "v3", 2);
    tree.insert("ns:b:2", "v4", 3);
    tree.flush_active_memtable(0)?;

    tree.insert("ns:c:1", "v5", 4);
    tree.insert("ns:c:2", "v6", 5);
    tree.flush_active_memtable(0)?;

    // Verify L0 has a multi-table run.
    assert_l0_multi_table_run(&tree, 3);

    // Scanning "ns:" matches ALL 3 tables' blooms (all indexed "ns:"
    // at write time). All 3 pass key-range and bloom → surviving.len() >= 2
    // → hits the `_ =>` branch that builds a new Run from survivors.
    let results: Vec<_> = tree
        .create_prefix("ns:", 6, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 6);

    // Narrow prefix: only 1 table survives → demoted to single-table path.
    let results: Vec<_> = tree
        .create_prefix("ns:b:", 6, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    Ok(())
}

/// Verify that prefix bloom skipping works correctly with overlapping
/// key ranges at L0 (where tables may overlap). Two flushes with
/// interleaved keys ensure the tables' key ranges overlap, and prefix
/// bloom filtering must still produce correct results. Because the
/// key ranges overlap, `optimize_runs` keeps them as separate
/// single-table runs (not fused into one multi-table run).
#[test]
fn prefix_bloom_overlapping_l0_tables() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // First flush: mix of prefixes
    tree.insert("user:1:name", "Alice", 0);
    tree.insert("order:1:item", "widget", 1);
    tree.flush_active_memtable(0)?;

    // Second flush: overlapping key range with different prefix mix
    tree.insert("user:2:name", "Bob", 2);
    tree.insert("order:2:item", "gadget", 3);
    tree.flush_active_memtable(0)?;

    assert!(tree.table_count() >= 2);

    // Both flushes contain "user:" keys — prefix scan must find all of them
    let results: Vec<_> = tree
        .create_prefix("user:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.as_ref(), b"user:1:name");
    assert_eq!(results[1].0.as_ref(), b"user:2:name");

    // Both flushes contain "order:" keys
    let results: Vec<_> = tree
        .create_prefix("order:", 4, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    Ok(())
}

/// Verifies the `prefix_bloom_skips` metric is incremented when bloom filters
/// reject prefixes that fall inside a table's key_range.
///
/// Creates a single L0 table (= single-table run, where bloom check applies)
/// with a wide key range, then performs many prefix scans for non-existent
/// prefixes. With 1000 keys and 10 bits-per-key the FP rate per query is ~1%,
/// so out of 24 distinct non-existent prefixes at least some must be rejected
/// by the bloom filter, producing a non-zero skip count.
#[cfg(feature = "metrics")]
#[test]
fn prefix_bloom_skip_metrics() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // Insert 500 keys under "aaa:" and 500 under "zzz:" to create a single
    // table with key_range [aaa:0000, zzz:0499] and a large bloom filter.
    let mut seqno = 0u64;
    for i in 0..500 {
        tree.insert(format!("aaa:{i:04}"), "v", seqno);
        seqno += 1;
    }
    for i in 0..500 {
        tree.insert(format!("zzz:{i:04}"), "v", seqno);
        seqno += 1;
    }
    tree.flush_active_memtable(0)?;

    // The test relies on the single-table prefix-bloom fast path
    // (run.len() == 1) in TreeIter::create_range. Fail early if flush
    // produces multiple tables so metric failures reflect a real bloom
    // regression rather than layout differences.
    assert_eq!(
        tree.table_count(),
        1,
        "expected single-table run; flush produced {} tables",
        tree.table_count()
    );

    assert_eq!(tree.metrics().prefix_bloom_skips(), 0);

    // Scan for 24 non-existent prefixes that fall inside the key_range.
    // Each prefix is a valid extractor boundary (ends with ':').
    let scan_nonexistent_prefixes = || -> lsm_tree::Result<()> {
        for c in b'b'..=b'y' {
            let prefix = format!("{}:", c as char);
            let results: Vec<_> = tree
                .create_prefix(&prefix, seqno, None)
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(results.len(), 0, "prefix '{prefix}' should match no keys");
        }
        Ok(())
    };

    // This is a probabilistic smoke test: we issue many lookups for prefixes
    // that do not exist but fall within the table's key range. For any
    // reasonable prefix-bloom configuration with a non-zero false-positive
    // rate, we expect at least one of these lookups to be fully filtered by
    // the bloom (counted as a skip). To make the test robust against rare
    // all-false-positive runs or configuration changes, we retry the scan a
    // generous number of times before failing.
    const MAX_SCAN_ATTEMPTS: u32 = 20;

    for _attempt in 0..MAX_SCAN_ATTEMPTS {
        scan_nonexistent_prefixes()?;
        if tree.metrics().prefix_bloom_skips() > 0 {
            return Ok(());
        }
    }

    let skips = tree.metrics().prefix_bloom_skips();
    assert!(
        skips > 0,
        "expected at least one prefix bloom skip out of 24 non-existent prefix scans \
         after {MAX_SCAN_ATTEMPTS} attempts, got {skips}"
    );

    Ok(())
}

/// Verifies `prefix_bloom_skips` stays at zero when no bloom filtering occurs.
///
/// Without a prefix extractor, no prefix hash is computed — the bloom check is
/// never reached, and the counter must remain zero.
#[cfg(feature = "metrics")]
#[test]
fn prefix_bloom_skip_metrics_zero_without_extractor() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let tree = match tree {
        lsm_tree::AnyTree::Standard(t) => t,
        _ => panic!("expected standard tree"),
    };

    tree.insert("user:1", "v", 0);
    tree.insert("user:2", "v", 1);
    tree.flush_active_memtable(0)?;

    let results: Vec<_> = tree
        .create_prefix("user:", 2, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 2);

    assert_eq!(
        tree.metrics().prefix_bloom_skips(),
        0,
        "no bloom skips should occur without a prefix extractor"
    );

    Ok(())
}

/// Prefix scan correctness when an L0 single-table run has a wide key range
/// that overlaps the scanned prefix but does not contain any matching keys.
/// Ensures the non-matching table does not affect scan results, independent of
/// whether the prefix bloom happens to reject the scanned prefix.
#[test]
fn prefix_scan_l0_wide_non_matching_table_does_not_affect_results() -> lsm_tree::Result<()> {
    let folder = tempfile::tempdir()?;
    let tree = tree_with_prefix_bloom(&folder)?;

    // L0 table 1: keys with prefixes "aaa:" and "zzz:" — wide key range.
    // Bloom contains "aaa:" and "zzz:" but NOT "mmm:".
    for i in 0..5 {
        tree.insert(format!("aaa:{i}"), "v", i);
    }
    for i in 5..10 {
        tree.insert(format!("zzz:{i}"), "v", i);
    }
    tree.flush_active_memtable(0)?;

    // L0 table 2: keys with prefix "mmm:" — bloom contains "mmm:".
    for i in 10..15 {
        tree.insert(format!("mmm:{i}"), "v", i);
    }
    tree.flush_active_memtable(0)?;

    let results: Vec<_> = tree
        .create_prefix("mmm:", 15, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 5);

    // Verify existing prefixes still work
    let results: Vec<_> = tree
        .create_prefix("aaa:", 15, None)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(results.len(), 5);

    Ok(())
}
