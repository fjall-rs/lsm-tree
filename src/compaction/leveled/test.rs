use super::*;
use crate::{AbstractTree, Config, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use test_log::test;

#[test]
fn leveled_empty_levels() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 0)?;

    assert_eq!(0, tree.table_count());
    Ok(())
}

#[test]
fn leveled_l0_below_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "v", 0);
        tree.flush_active_memtable(0)?;
    }

    let before = tree.table_count();
    assert_eq!(3, before);

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 0)?;

    assert_eq!(before, tree.table_count());

    Ok(())
}

#[test]
fn leveled_intra_l0_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 3 overlapping memtables with distinct values (below configured l0_threshold=4)
    for i in 0..3u8 {
        tree.insert("a", [b'v', i].as_slice(), u64::from(i));
        tree.insert([b'k', i].as_slice(), "v", 0);
        tree.insert("z", [b'v', i].as_slice(), u64::from(i));
        tree.flush_active_memtable(0)?;
    }

    assert_eq!(3, tree.table_count());
    assert!(
        tree.l0_run_count() > 1,
        "L0 should have multiple overlapping runs"
    );

    let strategy = Arc::new(
        Strategy::default()
            .with_l0_threshold(4)
            .with_table_target_size(128 * 1024 * 1024),
    );
    tree.compact(strategy, 0)?;

    // Intra-L0 compaction should consolidate runs within L0
    assert_eq!(
        1,
        tree.l0_run_count(),
        "L0 should have exactly 1 run after intra-L0 compaction"
    );
    assert_eq!(
        1,
        tree.table_count(),
        "Tables should be merged into 1 after intra-L0 compaction"
    );

    // All data must still be readable with correct values
    for i in 0..3u8 {
        assert!(tree.get([b'k', i].as_slice(), SeqNo::MAX)?.is_some());
    }
    // Latest visible versions should be the last written values
    assert_eq!(
        tree.get("a", SeqNo::MAX)?.as_deref(),
        Some([b'v', 2].as_slice()),
    );
    assert_eq!(
        tree.get("z", SeqNo::MAX)?.as_deref(),
        Some([b'v', 2].as_slice()),
    );

    // Verify data stayed in L0 (not pushed to L1)
    assert!(
        tree.current_version()
            .level(1)
            .map_or(true, |l| l.is_empty()),
        "L1 should remain empty after intra-L0 compaction"
    );

    Ok(())
}

#[test]
fn leveled_intra_l0_preserves_newer_run_ordering() -> crate::Result<()> {
    // This test exercises the `with_merge` ordering fix directly: when an intra-L0
    // merge produces a result, any L0 run NOT in `old_ids` (i.e., concurrently flushed
    // during compaction) must remain at the front of L0 so it is searched first.
    //
    // We simulate the concurrent-flush scenario by:
    // 1. Creating 3 L0 runs (newest at front)
    // 2. Calling `Version::with_merge` with only the 2 older runs' IDs in `old_ids`
    // 3. Verifying the newest run (not in old_ids) stays at position 0 in L0
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 3 overlapping memtables into L0
    tree.insert("key", "oldest", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("key", "middle", 1);
    tree.flush_active_memtable(0)?;
    tree.insert("key", "newest", 2);
    tree.flush_active_memtable(0)?;

    assert_eq!(3, tree.l0_run_count());

    let version = tree.current_version();
    let l0 = version.l0();
    // L0 runs are ordered newest-first: [newest_run, middle_run, oldest_run]
    assert_eq!(3, l0.run_count());

    // Collect table IDs from the 2 OLDER runs (index 1 and 2) — these are the ones
    // that would have been selected for intra-L0 compaction before the newest flush
    let older_ids: Vec<_> = l0
        .iter()
        .skip(1) // skip the newest run
        .flat_map(|run| run.iter())
        .map(|t| t.id())
        .collect();

    // Use the tables from the oldest run as the "merged output" (simulating the
    // compaction result — in reality it would be a newly written table, but for
    // ordering verification any table works)
    let merged_tables: Vec<_> = l0.iter().last().unwrap().iter().cloned().collect();

    // Record the newest run's table IDs (the "concurrently flushed" run)
    let newest_run_ids: Vec<_> = l0.iter().next().unwrap().iter().map(|t| t.id()).collect();

    // Call with_merge targeting L0 — this is the code path that previously used
    // `runs.insert(0, run)` which would incorrectly place the merged (older) run
    // BEFORE the concurrently flushed newer run
    let new_version = version.with_merge(
        &older_ids,
        &merged_tables,
        0, // dest_level = 0 (intra-L0)
        None,
        vec![],
        &Default::default(),
    );

    let new_l0 = new_version.l0();
    // Should have 2 runs: the untouched newest run + the merged older run
    assert_eq!(
        2,
        new_l0.run_count(),
        "L0 should have 2 runs: newest (untouched) + merged (older)"
    );

    // The FIRST run in L0 must be the newest (concurrently flushed) run, not the
    // merged run. This is what the `runs.push(run)` fix ensures — without it,
    // `runs.insert(0, run)` would place the merged run first, causing stale reads.
    let first_run_ids: Vec<_> = new_l0
        .iter()
        .next()
        .unwrap()
        .iter()
        .map(|t| t.id())
        .collect();

    assert_eq!(
        newest_run_ids, first_run_ids,
        "newest (concurrently flushed) L0 run must remain at front after intra-L0 merge"
    );

    Ok(())
}

#[test]
fn leveled_l0_reached_limit() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        // NOTE: Tables need to overlap
        tree.insert("a", "v", 0);
        tree.insert([b'k', i].as_slice(), "v", 0);
        tree.insert("z", "v", 0);
        tree.flush_active_memtable(0)?;
    }

    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 0)?;

    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn leveled_l0_reached_limit_disjoint() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", 0);
        tree.flush_active_memtable(0)?;
    }

    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 0)?;

    assert_eq!(4, tree.table_count());

    Ok(())
}

#[test]
fn leveled_l0_reached_limit_disjoint_l1() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..4 {
        // NOTE: Tables need to overlap
        tree.insert("a", "v", i);
        tree.insert("b", "v", i);
        tree.flush_active_memtable(0)?;
    }

    let fifo = Arc::new(Strategy::default());
    tree.compact(fifo, 0)?;

    assert_eq!(1, tree.table_count());

    for i in 0..4u8 {
        tree.insert([b'k', i].as_slice(), "v", 0);
        tree.flush_active_memtable(0)?;
    }

    assert_eq!(5, tree.table_count());

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 0)?;

    assert_eq!(5, tree.table_count());

    Ok(())
}

#[test]
#[expect(clippy::unwrap_used)]
fn leveled_sequential_inserts() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(Strategy {
        target_size: 1,
        ..Default::default()
    });

    let mut table_count = 0;

    for k in 0u64..100 {
        table_count += 1;

        tree.insert(k.to_be_bytes(), "", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(table_count, tree.table_count());
        tree.compact(strategy.clone(), 0)?;
        assert_eq!(table_count, tree.table_count());

        for idx in 1..=5 {
            assert_eq!(
                0,
                tree.current_version().level(idx).unwrap().len(),
                "no tables should be in intermediary level (L{idx})",
            );
        }
    }

    Ok(())
}
