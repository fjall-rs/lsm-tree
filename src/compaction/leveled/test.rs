use super::*;
use crate::{AbstractTree, Config, SequenceNumberCounter};
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
        assert!(tree.get([b'k', i].as_slice(), u64::MAX)?.is_some());
    }
    // Latest visible versions should be the last written values
    assert_eq!(
        tree.get("a", u64::MAX)?.as_deref(),
        Some([b'v', 2].as_slice()),
    );
    assert_eq!(
        tree.get("z", u64::MAX)?.as_deref(),
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
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 2 overlapping memtables (below l0_threshold=4)
    tree.insert("key", "old_1", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("key", "old_2", 1);
    tree.flush_active_memtable(0)?;

    assert_eq!(2, tree.l0_run_count());

    // Intra-L0 compaction merges the 2 runs
    let strategy = Arc::new(
        Strategy::default()
            .with_l0_threshold(4)
            .with_table_target_size(128 * 1024 * 1024),
    );
    tree.compact(strategy, 0)?;
    assert_eq!(1, tree.l0_run_count());

    // Flush a newer memtable AFTER compaction — this run must be searched first
    tree.insert("key", "newest", 2);
    tree.flush_active_memtable(0)?;

    assert_eq!(2, tree.l0_run_count());

    // The newest flush must win: merged (older) run is appended, newer run is at front
    assert_eq!(
        tree.get("key", u64::MAX)?.as_deref(),
        Some(b"newest".as_slice()),
        "newer L0 run must be found before merged (older) run"
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
