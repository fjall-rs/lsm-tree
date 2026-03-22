use super::*;
use crate::{AbstractTree, Config, SequenceNumberCounter, MAX_SEQNO};
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
        assert!(tree.get([b'k', i].as_slice(), MAX_SEQNO)?.is_some());
    }
    // Latest visible versions should be the last written values
    assert_eq!(
        tree.get("a", MAX_SEQNO)?.as_deref(),
        Some([b'v', 2].as_slice()),
    );
    assert_eq!(
        tree.get("z", MAX_SEQNO)?.as_deref(),
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
        tree.get("key", MAX_SEQNO)?.as_deref(),
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

// --- Dynamic Leveling Tests ---

#[test]
fn dynamic_leveling_empty() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Dynamic leveling on an empty tree should behave like static (DoNothing)
    let strategy = Arc::new(Strategy::default().with_dynamic_level_bytes(true));
    tree.compact(strategy, 0)?;

    assert_eq!(0, tree.table_count());
    Ok(())
}

#[test]
fn dynamic_leveling_basic_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert enough overlapping data to trigger L0→L1 compaction
    for i in 0..4u8 {
        tree.insert("a", "v", u64::from(i));
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.insert("z", "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_dynamic_level_bytes(true));
    tree.compact(strategy, 4)?;

    // Compaction should still work — data should be merged
    assert!(tree.table_count() < 4, "tables should be compacted");

    // All data should be readable
    for i in 0..4u8 {
        assert!(tree.get([b'k', i].as_slice(), MAX_SEQNO)?.is_some());
    }

    Ok(())
}

#[test]
fn dynamic_leveling_data_integrity() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(
        Strategy::default()
            .with_dynamic_level_bytes(true)
            .with_table_target_size(1), // tiny tables to force many small output tables under dynamic leveling
    );

    // Insert and compact multiple rounds
    for round in 0..5u64 {
        for k in 0..4u8 {
            tree.insert(
                "a",
                format!("r{round}").as_bytes(),
                round * 4 + u64::from(k),
            );
            tree.insert(
                [b'k', k].as_slice(),
                format!("r{round}").as_bytes(),
                round * 4 + u64::from(k),
            );
            tree.insert(
                "z",
                format!("r{round}").as_bytes(),
                round * 4 + u64::from(k),
            );
            tree.flush_active_memtable(round * 4 + u64::from(k))?;
        }
        tree.compact(strategy.clone(), (round + 1) * 4)?;
    }

    // All data should be readable with latest values
    for k in 0..4u8 {
        let val = tree.get([b'k', k].as_slice(), MAX_SEQNO)?;
        assert!(val.is_some(), "key k{k} should exist");
        assert_eq!(val.as_deref(), Some(b"r4".as_slice()));
    }

    Ok(())
}

// --- Multi-Level Compaction Tests ---

#[test]
fn multi_level_no_skip_when_l1_has_room() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert enough to trigger L0→L1 but L1 should be under its target
    for i in 0..4u8 {
        tree.insert("a", "v", u64::from(i));
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.insert("z", "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let strategy = Arc::new(Strategy::default().with_multi_level(true));
    tree.compact(strategy, 4)?;

    // L1 has room — normal L0→L1 compaction, not multi-level skip
    // Data should still be compacted and readable
    assert!(tree.table_count() < 4);

    // Verify data went to L1 (not skipped to L2) — L2 should be empty
    assert!(
        tree.current_version()
            .level(2)
            .map_or(true, |l| l.is_empty()),
        "L2 should remain empty when L1 has room (no multi-level skip)",
    );

    for i in 0..4u8 {
        assert!(tree.get([b'k', i].as_slice(), MAX_SEQNO)?.is_some());
    }

    Ok(())
}

#[test]
fn multi_level_data_integrity() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(
        Strategy::default()
            .with_multi_level(true)
            .with_table_target_size(1), // tiny tables to stress test
    );

    // Run multiple rounds of inserts + compaction
    for round in 0..8u64 {
        for k in 0..4u8 {
            tree.insert(
                "a",
                format!("r{round}").as_bytes(),
                round * 4 + u64::from(k),
            );
            tree.insert(
                [b'k', k].as_slice(),
                format!("r{round}").as_bytes(),
                round * 4 + u64::from(k),
            );
            tree.insert(
                "z",
                format!("r{round}").as_bytes(),
                round * 4 + u64::from(k),
            );
            tree.flush_active_memtable(round * 4 + u64::from(k))?;
        }
        tree.compact(strategy.clone(), (round + 1) * 4)?;
    }

    // All keys readable with latest values
    for k in 0..4u8 {
        let val = tree.get([b'k', k].as_slice(), MAX_SEQNO)?;
        assert!(val.is_some(), "key k{k} should exist");
        assert_eq!(val.as_deref(), Some(b"r7".as_slice()));
    }

    Ok(())
}

// --- Coverage: get_config, get_name, builder methods ---

#[test]
fn leveled_get_name() {
    use crate::compaction::CompactionStrategy;
    let strategy = Strategy::default();
    assert_eq!(strategy.get_name(), "LeveledCompaction");
}

#[test]
fn leveled_get_config_includes_new_fields() {
    use crate::compaction::CompactionStrategy;
    let strategy = Strategy::default()
        .with_dynamic_level_bytes(true)
        .with_multi_level(true);

    let config = strategy.get_config();

    let keys: Vec<_> = config.iter().map(|(k, _)| k.as_ref()).collect();
    assert!(
        keys.iter().any(|k| k == b"leveled_dynamic"),
        "should have leveled_dynamic key",
    );
    assert!(
        keys.iter().any(|k| k == b"leveled_multi_level"),
        "should have leveled_multi_level key",
    );
    assert!(
        keys.iter().any(|k| k == b"leveled_l0_threshold"),
        "should have leveled_l0_threshold key",
    );
    assert!(
        keys.iter().any(|k| k == b"leveled_target_size"),
        "should have leveled_target_size key",
    );
    assert!(
        keys.iter().any(|k| k == b"leveled_level_ratio_policy"),
        "should have leveled_level_ratio_policy key",
    );
}

#[test]
fn leveled_builder_chaining() {
    use crate::compaction::CompactionStrategy;
    // Exercise all builder methods to cover their code paths
    let strategy = Strategy::default()
        .with_l0_threshold(8)
        .with_table_target_size(32 * 1024 * 1024)
        .with_level_ratio_policy(vec![8.0, 10.0])
        .with_dynamic_level_bytes(true)
        .with_multi_level(true);

    let config = strategy.get_config();
    assert!(!config.is_empty());
    assert_eq!(strategy.get_name(), "LeveledCompaction");
}

// --- Coverage: dynamic leveling with enough data to exercise backward computation ---

#[test]
fn dynamic_leveling_multiple_levels() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(
        Strategy::default()
            .with_dynamic_level_bytes(true)
            .with_l0_threshold(4)
            .with_table_target_size(1), // tiny tables → many levels get populated
    );

    let mut seqno = 0u64;

    // Multiple rounds of flush + compact to push data through levels
    for _round in 0..12 {
        for _k in 0..4 {
            tree.insert("a", "val", seqno);
            tree.insert(format!("key_{seqno}").as_bytes(), "val", seqno);
            tree.insert("z", "val", seqno);
            tree.flush_active_memtable(seqno)?;
            seqno += 1;
        }
        // Run compaction multiple times per round to propagate through levels
        for _ in 0..3 {
            tree.compact(strategy.clone(), seqno)?;
        }
    }

    // All keys should be readable
    for s in 0..seqno {
        assert!(
            tree.get(format!("key_{s}").as_bytes(), MAX_SEQNO)?
                .is_some(),
            "key_{s} should exist",
        );
    }

    Ok(())
}

// --- Coverage: multi-level compaction with oversized L1 ---

#[test]
fn multi_level_skip_fires_when_l1_oversized() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Step 1: Fill L1 with overlapping data using normal leveled compaction.
    // Use default target_size (64MiB) so data stays in L1 instead of
    // trivial-moving to Lmax (which happens with target_size=1).
    let leveled = Arc::new(Strategy::default().with_l0_threshold(4));

    let mut seqno = 0u64;

    for _round in 0..3 {
        for _k in 0..4 {
            tree.insert("a", "val", seqno);
            tree.insert(format!("k_{seqno}").as_bytes(), "val", seqno);
            tree.insert("z", "val", seqno);
            tree.flush_active_memtable(seqno)?;
            seqno += 1;
        }
        tree.compact(leveled.clone(), seqno)?;
    }

    // Step 2: Switch to tiny target_size so L1 target (target_size * l0_threshold
    // = 64 * 4 = 256 bytes) is small enough that any real table in L1 causes
    // l1_score > 1.0, but L0 with 8+ tables still wins overall scoring
    // (L0 score = tables/threshold = 8/4 = 2.0 > l1_score ≈ 1.x).
    // This makes level_idx_with_highest_score == 0 AND l1_score > 1.0,
    // which is the condition for multi-level skip to fire.
    let multi = Arc::new(
        Strategy::default()
            .with_multi_level(true)
            .with_table_target_size(64)
            .with_l0_threshold(4),
    );

    // Flush 8 overlapping tables so L0 scores 2.0 (8/4)
    for _k in 0..8 {
        tree.insert("a", "val", seqno);
        tree.insert(format!("k_{seqno}").as_bytes(), "val", seqno);
        tree.insert("z", "val", seqno);
        tree.flush_active_memtable(seqno)?;
        seqno += 1;
    }

    // Single compact should trigger multi-level: L0 wins scoring,
    // L1 is oversized (real table > 256 byte target) → L0+L1→L2 skip
    let result = tree.compact(multi.clone(), seqno)?;

    // compact() now returns CompactionResult so we can assert the merge path fired
    assert_eq!(
        result.action,
        crate::compaction::CompactionAction::Merged,
        "multi-level skip should produce a Merged action",
    );
    assert!(
        result.dest_level.is_some_and(|lvl| lvl >= 2),
        "multi-level skip should target L2+, got {:?}",
        result.dest_level,
    );

    // Data MUST have propagated beyond L1 into deeper levels.
    let version = tree.current_version();
    let has_deep_data =
        (2..version.level_count()).any(|idx| version.level(idx).is_some_and(|l| !l.is_empty()));
    assert!(
        has_deep_data,
        "data should exist in L2+ after compaction with multi_level",
    );

    // All data should be readable
    for s in 0..seqno {
        assert!(
            tree.get(format!("k_{s}").as_bytes(), MAX_SEQNO)?.is_some(),
            "key k_{s} should exist",
        );
    }

    Ok(())
}

// --- Coverage: dynamic fallback to static when tree is small ---

#[test]
fn dynamic_leveling_fallback_to_static() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // With large target_size, dynamic L1 target will be smaller than
    // level_base_size, triggering the static fallback path.
    let strategy = Arc::new(
        Strategy::default()
            .with_dynamic_level_bytes(true)
            .with_table_target_size(64 * 1024 * 1024) // 64 MiB — large
            .with_l0_threshold(4),
    );

    // Flush a small amount of data — dynamic targets will be tiny
    // compared to level_base_size (64M * 4 = 256M), so the dynamic path
    // falls back to static targets. Exercising this code path ensures
    // compute_level_targets covers the fallback branch.
    for i in 0..4u8 {
        tree.insert("a", "v", u64::from(i));
        tree.insert([b'k', i].as_slice(), "v", u64::from(i));
        tree.insert("z", "v", u64::from(i));
        tree.flush_active_memtable(u64::from(i))?;
    }

    let result = tree.compact(strategy, 4)?;
    assert_ne!(
        result.action,
        crate::compaction::CompactionAction::Nothing,
        "compaction should have done work",
    );

    // Data should be compacted and readable
    for i in 0..4u8 {
        assert!(tree.get([b'k', i].as_slice(), MAX_SEQNO)?.is_some());
    }

    Ok(())
}

#[test]
fn multi_level_with_both_flags() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Test that both dynamic + multi_level work together
    let strategy = Arc::new(
        Strategy::default()
            .with_dynamic_level_bytes(true)
            .with_multi_level(true)
            .with_table_target_size(1),
    );

    let mut seqno = 0u64;

    for _round in 0..10 {
        for _k in 0..4 {
            tree.insert("a", "val", seqno);
            tree.insert(format!("key_{seqno}").as_bytes(), "val", seqno);
            tree.insert("z", "val", seqno);
            tree.flush_active_memtable(seqno)?;
            seqno += 1;
        }
        for _ in 0..3 {
            tree.compact(strategy.clone(), seqno)?;
        }
    }

    // All data readable
    for s in 0..seqno {
        assert!(
            tree.get(format!("key_{s}").as_bytes(), MAX_SEQNO)?
                .is_some(),
            "key_{s} should exist",
        );
    }

    Ok(())
}
