// Tests for per-level Fs routing (tiered storage).
//
// Verifies that tables are written to the correct directory based on their
// destination level, and that recovery discovers tables across all paths.

use lsm_tree::{
    config::{CompressionPolicy, LevelRoute},
    fs::StdFs,
    AbstractTree, Config, SequenceNumberCounter,
};
use std::sync::Arc;

/// Helper: create a 3-tier config (hot L0-L1 / warm L2-L4 / cold L5-L6).
fn three_tier_config(base: &std::path::Path) -> Config {
    let hot = base.join("hot");
    let warm = base.join("warm");

    Config::new(
        base.join("primary"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .level_routes(vec![
        LevelRoute {
            levels: 0..2,
            path: hot,
            fs: Arc::new(StdFs),
        },
        LevelRoute {
            levels: 2..5,
            path: warm,
            fs: Arc::new(StdFs),
        },
        // L5-L6: falls back to primary path
    ])
}

#[test]
fn flush_writes_to_hot_tier() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = three_tier_config(dir.path());
    let tree = config.open()?;

    tree.insert("a", "value_a", 0);
    tree.flush_active_memtable(0)?;

    // L0 flush → hot tier
    let hot_tables = dir.path().join("hot").join("tables");
    let files: Vec<_> = std::fs::read_dir(&hot_tables)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map_or(false, |n| n.parse::<u64>().is_ok())
        })
        .collect();

    assert!(
        !files.is_empty(),
        "expected table files in hot tier ({hot_tables:?}), found none"
    );

    // Primary tables folder should be empty (no L5-L6 tables yet)
    let primary_tables = dir.path().join("primary").join("tables");
    let primary_files: Vec<_> = std::fs::read_dir(&primary_tables)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map_or(false, |n| n.parse::<u64>().is_ok())
        })
        .collect();

    assert!(
        primary_files.is_empty(),
        "expected no table files in primary tier, found {}",
        primary_files.len()
    );

    Ok(())
}

#[test]
fn compaction_writes_to_correct_tier() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = three_tier_config(dir.path());
    let tree = config.open()?;

    // Insert enough data, flush to L0
    for i in 0u64..20 {
        tree.insert(format!("key{i:04}"), "x".repeat(100), i);
        if i % 4 == 3 {
            tree.flush_active_memtable(0)?;
        }
    }

    // Force compaction to last level (cold tier = primary, L6)
    tree.major_compact(u64::MAX, u64::MAX)?;

    // After major compaction, all tables should be at L6 (primary/cold tier)
    let primary_tables = dir.path().join("primary").join("tables");
    let primary_count = std::fs::read_dir(&primary_tables)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map_or(false, |n| n.parse::<u64>().is_ok())
        })
        .count();

    assert!(
        primary_count > 0,
        "expected table files in primary/cold tier after major compaction"
    );

    // Data should still be readable
    assert!(tree.get("key0000", lsm_tree::SeqNo::MAX)?.is_some());

    Ok(())
}

#[test]
fn recovery_discovers_tables_across_tiers() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Phase 1: write data and close
    {
        let config = three_tier_config(dir.path());
        let tree = config.open()?;

        tree.insert("a", "value_a", 0);
        tree.insert("b", "value_b", 1);
        tree.flush_active_memtable(0)?;
    }

    // Phase 2: reopen with the same config and verify data
    {
        let config = three_tier_config(dir.path());
        let tree = config.open()?;

        assert_eq!(
            tree.get("a", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
            Some(b"value_a".to_vec()),
        );
        assert_eq!(
            tree.get("b", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
            Some(b"value_b".to_vec()),
        );
    }

    Ok(())
}

#[test]
fn no_overhead_without_level_routes() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Config without level_routes — should work identically to before
    let config = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );
    assert!(config.level_routes.is_none());

    let tree = config.open()?;
    tree.insert("a", "value_a", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(
        tree.get("a", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
        Some(b"value_a".to_vec()),
    );

    Ok(())
}

#[test]
fn tables_folder_for_level_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let config = three_tier_config(dir.path());

    // L0 → hot tier
    let (folder, _) = config.tables_folder_for_level(0);
    assert_eq!(folder, dir.path().join("hot").join("tables"));

    // L1 → hot tier (0..2 includes 1)
    let (folder, _) = config.tables_folder_for_level(1);
    assert_eq!(folder, dir.path().join("hot").join("tables"));

    // L2 → warm tier
    let (folder, _) = config.tables_folder_for_level(2);
    assert_eq!(folder, dir.path().join("warm").join("tables"));

    // L4 → warm tier (2..5 includes 4)
    let (folder, _) = config.tables_folder_for_level(4);
    assert_eq!(folder, dir.path().join("warm").join("tables"));

    // L5 → primary (fallback, no route covers 5..7)
    let (folder, _) = config.tables_folder_for_level(5);
    assert_eq!(folder, dir.path().join("primary").join("tables"));

    // L6 → primary (fallback)
    let (folder, _) = config.tables_folder_for_level(6);
    assert_eq!(folder, dir.path().join("primary").join("tables"));
}

#[test]
fn all_tables_folders_deduplicates() {
    let dir = tempfile::tempdir().unwrap();
    let config = three_tier_config(dir.path());

    let folders = config.all_tables_folders();
    // primary + hot + warm = 3
    assert_eq!(folders.len(), 3);
}

/// Helper: config where L0–L1 is on hot, L2+ is on cold (primary).
/// This means Leveled compaction L1→L2 crosses a device boundary.
fn two_tier_config(base: &std::path::Path) -> Config {
    let hot = base.join("hot");

    Config::new(
        base.join("primary"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .level_routes(vec![LevelRoute {
        levels: 0..2,
        path: hot,
        fs: Arc::new(StdFs),
    }])
}

fn count_table_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name()
                        .to_str()
                        .map_or(false, |n| n.parse::<u64>().is_ok())
                })
                .count()
        })
        .unwrap_or(0)
}

// Cross-device compaction: L0 (hot) → L2+ (primary/cold) forces a rewrite
// instead of a trivial move, because the table must physically relocate.
#[test]
fn cross_device_compaction_rewrites_tables() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = two_tier_config(dir.path());
    let tree = config.open()?;

    // Flush enough data to L0 (hot tier)
    for i in 0u64..30 {
        tree.insert(format!("key{i:04}"), "x".repeat(200), i);
        if i % 5 == 4 {
            tree.flush_active_memtable(0)?;
        }
    }

    let hot_before = count_table_files(&dir.path().join("hot").join("tables"));
    assert!(
        hot_before > 0,
        "should have tables in hot tier before compaction"
    );

    // Major compact pushes everything to L6 (primary/cold)
    tree.major_compact(u64::MAX, u64::MAX)?;

    let primary_after = count_table_files(&dir.path().join("primary").join("tables"));
    assert!(
        primary_after > 0,
        "tables should be rewritten to cold tier after cross-device compaction"
    );

    // Verify data is still correct after cross-device compaction
    for i in 0u64..30 {
        let key = format!("key{i:04}");
        assert!(
            tree.get(&key, lsm_tree::SeqNo::MAX)?.is_some(),
            "key {key} should be readable after cross-device compaction"
        );
    }

    Ok(())
}

// Recovery with tables scattered across multiple tiers after compaction.
#[test]
fn recovery_after_cross_device_compaction() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    {
        let config = two_tier_config(dir.path());
        let tree = config.open()?;

        // Write some data and flush to L0 (hot)
        for i in 0u64..10 {
            tree.insert(format!("old{i:04}"), "cold_value", i);
        }
        tree.flush_active_memtable(0)?;

        // Compact to L6 (cold/primary)
        tree.major_compact(u64::MAX, u64::MAX)?;

        // Write more data and flush to L0 (hot) — these stay in hot
        for i in 0u64..5 {
            tree.insert(format!("new{i:04}"), "hot_value", 100 + i);
        }
        tree.flush_active_memtable(0)?;

        // Now we have tables in BOTH hot and primary tiers
        let hot = count_table_files(&dir.path().join("hot").join("tables"));
        let cold = count_table_files(&dir.path().join("primary").join("tables"));
        assert!(hot > 0, "should have tables in hot tier");
        assert!(cold > 0, "should have tables in cold tier");
    }

    // Reopen and verify ALL data from both tiers
    {
        let config = two_tier_config(dir.path());
        let tree = config.open()?;

        for i in 0u64..10 {
            let key = format!("old{i:04}");
            assert_eq!(
                tree.get(&key, lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
                Some(b"cold_value".to_vec()),
                "cold-tier key {key} not found after recovery"
            );
        }
        for i in 0u64..5 {
            let key = format!("new{i:04}");
            assert_eq!(
                tree.get(&key, lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
                Some(b"hot_value".to_vec()),
                "hot-tier key {key} not found after recovery"
            );
        }
    }

    Ok(())
}

// Empty routes vec normalizes to None.
#[test]
fn empty_routes_normalizes_to_none() {
    let config = Config::new(
        "/tmp/test",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(vec![]);

    assert!(config.level_routes.is_none());
}

// all_tables_folders deduplicates routes with the same path.
#[test]
fn all_tables_folders_dedup_same_path_routes() {
    let dir = tempfile::tempdir().unwrap();

    let config = Config::new(
        dir.path().join("db"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(vec![
        LevelRoute {
            levels: 0..2,
            path: dir.path().join("other"),
            fs: Arc::new(StdFs),
        },
        LevelRoute {
            levels: 2..5,
            path: dir.path().join("other"), // same path as above
            fs: Arc::new(StdFs),
        },
    ]);

    let folders = config.all_tables_folders();
    // primary + other = 2 (second "other" deduplicated by path)
    assert_eq!(folders.len(), 2);
}

// Same path with different Fs instances IS deduplicated (by path).
// Scanning the same directory twice would orphan-delete live SSTs.
#[test]
fn all_tables_folders_same_path_different_fs_deduped() {
    let dir = tempfile::tempdir().unwrap();

    let config = Config::new(
        dir.path().join("db"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(vec![
        LevelRoute {
            levels: 0..2,
            path: dir.path().join("shared"),
            fs: Arc::new(StdFs),
        },
        LevelRoute {
            levels: 2..5,
            path: dir.path().join("shared"), // same path, different Arc
            fs: Arc::new(StdFs),
        },
    ]);

    let folders = config.all_tables_folders();
    // primary + shared = 2 (duplicate path deduplicated to prevent double scan)
    assert_eq!(folders.len(), 2);
}

// Same-device Move stays as Move (no unnecessary rewrite).
#[test]
fn same_device_move_not_converted() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // All levels on same device — Move should stay Move
    let config = Config::new(
        dir.path().join("db"),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .data_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .index_block_compression_policy(CompressionPolicy::all(lsm_tree::CompressionType::None))
    .level_routes(vec![LevelRoute {
        levels: 0..7, // all levels on same path
        path: dir.path().join("all"),
        fs: Arc::new(StdFs),
    }]);

    let tree = config.open()?;

    for i in 0u64..10 {
        tree.insert(format!("k{i:04}"), "v", i);
        if i % 2 == 1 {
            tree.flush_active_memtable(0)?;
        }
    }

    // Compact — should work without issues (moves stay moves)
    tree.compact(Arc::new(lsm_tree::compaction::Leveled::default()), u64::MAX)?;

    // All tables should be in the single configured path
    let all_tables = count_table_files(&dir.path().join("all").join("tables"));
    assert!(all_tables > 0);

    // Data readable
    assert!(tree.get("k0000", lsm_tree::SeqNo::MAX)?.is_some());

    Ok(())
}

// Leveled compaction with cross-device routing: tables at L1 (hot) must be
// rewritten to L2 (cold) rather than trivially moved. Leveled naturally picks
// Move when a single table has no overlap at the next level, so we set up
// exactly that scenario across the hot→cold boundary.
//
// The key insight: with enough flush+compact cycles, Leveled will produce a
// single-table L1 with no L2 overlap → triggers Choice::Move → our guard
// detects cross-folder and converts to Merge.
#[test]
fn leveled_compaction_across_device_boundary() -> lsm_tree::Result<()> {
    use lsm_tree::compaction::Leveled;

    let dir = tempfile::tempdir()?;
    let config = two_tier_config(dir.path());
    let tree = config.open()?;

    // Multiple rounds of flush+compact to push data through L0→L1→L2.
    // Each round adds non-overlapping keys to maximize chance of trivial moves.
    for round in 0u64..5 {
        for i in 0u64..10 {
            let key = format!("r{round:02}k{i:04}");
            tree.insert(key, "x".repeat(200), round * 100 + i);
        }
        tree.flush_active_memtable(0)?;

        // Compact multiple times per round to cascade L0→L1→L2
        for _ in 0..10 {
            tree.compact(Arc::new(Leveled::default()), u64::MAX)?;
        }
    }

    // Data must be fully readable after cross-device compactions
    for round in 0u64..5 {
        for i in 0u64..10 {
            let key = format!("r{round:02}k{i:04}");
            assert!(
                tree.get(&key, lsm_tree::SeqNo::MAX)?.is_some(),
                "key {key} missing after leveled cross-device compaction"
            );
        }
    }

    // Cold tier should have tables (data pushed past L1)
    let cold = count_table_files(&dir.path().join("primary").join("tables"));
    assert!(
        cold > 0,
        "expected tables in cold tier after repeated leveled compaction"
    );

    Ok(())
}

// Orphaned table files in a routed tier directory are cleaned up via the Fs
// trait during recovery (not std::fs).
#[test]
fn recovery_cleans_orphaned_tables_in_routed_tier() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Phase 1: create a tree and flush data
    {
        let config = two_tier_config(dir.path());
        let tree = config.open()?;
        tree.insert("x", "val", 0);
        tree.flush_active_memtable(0)?;
    }

    // Plant an orphan file in the hot tier tables directory
    let hot_tables = dir.path().join("hot").join("tables");
    let orphan_path = hot_tables.join("999999");
    // Write a minimal file (non-empty so it's a valid "table file" by name)
    std::fs::write(&orphan_path, b"orphan")?;
    assert!(orphan_path.exists());

    // Phase 2: reopen — recovery should delete the orphan
    {
        let config = two_tier_config(dir.path());
        let _tree = config.open()?;
    }

    assert!(
        !orphan_path.exists(),
        "orphaned table file should be cleaned up during recovery"
    );

    Ok(())
}

// create_new creates tables/ directories for all level routes.
#[test]
fn create_new_creates_all_tier_directories() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = three_tier_config(dir.path());
    let _tree = config.open()?;

    assert!(dir.path().join("hot").join("tables").exists());
    assert!(dir.path().join("warm").join("tables").exists());
    assert!(dir.path().join("primary").join("tables").exists());

    Ok(())
}

// Recovery skips unexpected subdirectories in tables/ instead of panicking.
#[test]
fn recovery_skips_unexpected_directory_in_tables_folder() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Phase 1: create tree and flush
    {
        let config = two_tier_config(dir.path());
        let tree = config.open()?;
        tree.insert("k", "v", 0);
        tree.flush_active_memtable(0)?;
    }

    // Plant a subdirectory in the hot tables folder
    let hot_tables = dir.path().join("hot").join("tables");
    std::fs::create_dir_all(hot_tables.join("unexpected_subdir"))?;

    // Phase 2: reopen should succeed (skip the dir, not panic)
    {
        let config = two_tier_config(dir.path());
        let tree = config.open()?;
        assert_eq!(
            tree.get("k", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
            Some(b"v".to_vec()),
        );
    }

    Ok(())
}

// Debug formatting works for LevelRoute.
#[test]
fn level_route_debug_format() {
    let route = LevelRoute {
        levels: 0..3,
        path: "/mnt/nvme/db".into(),
        fs: Arc::new(StdFs),
    };
    let dbg = format!("{route:?}");
    assert!(dbg.contains("LevelRoute"));
    assert!(dbg.contains("0..3"));
    assert!(dbg.contains("/mnt/nvme/db"));
}

// Reopen tree on fresh routed path that doesn't exist yet (recovery creates it).
#[test]
fn recovery_creates_missing_routed_tables_dir() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;

    // Phase 1: create with primary only (no routes)
    {
        let config = Config::new(
            dir.path().join("primary"),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        );
        let tree = config.open()?;
        tree.insert("a", "val", 0);
        tree.flush_active_memtable(0)?;
    }

    // Phase 2: reopen with routes — routed dirs don't exist yet, recovery creates them
    {
        let config = Config::new(
            dir.path().join("primary"),
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .level_routes(vec![LevelRoute {
            levels: 0..3,
            path: dir.path().join("new_hot"),
            fs: Arc::new(StdFs),
        }]);
        let tree = config.open()?;
        // Data from primary should still be found
        assert_eq!(
            tree.get("a", lsm_tree::SeqNo::MAX)?.map(|v| v.to_vec()),
            Some(b"val".to_vec()),
        );
        // New routed dir should have been created
        assert!(dir.path().join("new_hot").join("tables").exists());
    }

    Ok(())
}

#[test]
#[should_panic(expected = "empty or inverted level route range")]
fn empty_range_panics() {
    let _config = Config::new(
        "/tmp/test",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(vec![LevelRoute {
        levels: 3..3, // empty
        path: "/a".into(),
        fs: Arc::new(StdFs),
    }]);
}

#[test]
#[should_panic(expected = "overlapping level routes")]
fn overlapping_routes_panic() {
    let _config = Config::new(
        "/tmp/test",
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .level_routes(vec![
        LevelRoute {
            levels: 0..3,
            path: "/a".into(),
            fs: Arc::new(StdFs),
        },
        LevelRoute {
            levels: 2..5, // overlaps with 0..3
            path: "/b".into(),
            fs: Arc::new(StdFs),
        },
    ]);
}
