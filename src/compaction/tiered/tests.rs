use super::*;
use crate::{
    compaction::CompactionStrategy, AbstractTree, Config, SequenceNumberCounter, MAX_SEQNO,
};
use std::sync::Arc;
use test_log::test;

/// Helper: flush N overlapping memtables so each becomes a separate run in L0.
/// Each flush inserts shared boundary keys "a" and "z" plus a unique key to
/// ensure overlapping key ranges (preventing `optimize_runs` from merging
/// disjoint runs into one).
fn flush_overlapping(
    tree: &impl crate::AbstractTree,
    count: u8,
    seqno_base: u64,
) -> crate::Result<()> {
    for i in 0..count {
        let seqno = seqno_base + u64::from(i);
        tree.insert("a", "v", seqno);
        tree.insert([b'k', i].as_slice(), "v", seqno);
        tree.insert("z", "v", seqno);
        tree.flush_active_memtable(seqno)?;
    }
    Ok(())
}

#[test]
fn stcs_empty_levels() -> crate::Result<()> {
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
fn stcs_below_min_merge_width() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 2 overlapping tables — below default min_merge_width=4
    flush_overlapping(&tree, 2, 0)?;
    assert_eq!(2, tree.table_count());
    assert!(tree.l0_run_count() > 1, "runs should be separate");

    let strategy = Arc::new(Strategy::default());
    tree.compact(strategy, 2)?;

    // No merge should occur — still 2 tables
    assert_eq!(2, tree.table_count());
    Ok(())
}

#[test]
fn stcs_triggers_merge() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 4 similarly-sized overlapping tables
    flush_overlapping(&tree, 4, 0)?;
    assert_eq!(4, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(4));
    tree.compact(strategy, 4)?;

    // All 4 should merge into 1
    assert_eq!(1, tree.table_count());

    // All data should be readable
    for i in 0..4u8 {
        assert!(
            tree.get([b'k', i].as_slice(), MAX_SEQNO)?.is_some(),
            "key k{i} should exist after compaction",
        );
    }

    Ok(())
}

#[test]
fn stcs_min_merge_width_2() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 2 overlapping tables
    flush_overlapping(&tree, 2, 0)?;
    assert_eq!(2, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));
    tree.compact(strategy, 2)?;

    // With min_merge_width=2, 2 similar runs should merge
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn stcs_space_amp_full_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create 5 overlapping flushes.
    // With 5 similarly-sized runs: space_amp ~ (5S/S - 1)*100 = 400% > 200%
    flush_overlapping(&tree, 5, 0)?;
    assert_eq!(5, tree.table_count());

    // Use high min_merge_width so the size-ratio path wouldn't trigger,
    // but the space amp check still fires.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(100)
            .with_max_space_amplification_percent(200),
    );
    tree.compact(strategy, 5)?;

    // Space amp triggered full compaction
    assert_eq!(1, tree.table_count());

    Ok(())
}

#[test]
fn stcs_max_merge_width_cap() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 8 overlapping tables
    flush_overlapping(&tree, 8, 0)?;
    assert_eq!(8, tree.table_count());

    // max_merge_width=3 should only merge 3 out of 8.
    // Disable space amp check with very high threshold.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(2)
            .with_max_merge_width(3)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 8)?;

    // 8 runs -> merge 3 smallest into 1 -> 6 runs total
    // (8 - 3 + 1 = 6)
    assert_eq!(6, tree.table_count());

    Ok(())
}

#[test]
fn stcs_data_integrity_multi_compact() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Insert keys across 5 overlapping flushes with updates to test MVCC
    for batch in 0..5u64 {
        tree.insert("a", format!("v{batch}").as_bytes(), batch);
        for k in 0..4u8 {
            let key = [b'k', k];
            let val = format!("v{batch}");
            tree.insert(key.as_slice(), val.as_bytes(), batch);
        }
        tree.insert("z", format!("v{batch}").as_bytes(), batch);
        tree.flush_active_memtable(batch)?;
    }

    assert_eq!(5, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));

    // Run compaction multiple times to progressively merge
    for seqno in 5..8u64 {
        tree.compact(strategy.clone(), seqno)?;
    }

    // All keys should be readable with latest values
    for k in 0..4u8 {
        let val = tree.get([b'k', k].as_slice(), MAX_SEQNO)?;
        assert!(val.is_some(), "key k{k} should exist");
        assert_eq!(val.as_deref(), Some(b"v4".as_slice()));
    }

    Ok(())
}

#[test]
fn stcs_no_space_amp_trigger_below_threshold() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // 2 overlapping runs: space_amp = (2S/S - 1) * 100 = 100%. Below 200% threshold.
    flush_overlapping(&tree, 2, 0)?;

    // min_merge_width=100 so size-ratio path won't fire.
    // space_amp (100%) < threshold (200%) so nothing happens.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(100)
            .with_max_space_amplification_percent(200),
    );
    tree.compact(strategy, 2)?;

    assert_eq!(2, tree.table_count());
    Ok(())
}

#[test]
fn stcs_multiple_compaction_cycles() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));
    let mut seqno = 0u64;

    // Flush and compact in cycles
    for _cycle in 0..3 {
        for _k in 0..3 {
            // Overlapping keys to keep separate runs
            tree.insert("a", "val", seqno);
            tree.insert(format!("key_{seqno}").as_bytes(), "val", seqno);
            tree.insert("z", "val", seqno);
            tree.flush_active_memtable(seqno)?;
            seqno += 1;
        }
        tree.compact(strategy.clone(), seqno)?;
    }

    // All keys should be readable
    for s in 0..seqno {
        assert!(
            tree.get(format!("key_{s}").as_bytes(), MAX_SEQNO)?
                .is_some(),
            "key_{s} should exist after multiple compaction cycles",
        );
    }

    Ok(())
}

#[test]
fn stcs_get_name() {
    let strategy = Strategy::default();
    assert_eq!(strategy.get_name(), "SizeTieredCompaction");
}

#[test]
fn stcs_get_config_serialization() {
    let strategy = Strategy::default()
        .with_size_ratio(0.5)
        .with_min_merge_width(8)
        .with_max_merge_width(16)
        .with_max_space_amplification_percent(300)
        .with_table_target_size(128 * 1024 * 1024);

    let config = strategy.get_config();
    assert_eq!(config.len(), 5, "should serialize all 5 parameters");

    // Verify keys exist
    let keys: Vec<_> = config.iter().map(|(k, _)| k.as_ref()).collect();
    assert!(keys.iter().any(|k| k == b"tiered_size_ratio"));
    assert!(keys.iter().any(|k| k == b"tiered_min_merge_width"));
    assert!(keys.iter().any(|k| k == b"tiered_max_merge_width"));
    assert!(keys.iter().any(|k| k == b"tiered_max_space_amp_pct"));
    assert!(keys.iter().any(|k| k == b"tiered_target_size"));
}

#[test]
fn stcs_builder_with_size_ratio() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 4 runs with overlapping keys
    flush_overlapping(&tree, 4, 0)?;

    // Very tight size_ratio=0.01 means only nearly-identical-sized runs merge
    // All our flushes are similar, so this should still trigger
    let strategy = Arc::new(
        Strategy::default()
            .with_size_ratio(0.01)
            .with_min_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 4)?;

    // Some merging should occur (runs are similarly sized)
    assert!(tree.table_count() < 4);

    Ok(())
}

#[test]
fn stcs_max_merge_width_less_than_min_no_merge() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Flush 4 overlapping runs
    flush_overlapping(&tree, 4, 0)?;
    assert_eq!(4, tree.table_count());

    // Configure max_merge_width=2 but min_merge_width=4.
    // prefix_len might be >= 4 (min), but merge_count = min(prefix, 2) = 2 < 4 (min).
    // Guard should prevent merge.
    let strategy = Arc::new(
        Strategy::default()
            .with_min_merge_width(4)
            .with_max_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 4)?;

    // No merge should occur — misconfigured max < min is guarded
    assert_eq!(4, tree.table_count());
    Ok(())
}

#[test]
fn stcs_single_run_no_compaction() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Single flush = single run in L0
    tree.insert("a", "v", 0);
    tree.flush_active_memtable(0)?;

    assert_eq!(1, tree.table_count());

    let strategy = Arc::new(Strategy::default().with_min_merge_width(2));
    tree.compact(strategy, 1)?;

    // Single run → DoNothing (runs.len() < 2)
    assert_eq!(1, tree.table_count());
    Ok(())
}

#[test]
fn stcs_dissimilar_sizes_break() -> crate::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(
        dir.path(),
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create runs of very different sizes so the size-ratio break fires.
    // First run: tiny (1 key)
    tree.insert("a", "v", 0);
    tree.insert("z", "v", 0);
    tree.flush_active_memtable(0)?;

    // Second run: much larger (many keys → bigger table)
    for k in 0..200u16 {
        tree.insert("a", "v", 1);
        tree.insert(k.to_be_bytes().as_slice(), "large_value_padding_xxxxx", 1);
        tree.insert("z", "v", 1);
    }
    tree.flush_active_memtable(1)?;

    // Very tight size_ratio=0.01, min_merge_width=2.
    // The two runs differ hugely in size, so ratio > 1.01 → break fires.
    let strategy = Arc::new(
        Strategy::default()
            .with_size_ratio(0.01)
            .with_min_merge_width(2)
            .with_max_space_amplification_percent(u64::MAX),
    );
    tree.compact(strategy, 2)?;

    // No merge — sizes too different
    assert_eq!(2, tree.table_count());
    Ok(())
}
