use lsm_tree::{
    compaction::Fifo, AbstractTree, Config, KvSeparationOptions, SequenceNumberCounter,
};
use std::sync::Arc;

#[test]
fn fifo_ttl_no_drop_when_recent_or_disabled() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(dir.path(), SequenceNumberCounter::default()).open()?;

    // Two quick tables (both recent)
    tree.insert("a", "1", 0);
    tree.flush_active_memtable(0)?;
    tree.insert("b", "2", 1);
    tree.flush_active_memtable(1)?;

    assert_eq!(2, tree.table_count());

    // TTL enabled but not yet expired
    let fifo_recent = Arc::new(Fifo::new(u64::MAX, Some(1)));
    tree.compact(fifo_recent, 2)?;
    assert_eq!(2, tree.table_count());

    // TTL disabled explicitly
    let fifo_disabled = Arc::new(Fifo::new(u64::MAX, None));
    tree.compact(fifo_disabled, 2)?;
    assert_eq!(2, tree.table_count());

    Ok(())
}

#[test]
fn fifo_below_limit_no_drop_standard() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(dir.path(), SequenceNumberCounter::default()).open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "v", i as u64);
        tree.flush_active_memtable(i as u64)?;
    }

    let fifo = Arc::new(Fifo::new(u64::MAX, None));
    tree.compact(fifo, 3)?;

    assert_eq!(3, tree.table_count());

    Ok(())
}

#[test]
fn fifo_limit_considers_blob_bytes() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(dir.path(), SequenceNumberCounter::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    // Create multiple small tables referencing blob files
    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "$", i as u64); // value goes to blob
        tree.flush_active_memtable(i as u64)?;
    }

    let before = tree.table_count();
    assert_eq!(3, before);

    // Very small limit forces dropping based on (table + blob) size
    let fifo = Arc::new(Fifo::new(10, None));
    tree.compact(fifo, 3)?;

    // Should have dropped at least one table due to blob bytes pressure
    assert!(tree.table_count() < before);

    Ok(())
}

#[test]
fn fifo_compact_empty_tree_noop() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(dir.path(), SequenceNumberCounter::default()).open()?;

    let fifo = Arc::new(Fifo::new(1_000_000, Some(1)));
    tree.compact(fifo, 0)?;

    assert_eq!(0, tree.table_count());

    Ok(())
}

#[test]
fn fifo_idempotent_when_within_limits() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = Config::new(dir.path(), SequenceNumberCounter::default()).open()?;

    for i in 0..3u8 {
        tree.insert([b'k', i].as_slice(), "v", i as u64);
        tree.flush_active_memtable(i as u64)?;
    }

    let fifo = Arc::new(Fifo::new(u64::MAX, None));
    tree.compact(fifo.clone(), 3)?;
    let after_first = tree.table_count();

    tree.compact(fifo, 3)?;
    let after_second = tree.table_count();

    assert_eq!(after_first, after_second);

    Ok(())
}
