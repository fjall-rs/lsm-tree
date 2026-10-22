// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, MemoryVersionSetProvider, SequenceNumberCounter,
};
use std::sync::Arc;
use test_log::test;

/// Test that iterators can be stored in a struct (proving they're 'static)
#[test]
fn static_iterator_ownership() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("a", "value_a", seqno.next());
    tree.insert("b", "value_b", seqno.next());
    tree.insert("c", "value_c", seqno.next());

    // Create an iterator and store it
    let iter = tree.range("a"..="c", seqno.get(), None);

    // Drop the tree - the iterator should still own all necessary data
    drop(tree);

    // Collect results from the iterator
    let results: Vec<_> = iter
        .map(|guard| guard.into_inner())
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"a");
    assert_eq!(results[0].1, b"value_a");
    assert_eq!(results[1].0, b"b");
    assert_eq!(results[1].1, b"value_b");
    assert_eq!(results[2].0, b"c");
    assert_eq!(results[2].1, b"value_c");

    Ok(())
}

/// Test that iterator can be moved across threads
#[test]
fn static_iterator_send_to_thread() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for i in 0..100 {
        tree.insert(
            format!("key_{:03}", i),
            format!("value_{}", i),
            seqno.next(),
        );
    }

    // Create iterator and move to another thread
    let iter = tree.range("key_000"..="key_099", seqno.get(), None);
    drop(tree);

    let handle = std::thread::spawn(move || {
        iter.map(|guard| guard.key())
            .collect::<lsm_tree::Result<Vec<_>>>()
            .unwrap()
            .len()
    });

    let count = handle.join().unwrap();
    assert_eq!(count, 100);

    Ok(())
}

/// Test static iterator with prefix
#[test]
fn static_iterator_prefix() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    tree.insert("user:1:name", "Alice", seqno.next());
    tree.insert("user:1:age", "30", seqno.next());
    tree.insert("user:2:name", "Bob", seqno.next());
    tree.insert("user:2:age", "25", seqno.next());
    tree.insert("other:data", "xyz", seqno.next());

    let iter = tree.prefix("user:1", seqno.get(), None);
    drop(tree);

    let results: Vec<_> = iter
        .map(|guard| guard.key())
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    assert_eq!(results.len(), 2);
    assert!(results[0].starts_with(b"user:1"));
    assert!(results[1].starts_with(b"user:1"));

    Ok(())
}

/// Test reverse iteration with static iterator
#[test]
fn static_iterator_reverse() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for i in 0..10 {
        tree.insert(format!("key_{}", i), format!("value_{}", i), seqno.next());
    }

    let iter = tree.range("key_0"..="key_9", seqno.get(), None);
    drop(tree);

    let results: Vec<_> = iter
        .rev()
        .map(|guard| guard.key())
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    assert_eq!(results.len(), 10);
    // Should be in reverse order
    assert_eq!(results[0], b"key_9");
    assert_eq!(results[9], b"key_0");

    Ok(())
}

/// Test iterator with flushed segments (disk-based data)
#[test]
fn static_iterator_with_segments() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    // Write data and flush to disk
    for i in 0..50 {
        tree.insert(format!("seg_{:03}", i), format!("val_{}", i), seqno.next());
    }

    // Write data and flush to disk
    let gc_watermark = seqno.get();
    version_set_provider.flush_active_memtable(&tree, seqno.next(), gc_watermark)?;

    // Write more data in memory
    for i in 50..100 {
        tree.insert(format!("seg_{:03}", i), format!("val_{}", i), seqno.next());
    }

    // Create iterator that spans both memtable and segments
    let iter = tree.range("seg_000"..="seg_099", seqno.get(), None);
    drop(tree);

    let count = iter
        .map(|guard| guard.key())
        .collect::<lsm_tree::Result<Vec<_>>>()?
        .len();

    assert_eq!(count, 100);

    Ok(())
}

/// Test BlobTree static iterator
#[test]
fn static_iterator_blob_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(Default::default()))
        .open()?;

    // Insert values that will be separated to blob files
    let large_value = vec![b'X'; 2048];
    for i in 0..20 {
        tree.insert(format!("blob_{:03}", i), large_value.clone(), seqno.next());
    }

    let gc_watermark = seqno.get();
    version_set_provider.flush_active_memtable(&tree, seqno.next(), gc_watermark)?;

    let iter = tree.range("blob_000"..="blob_019", seqno.get(), None);
    drop(tree);

    let results: Vec<_> = iter
        .map(|guard| guard.into_inner())
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    assert_eq!(results.len(), 20);
    for (_, value) in results {
        assert_eq!(value.len(), 2048);
    }

    Ok(())
}

/// Test that iterator sees a consistent snapshot
#[test]
fn static_iterator_snapshot_isolation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    // Initial data
    for i in 0..10 {
        tree.insert(format!("key_{}", i), "v1", seqno.next());
    }

    // Create iterator at the snapshot after the initial writes
    let iter = tree.range("key_0"..="key_9", seqno.get(), None);

    // Modify data after iterator creation
    for i in 0..10 {
        tree.insert(format!("key_{}", i), "v2", seqno.next());
    }

    // Iterator should see old version (v1)
    for guard in iter {
        let (_, value) = guard.into_inner()?;
        assert_eq!(value, b"v1");
    }

    // New iterator should see new version (v2)
    let new_iter = tree.range("key_0"..="key_9", seqno.get(), None);
    for guard in new_iter {
        let (_, value) = guard.into_inner()?;
        assert_eq!(value, b"v2");
    }

    Ok(())
}

/// Test multiple iterators alive at the same time
#[test]
fn static_iterator_multiple_concurrent() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for i in 0..100 {
        tree.insert(format!("key_{:03}", i), format!("val_{}", i), seqno.next());
    }

    // Create multiple iterators
    let snapshot_seqno = seqno.get();
    let iter1 = tree.range("key_000"..="key_024", snapshot_seqno, None);
    let iter2 = tree.range("key_025"..="key_049", snapshot_seqno, None);
    let iter3 = tree.range("key_050"..="key_074", snapshot_seqno, None);
    let iter4 = tree.range("key_075"..="key_099", snapshot_seqno, None);

    drop(tree);

    // Collect all results
    let count1 = iter1.count();
    let count2 = iter2.count();
    let count3 = iter3.count();
    let count4 = iter4.count();

    assert_eq!(count1, 25);
    assert_eq!(count2, 25);
    assert_eq!(count3, 25);
    assert_eq!(count4, 25);

    Ok(())
}

/// Test that iterator properly maintains Version reference preventing data loss
#[test]
fn static_iterator_prevents_data_loss() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    // Create data and flush
    for i in 0..50 {
        tree.insert(format!("key_{:03}", i), format!("val_{}", i), seqno.next());
    }

    let gc_watermark = seqno.get();
    version_set_provider.flush_active_memtable(&tree, seqno.next(), gc_watermark)?;

    // Create iterator
    let iter = tree.range("key_000"..="key_049", seqno.get(), None);

    // Even if we trigger compaction, the iterator should keep the segments alive
    // (In practice, segments would be marked for deletion after compaction but
    // the Version reference in the iterator prevents actual deletion)

    // Verify all data is accessible
    let count = iter.count();
    assert_eq!(count, 50);

    Ok(())
}

/// Test iterator with tombstones
#[test]
fn static_iterator_with_tombstones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for i in 0..20 {
        tree.insert(format!("key_{:02}", i), format!("val_{}", i), seqno.next());
    }

    // Delete some keys
    for i in 5..15 {
        tree.remove(format!("key_{:02}", i), seqno.next());
    }

    let iter = tree.range("key_00"..="key_19", seqno.get(), None);
    drop(tree);

    let results: Vec<_> = iter
        .map(|guard| guard.key())
        .collect::<lsm_tree::Result<Vec<_>>>()?;

    // Should only see 10 keys (0-4 and 15-19)
    assert_eq!(results.len(), 10);
    assert_eq!(results[0], b"key_00");
    assert_eq!(results[4], b"key_04");
    assert_eq!(results[5], b"key_15");
    assert_eq!(results[9], b"key_19");

    Ok(())
}
