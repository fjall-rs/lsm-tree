use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use test_log::test;

#[test]
fn multi_get_all_existing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for i in 0..100u64 {
        tree.insert(format!("key_{i:04}"), format!("value_{i}"), i);
    }

    tree.flush_active_memtable(0)?;

    let keys: Vec<String> = (0..100u64).map(|i| format!("key_{i:04}")).collect();
    let results = tree.multi_get(&keys, SeqNo::MAX)?;

    assert_eq!(results.len(), 100);
    for (i, result) in results.iter().enumerate() {
        let expected = format!("value_{i}");
        assert_eq!(
            result.as_deref(),
            Some(expected.as_bytes()),
            "mismatch at index {i}",
        );
    }

    Ok(())
}

#[test]
fn multi_get_mixed_existing_and_missing() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("c", "val_c", 1);
    tree.insert("e", "val_e", 2);

    let results = tree.multi_get(["a", "b", "c", "d", "e"], 3)?;

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[1], None);
    assert_eq!(results[2].as_deref(), Some(b"val_c".as_slice()));
    assert_eq!(results[3], None);
    assert_eq!(results[4].as_deref(), Some(b"val_e".as_slice()));

    Ok(())
}

#[test]
fn multi_get_empty_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);

    let results = tree.multi_get(Vec::<&str>::new(), 1)?;
    assert!(results.is_empty());

    Ok(())
}

#[test]
fn multi_get_snapshot_isolation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "v1", 0);
    tree.insert("b", "v1", 1);

    // Update values at higher seqno
    tree.insert("a", "v2", 2);
    tree.insert("b", "v2", 3);

    // Read at snapshot seqno=2: should see a=v1, b=v1
    let results = tree.multi_get(["a", "b"], 2)?;
    assert_eq!(results[0].as_deref(), Some(b"v1".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"v1".as_slice()));

    // Read at snapshot seqno=4: should see a=v2, b=v2
    let results = tree.multi_get(["a", "b"], 4)?;
    assert_eq!(results[0].as_deref(), Some(b"v2".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"v2".as_slice()));

    Ok(())
}

#[test]
fn multi_get_with_tombstones() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.remove("a", 2);

    let results = tree.multi_get(["a", "b"], 3)?;
    assert_eq!(results[0], None);
    assert_eq!(results[1].as_deref(), Some(b"val_b".as_slice()));

    Ok(())
}

#[test]
fn multi_get_from_disk() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);
    tree.flush_active_memtable(0)?;

    // Insert more to memtable
    tree.insert("d", "val_d", 3);

    // Multi-get spanning both disk and memtable
    let results = tree.multi_get(["a", "b", "c", "d", "e"], SeqNo::MAX)?;
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"val_b".as_slice()));
    assert_eq!(results[2].as_deref(), Some(b"val_c".as_slice()));
    assert_eq!(results[3].as_deref(), Some(b"val_d".as_slice()));
    assert_eq!(results[4], None);

    Ok(())
}

#[test]
fn multi_get_blob_tree_with_kv_separation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions {
        separation_threshold: 1, // separate all values
        ..Default::default()
    }))
    .open()?;

    let big_val_a = b"aaa".repeat(1000);
    let big_val_b = b"bbb".repeat(1000);

    tree.insert("a", big_val_a.as_slice(), 0);
    tree.insert("b", big_val_b.as_slice(), 1);
    tree.insert("c", b"ccc".repeat(1000).as_slice(), 2);
    tree.remove("c", 3);

    tree.flush_active_memtable(0)?;

    // Verify blob indirections were created
    assert!(tree.blob_file_count() > 0);

    let results = tree.multi_get(["a", "b", "c", "missing"], SeqNo::MAX)?;

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_deref(), Some(big_val_a.as_slice()));
    assert_eq!(results[1].as_deref(), Some(big_val_b.as_slice()));
    assert_eq!(results[2], None); // tombstoned
    assert_eq!(results[3], None); // never existed

    Ok(())
}

#[test]
fn multi_get_unsorted_and_duplicate_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    tree.insert("a", "val_a", 0);
    tree.insert("b", "val_b", 1);
    tree.insert("c", "val_c", 2);

    // Unsorted keys with a duplicate — results must match input order 1:1
    let results = tree.multi_get(["c", "a", "b", "a", "missing"], 3)?;

    assert_eq!(results.len(), 5);
    assert_eq!(results[0].as_deref(), Some(b"val_c".as_slice()));
    assert_eq!(results[1].as_deref(), Some(b"val_a".as_slice()));
    assert_eq!(results[2].as_deref(), Some(b"val_b".as_slice()));
    assert_eq!(results[3].as_deref(), Some(b"val_a".as_slice())); // duplicate
    assert_eq!(results[4], None);

    Ok(())
}
