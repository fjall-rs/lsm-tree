use lsm_tree::{get_tmp_folder, AbstractTree, Config, KvSeparationOptions, SeqNo};

#[test]
fn tree_ingestion_tombstones_delete_existing_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, Default::default()).open()?;

    for i in 0..10u32 {
        let key = format!("k{:03}", i);
        tree.insert(key.as_bytes(), b"v", 0);
    }

    let mut ingest = tree.ingestion()?;
    for i in 0..10u32 {
        let key = format!("k{:03}", i);
        ingest.write_tombstone(key)?;
    }
    ingest.finish()?;

    for i in 0..10u32 {
        let key = format!("k{:03}", i);
        assert!(tree.get(key.as_bytes(), SeqNo::MAX)?.is_none());
    }
    assert_eq!(tree.tombstone_count(), 10);

    Ok(())
}

#[test]
fn sealed_memtable_value_overrides_table_value() -> lsm_tree::Result<()> {
    use lsm_tree::AbstractTree;
    let folder = get_tmp_folder();

    let tree = lsm_tree::Config::new(&folder, Default::default()).open()?;

    // Older table value via ingestion (seqno 1)
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"old")?;
        ingest.finish()?;
    }

    // Newer value in memtable (seqno 2), then seal it
    tree.insert(b"k", b"new", 2);
    let _ = tree.rotate_memtable(); // move active -> sealed

    // Read should return the sealed memtable value
    assert_eq!(
        tree.get(b"k", lsm_tree::SeqNo::MAX)?,
        Some(b"new".as_slice().into())
    );

    Ok(())
}

#[test]
fn sealed_memtable_tombstone_overrides_table_value() -> lsm_tree::Result<()> {
    use lsm_tree::AbstractTree;
    let folder = get_tmp_folder();

    let tree = lsm_tree::Config::new(&folder, Default::default()).open()?;

    // Older table value via ingestion (seqno 1)
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"old")?;
        ingest.finish()?;
    }

    // Newer tombstone in memtable (seqno 2), then seal it
    tree.remove(b"k", 2);
    let _ = tree.rotate_memtable();

    // Read should see the delete from sealed memtable
    assert!(tree.get(b"k", lsm_tree::SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
fn tables_newest_first_returns_highest_seqno() -> lsm_tree::Result<()> {
    use lsm_tree::AbstractTree;
    let folder = get_tmp_folder();

    let tree = lsm_tree::Config::new(&folder, Default::default()).open()?;

    // Two separate ingestions create two tables containing the same key at different seqnos
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"v1")?;
        ingest.finish()?;
    }
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"v2")?;
        ingest.finish()?;
    }

    // With memtables empty, read should return the value from the newest table run (seqno 2)
    assert_eq!(
        tree.get(b"k", lsm_tree::SeqNo::MAX)?,
        Some(b"v2".as_slice().into())
    );
    Ok(())
}

#[test]
#[should_panic(expected = "next key in ingestion must be greater than last key")]
fn ingestion_enforces_order_standard_panics() {
    let folder = tempfile::tempdir().unwrap();
    let tree = lsm_tree::Config::new(&folder, Default::default())
        .open()
        .unwrap();

    let mut ingest = tree.ingestion().unwrap();

    // First write higher key, then lower to trigger ordering assertion
    ingest.write(b"k2", b"v").unwrap();

    // Panics here
    let _ = ingest.write(b"k1", b"v");
}

#[test]
fn blob_ingestion_out_of_order_panics_without_blob_write() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = lsm_tree::Config::new(&folder, Default::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(8)))
        .open()?;

    let before = tree.blob_file_count();

    // Use a small value for the first write to avoid blob I/O
    let result = std::panic::catch_unwind(|| {
        let mut ingest = tree.ingestion().unwrap();
        ingest.write(b"k2", b"x").unwrap();

        // Second write would require blob I/O, but ordering check should fire before any blob write
        let _ = ingest.write(b"k1", [1u8; 16]);
    });
    assert!(result.is_err());

    let after = tree.blob_file_count();
    assert_eq!(before, after);

    Ok(())
}

#[test]
fn memtable_put_overrides_table_tombstone() -> lsm_tree::Result<()> {
    use lsm_tree::AbstractTree;
    let folder = get_tmp_folder();

    let tree = lsm_tree::Config::new(&folder, Default::default()).open()?;

    // Older put written via ingestion to tables (seqno 1)
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"v1")?;
        ingest.finish()?;
    }

    // Newer tombstone written via ingestion to tables (seqno 2)
    {
        let mut ingest = tree.ingestion()?;
        ingest.write_tombstone(b"k")?;
        ingest.finish()?;
    }

    // Newest put in active memtable (seqno 3) should override table tombstone
    tree.insert(b"k", b"v3", 3);
    assert_eq!(
        tree.get(b"k", lsm_tree::SeqNo::MAX)?,
        Some(b"v3".as_slice().into())
    );
    Ok(())
}

#[test]
fn blob_tree_ingestion_tombstones_delete_existing_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, Default::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    for i in 0..8u32 {
        let key = format!("b{:03}", i);
        tree.insert(key.as_bytes(), b"x", 0);
    }

    let mut ingest = tree.ingestion()?;
    for i in 0..8u32 {
        let key = format!("b{:03}", i);
        ingest.write_tombstone(key)?;
    }
    ingest.finish()?;

    for i in 0..8u32 {
        let key = format!("b{:03}", i);
        assert!(tree.get(key.as_bytes(), SeqNo::MAX)?.is_none());
    }
    assert_eq!(tree.tombstone_count(), 8);

    Ok(())
}

#[test]
fn tree_ingestion_finish_no_writes_noop() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, Default::default()).open()?;

    let before_tables = tree.table_count();
    tree.ingestion()?.finish()?;
    let after_tables = tree.table_count();

    assert_eq!(before_tables, after_tables);
    assert!(tree.is_empty(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn blob_ingestion_only_tombstones_does_not_create_blob_files() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, Default::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    for i in 0..5u32 {
        let key = format!("d{:03}", i);
        tree.insert(key.as_bytes(), b"value", 0);
    }

    let before_blobs = tree.blob_file_count();

    let mut ingest = tree.ingestion()?;
    for i in 0..5u32 {
        let key = format!("d{:03}", i);
        ingest.write_tombstone(key)?;
    }
    ingest.finish()?;

    let after_blobs = tree.blob_file_count();
    assert_eq!(before_blobs, after_blobs);

    for i in 0..5u32 {
        let key = format!("d{:03}", i);
        assert!(tree.get(key.as_bytes(), SeqNo::MAX)?.is_none());
    }

    Ok(())
}

#[test]
fn blob_ingestion_finish_no_writes_noop() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, Default::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let before_tables = tree.table_count();
    let before_blobs = tree.blob_file_count();

    tree.ingestion()?.finish()?;

    let after_tables = tree.table_count();
    let after_blobs = tree.blob_file_count();

    assert_eq!(before_tables, after_tables);
    assert_eq!(before_blobs, after_blobs);
    assert!(tree.is_empty(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn blob_ingestion_separates_large_values_and_reads_ok() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(&folder, Default::default())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(8)))
        .open()?;

    let mut ingest = tree.ingestion()?;
    ingest.write("k_big1", [1u8; 16])?;
    ingest.write("k_big2", [2u8; 32])?;
    ingest.write("k_small", "abc")?;
    ingest.finish()?;

    assert!(tree.blob_file_count() >= 1);

    assert_eq!(
        tree.get("k_small", SeqNo::MAX)?,
        Some(b"abc".as_slice().into())
    );
    assert_eq!(
        tree.get("k_big1", SeqNo::MAX)?.as_deref().map(|s| s.len()),
        Some(16)
    );
    assert_eq!(
        tree.get("k_big2", SeqNo::MAX)?.as_deref().map(|s| s.len()),
        Some(32)
    );

    Ok(())
}
