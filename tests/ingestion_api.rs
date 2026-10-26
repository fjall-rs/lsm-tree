use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, KvSeparationOptions, MemoryVersionSetProvider, SeqNo,
    SequenceNumberCounter,
};
use std::sync::Arc;

fn finish_ingestion(
    ingestion: lsm_tree::AnyIngestion<'_>,
    tree: &lsm_tree::AnyTree,
    version_set_provider: &MemoryVersionSetProvider,
    seqno: &SequenceNumberCounter,
) -> lsm_tree::Result<()> {
    if let Some((ingestion_result, ingestion_seqno)) = ingestion.finish(
        || {
            version_set_provider
                .flush_active_memtable(tree, seqno.next(), 0)
                .map(|_| ())
        },
        || seqno.next(),
    )? {
        version_set_provider.register_flush(ingestion_result, ingestion_seqno);
    }

    Ok(())
}

#[test]
fn tree_ingestion_tombstones_delete_existing_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    for i in 0..10u32 {
        let key = format!("k{:03}", i);
        tree.insert(key.as_bytes(), b"v", seqno.next());
    }

    let mut ingest = tree.ingestion()?;
    for i in 0..10u32 {
        let key = format!("k{:03}", i);
        ingest.write_tombstone(key)?;
    }
    finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;

    for i in 0..10u32 {
        let key = format!("k{:03}", i);
        assert!(tree.get(key.as_bytes(), SeqNo::MAX)?.is_none());
    }
    assert_eq!(tree.tombstone_count(), 10);

    Ok(())
}

#[test]
fn sealed_memtable_value_overrides_table_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    // Older table value via ingestion (seqno 1)
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"old")?;
        finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;
    }

    // Newer value in memtable, then seal it
    tree.insert(b"k", b"new", seqno.next());
    let _ = version_set_provider.rotate_active_memtable(&tree); // move active -> sealed

    // Read should return the sealed memtable value
    assert_eq!(
        tree.get(b"k", lsm_tree::SeqNo::MAX)?,
        Some(b"new".as_slice().into())
    );

    Ok(())
}

#[test]
fn sealed_memtable_tombstone_overrides_table_value() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    // Older table value via ingestion (seqno 1)
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"old")?;
        finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;
    }

    // Newer tombstone in memtable, then seal it
    tree.remove(b"k", seqno.next());
    let _ = version_set_provider.rotate_active_memtable(&tree);

    // Read should see the delete from sealed memtable
    assert!(tree.get(b"k", lsm_tree::SeqNo::MAX)?.is_none());

    Ok(())
}

#[test]
fn tables_newest_first_returns_highest_seqno() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    // Two separate ingestions create two tables containing the same key at different seqnos
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"v1")?;
        finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;
    }
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"v2")?;
        finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;
    }

    // With memtables empty, read should return the value from the newest table run
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
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());
    let tree = lsm_tree::Config::new(&folder, version_set_provider)
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
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(&folder, version_set_provider)
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(8)))
        .open()?;

    let before = tree.blob_file_count();

    // Use a small value for the first write to avoid blob I/O
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut ingest = tree.ingestion().unwrap();
        ingest.write(b"k2", b"x").unwrap();

        // Second write would require blob I/O, but ordering check should fire before any blob write
        let _ = ingest.write(b"k1", [1u8; 16]);
    }));
    assert!(result.is_err());

    let after = tree.blob_file_count();
    assert_eq!(before, after);

    Ok(())
}

#[test]
fn memtable_put_overrides_table_tombstone() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = lsm_tree::Config::new(&folder, version_set_provider.clone()).open()?;

    // Older put written via ingestion to tables
    {
        let mut ingest = tree.ingestion()?;
        ingest.write(b"k", b"v1")?;
        finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;
    }

    // Newer tombstone written via ingestion to tables
    {
        let mut ingest = tree.ingestion()?;
        ingest.write_tombstone(b"k")?;
        finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;
    }

    // Newest put in active memtable should override table tombstone
    tree.insert(b"k", b"v3", seqno.next());
    assert_eq!(
        tree.get(b"k", lsm_tree::SeqNo::MAX)?,
        Some(b"v3".as_slice().into())
    );
    Ok(())
}

#[test]
fn blob_tree_ingestion_tombstones_delete_existing_keys() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    for i in 0..8u32 {
        let key = format!("b{:03}", i);
        tree.insert(key.as_bytes(), b"x", seqno.next());
    }

    let mut ingest = tree.ingestion()?;
    for i in 0..8u32 {
        let key = format!("b{:03}", i);
        ingest.write_tombstone(key)?;
    }
    finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;

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
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone()).open()?;

    let before_tables = tree.table_count();
    finish_ingestion(tree.ingestion()?, &tree, &version_set_provider, &seqno)?;
    let after_tables = tree.table_count();

    assert_eq!(before_tables, after_tables);
    assert!(tree.is_empty(SeqNo::MAX, None)?);

    Ok(())
}

#[test]
fn blob_ingestion_only_tombstones_does_not_create_blob_files() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    for i in 0..5u32 {
        let key = format!("d{:03}", i);
        tree.insert(key.as_bytes(), b"value", seqno.next());
    }
    version_set_provider.flush_active_memtable(&tree, seqno.next(), 0)?;

    let before_blobs = tree.blob_file_count();

    let mut ingest = tree.ingestion()?;
    for i in 0..5u32 {
        let key = format!("d{:03}", i);
        ingest.write_tombstone(key)?;
    }
    finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;

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
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
        .open()?;

    let before_tables = tree.table_count();
    let before_blobs = tree.blob_file_count();

    finish_ingestion(tree.ingestion()?, &tree, &version_set_provider, &seqno)?;

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
    let seqno = SequenceNumberCounter::default();
    let version_set_provider = Arc::new(MemoryVersionSetProvider::default());

    let tree = Config::new(&folder, version_set_provider.clone())
        .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(8)))
        .open()?;

    let mut ingest = tree.ingestion()?;
    ingest.write("k_big1", [1u8; 16])?;
    ingest.write("k_big2", [2u8; 32])?;
    ingest.write("k_small", "abc")?;
    finish_ingestion(ingest, &tree, &version_set_provider, &seqno)?;

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
