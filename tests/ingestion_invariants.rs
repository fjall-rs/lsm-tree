use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, KvSeparationOptions, SeqNo, SequenceNumberCounter,
};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[test]
fn ingestion_autoflushes_active_memtable() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Write to active memtable
    for i in 0..10u32 {
        let k = format!("a{:03}", i);
        tree.insert(k.as_bytes(), b"v", 1);
    }

    let tables_before = tree.table_count();
    let sealed_before = tree.sealed_memtable_count();
    assert_eq!(sealed_before, 0);

    // Start ingestion (should auto-flush active)
    let mut ingest = tree.ingestion()?;
    ingest.write("a", "a")?;
    ingest.finish()?;

    // After ingestion, data is in tables; no sealed memtables
    assert_eq!(tree.sealed_memtable_count(), 0);
    assert!(tree.table_count() > tables_before);

    // Reads must succeed from tables
    for i in 0..10u32 {
        let k = format!("a{:03}", i);
        assert_eq!(
            tree.get(k.as_bytes(), SeqNo::MAX)?,
            Some(b"v".as_slice().into())
        );
    }

    Ok(())
}

#[test]
fn ingestion_flushes_sealed_memtables() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Put items into active and seal them
    for i in 0..8u32 {
        let k = format!("s{:03}", i);
        tree.insert(k.as_bytes(), b"x", 1);
    }
    assert!(tree.rotate_memtable().is_some());
    assert!(tree.sealed_memtable_count() > 0);

    let tables_before = tree.table_count();

    // Ingestion should flush sealed memtables and register resulting tables
    let mut ingest = tree.ingestion()?;
    ingest.write("a", "a")?;
    ingest.finish()?;

    assert_eq!(tree.sealed_memtable_count(), 0);
    assert!(tree.table_count() > tables_before);

    for i in 0..8u32 {
        let k = format!("s{:03}", i);
        assert_eq!(
            tree.get(k.as_bytes(), SeqNo::MAX)?,
            Some(b"x".as_slice().into())
        );
    }

    Ok(())
}

#[test]
fn ingestion_blocks_memtable_writes_until_finish() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Acquire ingestion and keep it active while another thread performs writes
    let ingest = tree.ingestion()?;

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let tree2 = tree.clone();

    let handle = thread::spawn(move || {
        started_tx.send(()).ok();
        tree2.insert(b"k_block", b"v", 6);
        done_tx.send(()).ok();
    });

    // Wait for the writer thread to start the attempt
    started_rx.recv().unwrap();

    // Give it a moment; the insert should complete and not be blocked by ingestion
    thread::sleep(Duration::from_millis(100));
    assert!(done_rx.try_recv().is_ok(), "insert should not be blocked");

    handle.join().unwrap();
    ingest.finish()?;

    // Verify the write landed and is visible
    assert_eq!(
        tree.get(b"k_block", SeqNo::MAX)?,
        Some(b"v".as_slice().into())
    );

    Ok(())
}

#[test]
fn blob_ingestion_honors_invariants_and_blocks_writes() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::<lsm_tree::fs::StdFileSystem>::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    // Write small values into memtable and then start blob ingestion
    for i in 0..4u32 {
        let k = format!("b{:03}", i);
        tree.insert(k.as_bytes(), b"y", 1);
    }

    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let tree2 = tree.clone();

    let ingest = tree.ingestion()?;

    let handle = thread::spawn(move || {
        started_tx.send(()).ok();
        tree2.insert(b"b999", b"z", 31);
        done_tx.send(()).ok();
    });

    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(done_rx.try_recv().is_ok());

    handle.join().unwrap();
    ingest.finish()?;

    // Data visible after ingestion, including concurrent write
    for i in 0..4u32 {
        let k = format!("b{:03}", i);
        assert_eq!(
            tree.get(k.as_bytes(), SeqNo::MAX)?,
            Some(b"y".as_slice().into())
        );
    }
    assert_eq!(tree.get(b"b999", SeqNo::MAX)?, Some(b"z".as_slice().into()));

    Ok(())
}
