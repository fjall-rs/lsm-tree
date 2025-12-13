use lsm_tree::{
    get_tmp_folder, AbstractTree, CancellationToken, Config, KvSeparationOptions,
    SequenceNumberCounter, VerificationOptions, VerificationProgress,
};
use std::{
    io::{Seek, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use test_log::test;
#[test]
fn verify_checksums_no_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    let result = tree.verify_checksums()?;

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 1);
    assert_eq!(result.blob_files_verified, 0);
    assert_eq!(result.corrupted_count(), 0);
    assert!(result.bytes_verified > 0);
    assert!(!result.was_cancelled);

    Ok(())
}

#[test]
fn verify_checksums_detect_table_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    // Corrupt the table file
    let version = tree.current_version();
    let table = version.iter_tables().next().unwrap();

    {
        let mut f = std::fs::OpenOptions::new().write(true).open(&*table.path)?;

        f.seek(std::io::SeekFrom::Start(100))?;
        f.write_all(b"CORRUPTED!")?;
        f.sync_all()?;
    }

    let result = tree.verify_checksums()?;

    assert!(!result.is_ok());
    assert!(!result.no_corruption());
    assert_eq!(result.tables_verified, 1);
    assert_eq!(result.corrupted_tables.len(), 1);
    assert_eq!(result.corrupted_count(), 1);

    let (table_id, corrupted) = &result.corrupted_tables[0];
    assert_eq!(*table_id, table.id());
    assert_ne!(corrupted.expected, corrupted.actual);
    assert!(corrupted.file_size > 0);

    Ok(())
}

#[test]
fn verify_checksums_blob_tree_no_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    let result = tree.verify_checksums()?;

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 1);
    assert!(result.blob_files_verified >= 1);
    assert_eq!(result.corrupted_count(), 0);

    Ok(())
}

#[test]
fn verify_checksums_detect_blob_file_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    // Corrupt a blob file
    let version = tree.current_version();
    let blob_file = version.blob_files.iter().next().unwrap();

    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(blob_file.path())?;

        f.seek(std::io::SeekFrom::Start(100))?;
        f.write_all(b"CORRUPTED!")?;
        f.sync_all()?;
    }

    let result = tree.verify_checksums()?;

    assert!(!result.is_ok());
    assert!(result.blob_files_verified >= 1);
    assert_eq!(result.corrupted_blob_files.len(), 1);

    let (blob_id, corrupted) = &result.corrupted_blob_files[0];
    assert_eq!(*blob_id, blob_file.id());
    assert_ne!(corrupted.expected, corrupted.actual);

    Ok(())
}

#[test]
fn verify_checksums_multiple_tables() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create multiple tables
    for batch in 0..3 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let result = tree.verify_checksums()?;

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 3);
    assert_eq!(result.corrupted_count(), 0);

    Ok(())
}

#[test]
fn verify_checksums_empty_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    let result = tree.verify_checksums()?;

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 0);
    assert_eq!(result.blob_files_verified, 0);
    assert_eq!(result.corrupted_count(), 0);

    Ok(())
}

#[test]
fn verify_checksums_with_options_parallel() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create multiple tables for parallel verification
    for batch in 0..5 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let options = VerificationOptions::new().parallelism(4);

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 5);
    assert!(result.throughput_bytes_per_sec() > 0.0);

    Ok(())
}

#[test]
fn verify_checksums_with_progress_callback() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create multiple tables
    for batch in 0..3 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let progress_count = Arc::new(AtomicUsize::new(0));
    let progress_count_clone = Arc::clone(&progress_count);

    let options = VerificationOptions::new().parallelism(1); // Single thread for predictable ordering

    let result = tree.verify_checksums_with_options(
        &options,
        None,
        Some(move |progress: VerificationProgress| {
            progress_count_clone.fetch_add(1, Ordering::SeqCst);
            assert!(progress.files_verified <= progress.files_total);
            assert!(progress.bytes_verified <= progress.bytes_total);
            assert!(progress.bytes_per_second >= 0.0);
        }),
    );

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 3);

    // Progress callback should have been called at least once per file
    let count = progress_count.load(Ordering::SeqCst);
    assert!(
        count >= 3,
        "Expected at least 3 progress callbacks, got {count}"
    );

    Ok(())
}

#[test]
fn verify_checksums_with_cancellation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create some tables
    for batch in 0..3 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let cancel_token = CancellationToken::new();

    // Cancel immediately
    cancel_token.cancel();

    let options = VerificationOptions::new();
    let result = tree.verify_checksums_with_options(&options, Some(&cancel_token), None::<fn(_)>);

    assert!(result.was_cancelled);

    Ok(())
}

#[test]
fn verify_checksums_with_delayed_cancellation() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create many tables
    for batch in 0..10 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let cancel_token = CancellationToken::new();
    let cancel_clone = cancel_token.clone();

    // Cancel after a short delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        cancel_clone.cancel();
    });

    let options = VerificationOptions::new().parallelism(1);
    let result = tree.verify_checksums_with_options(&options, Some(&cancel_token), None::<fn(_)>);

    // May or may not be cancelled depending on timing
    // But should not panic and should return a valid result
    assert!(result.files_verified() <= 10);

    Ok(())
}

#[test]
fn verify_checksums_with_rate_limit() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create a table
    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    // Rate limit to a very low value
    let options = VerificationOptions::new()
        .rate_limit(1024 * 1024) // 1 MB/s
        .parallelism(1);

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 1);

    Ok(())
}

#[test]
fn verify_checksums_tables_only() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    let options = VerificationOptions::new()
        .verify_tables(true)
        .verify_blob_files(false);

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 1);
    assert_eq!(result.blob_files_verified, 0);

    Ok(())
}

#[test]
fn verify_checksums_blob_files_only() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    let options = VerificationOptions::new()
        .verify_tables(false)
        .verify_blob_files(true);

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 0);
    assert!(result.blob_files_verified >= 1);

    Ok(())
}

#[test]
fn verify_checksums_stop_on_first_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create multiple tables
    for batch in 0..5 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    // Corrupt all table files
    let version = tree.current_version();
    for table in version.iter_tables() {
        let mut f = std::fs::OpenOptions::new().write(true).open(&*table.path)?;

        f.seek(std::io::SeekFrom::Start(100))?;
        f.write_all(b"CORRUPTED!")?;
        f.sync_all()?;
    }

    let options = VerificationOptions::new()
        .stop_on_first_corruption(true)
        .parallelism(1); // Single thread for predictable behavior

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(!result.is_ok());
    // With stop_on_first_corruption, we should stop after first corruption
    assert_eq!(result.corrupted_count(), 1);

    Ok(())
}

#[test]
fn verify_checksums_custom_buffer_size() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        let value = nanoid::nanoid!();
        tree.insert(key, value.as_bytes(), 0);
    }
    tree.flush_active_memtable(0)?;

    // Use small buffer size
    let options = VerificationOptions::new().buffer_size(4096);

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 1);

    // Use large buffer size
    let options = VerificationOptions::new().buffer_size(4 * 1024 * 1024);

    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert_eq!(result.tables_verified, 1);

    Ok(())
}

#[test]
fn verify_checksums_throughput_reporting() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create multiple tables
    for batch in 0..3 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let options = VerificationOptions::new();
    let result = tree.verify_checksums_with_options(&options, None, None::<fn(_)>);

    assert!(result.is_ok());
    assert!(result.bytes_verified > 0);
    assert!(result.duration.as_nanos() > 0);
    assert!(result.throughput_bytes_per_sec() > 0.0);

    Ok(())
}

#[test]
fn verify_checksums_files_verified_count() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for batch in 0..5 {
        for key in ('a'..='z').map(|c| format!("{batch}_{c}")) {
            let value = nanoid::nanoid!();
            tree.insert(key, value.as_bytes(), 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let result = tree.verify_checksums()?;

    assert!(result.is_ok());
    assert_eq!(result.files_verified(), 5);
    assert_eq!(result.tables_verified, 5);
    assert_eq!(result.blob_files_verified, 0);

    Ok(())
}
