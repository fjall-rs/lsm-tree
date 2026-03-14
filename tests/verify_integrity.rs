use lsm_tree::{
    // AbstractTree must be in scope for enum_dispatch method resolution on AnyTree
    get_tmp_folder,
    verify,
    AbstractTree,
    Config,
    KvSeparationOptions,
    SequenceNumberCounter,
};
use test_log::test;

#[test]
fn verify_integrity_clean_tree() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        tree.insert(key, b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    let report = verify::verify_integrity(&tree);

    assert!(report.is_ok(), "clean tree should have no errors");
    assert_eq!(1, report.sst_files_checked);
    assert_eq!(0, report.blob_files_checked);

    Ok(())
}

#[test]
fn verify_integrity_detect_sst_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        tree.insert(key, b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    // Corrupt a byte in the SST file
    let version = tree.current_version();
    let table = version.iter_tables().next().unwrap();
    {
        use std::io::{Seek, Write};
        let mut f = std::fs::OpenOptions::new().write(true).open(&*table.path)?;
        f.seek(std::io::SeekFrom::Start(100))?;
        f.write_all(b"CORRUPT")?;
        f.sync_all()?;
    }

    let report = verify::verify_integrity(&tree);

    assert!(!report.is_ok(), "corrupted tree should have errors");
    assert_eq!(1, report.sst_files_checked);
    assert_eq!(1, report.errors.len());

    // Verify error type
    match &report.errors[0] {
        verify::IntegrityError::SstFileCorrupted { table_id, .. } => {
            assert_eq!(*table_id, table.id());
        }
        other => panic!("expected SstFileCorrupted, got: {other}"),
    }

    Ok(())
}

#[test]
fn verify_integrity_blob_tree_clean() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        tree.insert(key, b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    let report = verify::verify_integrity(&tree);

    assert!(report.is_ok(), "clean blob tree should have no errors");
    assert!(report.sst_files_checked > 0);
    assert!(report.blob_files_checked > 0);

    Ok(())
}

#[test]
fn verify_integrity_detect_blob_corruption() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .with_kv_separation(Some(KvSeparationOptions::default().separation_threshold(1)))
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        tree.insert(key, b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    // Corrupt a byte in the blob file
    let version = tree.current_version();
    let blob_file = version.blob_files.iter().next().unwrap();
    {
        use std::io::{Seek, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(blob_file.path())?;
        f.seek(std::io::SeekFrom::Start(100))?;
        f.write_all(b"CORRUPT")?;
        f.sync_all()?;
    }

    let report = verify::verify_integrity(&tree);

    assert!(!report.is_ok(), "corrupted blob tree should have errors");
    assert_eq!(1, report.errors.len());

    match &report.errors[0] {
        verify::IntegrityError::BlobFileCorrupted { blob_file_id, .. } => {
            assert_eq!(*blob_file_id, blob_file.id());
        }
        other => panic!("expected BlobFileCorrupted, got: {other}"),
    }

    Ok(())
}

#[test]
fn verify_integrity_multiple_tables() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    // Create multiple SST files
    for batch in 0..3 {
        for i in 0..10 {
            let key = format!("batch{batch}_key{i:04}");
            tree.insert(key, b"value", 0);
        }
        tree.flush_active_memtable(0)?;
    }

    let report = verify::verify_integrity(&tree);

    assert!(report.is_ok());
    assert_eq!(3, report.sst_files_checked);
    assert_eq!(3, report.files_checked());

    Ok(())
}

#[test]
fn verify_integrity_missing_sst_file() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ('a'..='z').map(|c| c.to_string()) {
        tree.insert(key, b"value", 0);
    }
    tree.flush_active_memtable(0)?;

    // Delete the SST file to trigger an IoError
    let version = tree.current_version();
    let table = version.iter_tables().next().unwrap();
    std::fs::remove_file(&*table.path)?;

    let report = verify::verify_integrity(&tree);

    assert!(!report.is_ok(), "missing file should produce an error");
    assert_eq!(1, report.errors.len());

    match &report.errors[0] {
        verify::IntegrityError::IoError { path, .. } => {
            assert_eq!(*path, *table.path);
        }
        other => panic!("expected IoError, got: {other}"),
    }

    Ok(())
}
