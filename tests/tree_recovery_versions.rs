use byteorder::{LittleEndian, ReadBytesExt};
use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
};
use test_log::test;

fn read_manifest_format_version(path: &Path) -> lsm_tree::Result<u8> {
    // read_u64 takes &mut self, but calling it on an owned File from `?` is
    // valid Rust — the compiler auto-borrows &mut on the owned temporary.
    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));
    let reader = sfa::Reader::new(&manifest_path)?;

    #[expect(
        clippy::expect_used,
        reason = "test fixture should contain format_version"
    )]
    let section = reader
        .toc()
        .section(b"format_version")
        .expect("format_version section should exist");

    Ok(section.buf_reader(&manifest_path)?.read_u8()?)
}

fn rewrite_manifest_format_version(path: &Path, version: u8) -> lsm_tree::Result<()> {
    let curr_version_id = File::open(path.join("current"))?.read_u64::<LittleEndian>()?;
    let manifest_path = path.join(format!("v{curr_version_id}"));
    let reader = sfa::Reader::new(&manifest_path)?;

    #[expect(
        clippy::expect_used,
        reason = "test fixture should contain format_version"
    )]
    let section = reader
        .toc()
        .section(b"format_version")
        .expect("format_version section should exist");

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&manifest_path)?;
    file.seek(SeekFrom::Start(section.pos()))?;
    file.write_all(&[version])?;
    file.flush()?;

    Ok(())
}

#[test]
fn tree_writes_v4_manifest_and_recovers_it() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(4, read_manifest_format_version(path)?);
    }

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        assert_eq!(Some("a".as_bytes().into()), tree.get("a", 1)?);
        assert_eq!(4, read_manifest_format_version(path)?);
    }

    Ok(())
}

#[test]
fn tree_recovers_safe_v3_manifest() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(4, read_manifest_format_version(path)?);
        rewrite_manifest_format_version(path, 3)?;
        assert_eq!(3, read_manifest_format_version(path)?);
    }

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        assert_eq!(Some("a".as_bytes().into()), tree.get("a", 1)?);
        assert_eq!(3, read_manifest_format_version(path)?);
    }

    Ok(())
}

#[test]
fn tree_rejects_unsupported_manifest_version() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;

        assert_eq!(4, read_manifest_format_version(path)?);
        rewrite_manifest_format_version(path, 99)?;
        assert_eq!(99, read_manifest_format_version(path)?);
    }

    let reopened = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open();

    match reopened {
        Err(lsm_tree::Error::InvalidVersion(v)) => {
            assert_eq!(v, 99, "rejected version should match the tampered value");
        }
        Err(other) => panic!("expected InvalidVersion(99), got: {other:?}"),
        Ok(_) => panic!("unsupported manifest version must be rejected"),
    }

    Ok(())
}

#[test]
#[ignore = "restore Version history maintenance"]
fn tree_recovery_version_free_list() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    let path = folder.path();

    {
        let tree = Config::new(
            path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert!(path.join("v0").try_exists()?);

        tree.insert("a", "a", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(1, tree.version_free_list_len());
        assert!(path.join("v1").try_exists()?);

        tree.insert("b", "b", 0);
        tree.flush_active_memtable(0)?;
        assert_eq!(2, tree.version_free_list_len());
        assert!(path.join("v2").try_exists()?);
    }

    {
        let tree = Config::new(
            &folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .open()?;
        assert_eq!(0, tree.version_free_list_len());
        assert!(!path.join("v0").try_exists()?);
        assert!(!path.join("v1").try_exists()?);
        assert!(path.join("v2").try_exists()?);

        assert!(tree.contains_key("a", 1)?);
        assert!(tree.contains_key("b", 1)?);
    }

    Ok(())
}
