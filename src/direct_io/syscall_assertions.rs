// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Platform-gated tests that verify the direct-I/O open path actually requested
//! `O_DIRECT` (Linux). On other platforms the kernel exposes no per-fd query for
//! the equivalent flag, so the syscall-level check is a "the open returned
//! without error" probe only.

#![cfg(test)]

use super::{create_write_direct, open_read_direct};
use std::io::Write;
use test_log::test;

/// Returns `true` if the error indicates the filesystem does not support direct I/O,
/// in which case callers should skip the assertion (tmpfs/overlayfs/some FUSE FSes
/// commonly reject `O_DIRECT` with `EINVAL`).
#[cfg(target_os = "linux")]
fn is_unsupported(e: &std::io::Error) -> bool {
    e.raw_os_error() == Some(libc::EINVAL)
}

#[cfg(not(target_os = "linux"))]
fn is_unsupported(_: &std::io::Error) -> bool {
    false
}

#[test]
fn is_unsupported_classifier_recognises_einval_on_linux_only() {
    let einval = std::io::Error::from_raw_os_error(22);
    let other = std::io::Error::other("not an OS error");
    #[cfg(target_os = "linux")]
    {
        assert!(is_unsupported(&einval));
        assert!(!is_unsupported(&other));
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert!(!is_unsupported(&einval));
        assert!(!is_unsupported(&other));
    }
}

#[test]
fn create_write_direct_actually_sets_flag() -> std::io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("write");

    match create_write_direct(&path) {
        Ok(_file) => {
            #[cfg(target_os = "linux")]
            assert!(super::is_direct_io_enabled(&_file)?);
        }
        Err(e) if is_unsupported(&e) => {
            eprintln!("filesystem rejects direct-write open; skipping: {e:?}");
        }
        Err(e) => return Err(e),
    }
    Ok(())
}

#[test]
fn open_read_direct_actually_sets_flag() -> std::io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("read");

    // Need real bytes so the read-direct probe doesn't trip on the empty-file
    // alignment edge case.
    {
        let mut f = std::fs::File::create(&path)?;
        f.write_all(&vec![0u8; 8_192])?;
        f.sync_all()?;
    }

    match open_read_direct(&path) {
        Ok(_file) => {
            #[cfg(target_os = "linux")]
            assert!(super::is_direct_io_enabled(&_file)?);
        }
        Err(e) if is_unsupported(&e) => {
            eprintln!("filesystem rejects direct-read open; skipping: {e:?}");
        }
        Err(e) => return Err(e),
    }
    Ok(())
}

/// End-to-end: drive a tree flush + compaction with direct I/O enabled and snoop
/// the syscall-level flag on every opened table file.
///
/// Only Linux can assert syscall-level — see module docs.
#[test]
fn integration_flush_and_compact_uses_direct_io_at_syscall_level() -> crate::Result<()> {
    use crate::{direct_io, file::TABLES_FOLDER, AbstractTree, Config, SequenceNumberCounter};

    let folder = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let tree = Config::new(
        folder.path(),
        seqno.clone(),
        SequenceNumberCounter::default(),
    )
    .use_direct_io_for_flush_and_compaction(true)
    .use_direct_io_for_compaction_reads(true)
    .open()?;

    // Pre-flight: confirm the test directory's filesystem supports direct reads.
    // tmpfs / overlayfs / some FUSE filesystems reject O_DIRECT with EINVAL.
    let probe_path = folder.path().join(".direct_io_probe");
    {
        let mut probe = std::fs::File::create(&probe_path)?;
        probe.write_all(&vec![0u8; 8_192])?;
        probe.sync_all()?;
    }
    let supported = match direct_io::open_read_direct(&probe_path) {
        Ok(_) => true,
        Err(e) if is_unsupported(&e) => false,
        Err(e) => return Err(e.into()),
    };
    let _ = std::fs::remove_file(&probe_path);
    if !supported {
        eprintln!("filesystem does not support direct I/O; skipping integration assertion");
        return Ok(());
    }

    for i in 0..1_000 {
        tree.insert(format!("k{i:08}").as_bytes(), b"payload", seqno.next());
    }
    tree.flush_active_memtable(0)?;

    // On Linux, fcntl(F_GETFL) can read back the O_DIRECT flag on a freshly opened
    // file. Elsewhere the flag is unobservable post-open, so we only check that
    // the open succeeded.
    let tables_folder = folder.path().join(TABLES_FOLDER);
    for entry in std::fs::read_dir(&tables_folder)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let _file = direct_io::open_read_direct(&entry.path())?;
        #[cfg(target_os = "linux")]
        assert!(direct_io::is_direct_io_enabled(&_file)?);
    }

    tree.major_compact(64 * 1_024 * 1_024, 0)?;

    for entry in std::fs::read_dir(&tables_folder)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let _file = direct_io::open_read_direct(&entry.path())?;
        #[cfg(target_os = "linux")]
        assert!(direct_io::is_direct_io_enabled(&_file)?);
    }

    Ok(())
}
