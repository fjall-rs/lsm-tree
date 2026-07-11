// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Slice;
use std::{fs::File, io::Write, path::Path};

pub const MAGIC_BYTES: [u8; 4] = [b'L', b'S', b'M', 3];

pub const TABLES_FOLDER: &str = "tables";
pub const BLOBS_FOLDER: &str = "blobs";
pub const CURRENT_VERSION_FILE: &str = "current";

/// Reads bytes from a file using `pread`.
pub fn read_exact(file: &File, offset: u64, size: usize) -> std::io::Result<Slice> {
    // SAFETY: This slice builder starts uninitialized, but we know its length
    //
    // We use read_at/seek_read which give us the number of bytes read
    // If that number does not match the slice length, the function errors,
    // so the (partially) uninitialized buffer is discarded
    //
    // Additionally, generally, block loads furthermore do a checksum check which
    // would likely catch the buffer being wrong somehow
    #[expect(unsafe_code, reason = "see safety")]
    let mut builder = unsafe { Slice::builder_unzeroed(size) };

    {
        let bytes_read: usize;

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;

            bytes_read = file.read_at(&mut builder, offset)?;
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;

            bytes_read = file.seek_read(&mut builder, offset)?;
        }

        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("unsupported platform");
            unimplemented!();
        }

        if bytes_read != size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("read_exact({bytes_read}) at {offset} did not read enough bytes {size}; file has length {}", file.metadata()?.len()),
            ));
        }
    }

    Ok(builder.freeze().into())
}

/// Runs an I/O operation, retrying transient Windows errors with backoff.
///
/// External programs (antivirus, indexers, backup agents) may briefly hold files open
/// without sharing flags on Windows, failing renames and deletes that would succeed a
/// moment later; POSIX has no such failure mode, so elsewhere the operation runs once.
pub fn retry_transient_io<T>(mut op: impl FnMut() -> std::io::Result<T>) -> std::io::Result<T> {
    #[cfg(windows)]
    {
        const MAX_ATTEMPTS: u32 = 10;

        let mut delay = std::time::Duration::from_millis(1);

        for _ in 1..MAX_ATTEMPTS {
            match op() {
                Err(e) if is_transient_windows_error(&e) => {
                    std::thread::sleep(delay);
                    delay *= 2;
                }
                result => return result,
            }
        }
    }

    op()
}

#[cfg(windows)]
fn is_transient_windows_error(e: &std::io::Error) -> bool {
    // ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION, ERROR_LOCK_VIOLATION, ERROR_USER_MAPPED_FILE
    matches!(e.raw_os_error(), Some(5 | 32 | 33 | 1224))
}

/// Persists a named temporary file to its final path, replacing any existing file.
pub fn persist_temp_file(temp_file: tempfile::NamedTempFile, path: &Path) -> std::io::Result<()> {
    let mut temp_file = Some(temp_file);

    retry_transient_io(|| {
        #[expect(clippy::expect_used, reason = "the temp file is put back on failure")]
        temp_file
            .take()
            .expect("temp file should be present")
            .persist(path)
            .map(|_| ())
            .map_err(|e| {
                temp_file = Some(e.file);
                e.error
            })
    })
}

/// Atomically rewrites a file.
pub fn rewrite_atomic(path: &Path, content: &[u8]) -> std::io::Result<()> {
    #[expect(
        clippy::expect_used,
        reason = "every file should have a parent directory"
    )]
    let folder = path.parent().expect("should have a parent");

    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.flush()?;
    temp_file.as_file_mut().sync_all()?;
    persist_temp_file(temp_file, path)?;

    // TODO: not sure why it fails on Windows...
    #[cfg(not(target_os = "windows"))]
    {
        let file = std::fs::File::open(path)?;
        file.sync_all()?;

        #[expect(
            clippy::expect_used,
            reason = "files should always have a parent directory"
        )]
        let folder = path.parent().expect("should have parent folder");
        fsync_directory(folder)?;
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory(path: &Path) -> std::io::Result<()> {
    let file = std::fs::File::open(path)?;
    debug_assert!(file.metadata()?.is_dir());
    file.sync_all()
}

#[cfg(target_os = "windows")]
pub fn fsync_directory(path: &Path) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }

    #[test]
    fn persist_temp_file_replaces_existing() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        std::fs::write(&path, b"old")?;

        let mut temp_file = tempfile::NamedTempFile::new_in(dir.path())?;
        write!(temp_file, "new")?;
        persist_temp_file(temp_file, &path)?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("new", content);

        Ok(())
    }

    #[test]
    #[cfg(windows)]
    fn retry_transient_io_retries_sharing_violation() {
        let mut attempts = 0;

        let result = retry_transient_io(|| {
            attempts += 1;

            if attempts < 3 {
                // ERROR_SHARING_VIOLATION
                Err(std::io::Error::from_raw_os_error(32))
            } else {
                Ok(42)
            }
        });

        assert!(matches!(result, Ok(42)));
        assert_eq!(3, attempts);
    }

    #[test]
    #[cfg(windows)]
    fn retry_transient_io_fails_fast_on_other_errors() {
        let mut attempts = 0;

        let result: std::io::Result<()> = retry_transient_io(|| {
            attempts += 1;

            // ERROR_FILE_NOT_FOUND
            Err(std::io::Error::from_raw_os_error(2))
        });

        assert!(result.is_err());
        assert_eq!(1, attempts);
    }
}
