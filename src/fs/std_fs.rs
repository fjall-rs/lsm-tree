// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

/// Default [`Fs`] implementation backed by [`std::fs`].
///
/// This is a zero-sized type — when used as a monomorphized generic
/// parameter it adds no runtime overhead.
#[derive(Clone, Copy, Debug, Default)]
pub struct StdFs;

// ---------------------------------------------------------------------------
// FsFile for std::fs::File
// ---------------------------------------------------------------------------
// Self:: calls delegate to File's inherent methods (clippy::use_self preference).

impl FsFile for File {
    fn sync_all(&self) -> io::Result<()> {
        Self::sync_all(self)
    }

    fn sync_data(&self) -> io::Result<()> {
        Self::sync_data(self)
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        let m = Self::metadata(self)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        Self::set_len(self, size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        // Fill-or-EOF loop: retry on short reads and EINTR so that callers
        // see either a full buffer or a short read that signals EOF.
        let mut filled = 0usize;

        while filled < buf.len() {
            // SAFETY: loop guard `filled < buf.len()` ensures this is in-bounds.
            #[expect(clippy::expect_used, reason = "filled < buf.len() by loop guard")]
            let remaining = buf.get_mut(filled..).expect("filled < buf.len()");
            let off = offset.saturating_add(filled as u64);

            let n = {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    match FileExt::read_at(self, remaining, off) {
                        Ok(n) => n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }

                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt;
                    match FileExt::seek_read(self, remaining, off) {
                        Ok(n) => n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }

                #[cfg(not(any(unix, windows)))]
                {
                    let _ = (remaining, off);
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "read_at is not supported on this platform",
                    ));
                }
            };

            if n == 0 {
                break; // EOF
            }
            filled += n;
        }

        Ok(filled)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        sys::lock_exclusive(self)
    }
}

// ---------------------------------------------------------------------------
// Fs for StdFs
// ---------------------------------------------------------------------------

impl Fs for StdFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let file = OpenOptions::new()
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .create_new(opts.create_new)
            .truncate(opts.truncate)
            .append(opts.append)
            .open(path)?;
        Ok(Box::new(file))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        // Fail-fast on bad entries is intentional: non-UTF-8 filenames in an
        // lsm-tree data directory indicate filesystem corruption (see FsDirEntry docs).
        std::fs::read_dir(path)?
            .map(|res| {
                let entry = res?;
                let file_type = entry.file_type()?;
                let file_name_os = entry.file_name();
                let file_name = file_name_os.into_string().map_err(|os| {
                    #[expect(
                        clippy::unnecessary_debug_formatting,
                        reason = "OsString has no Display impl — Debug is required"
                    )]
                    let msg = format!("non-UTF-8 filename in directory {}: {os:?}", path.display());
                    io::Error::new(io::ErrorKind::InvalidData, msg)
                })?;
                Ok(FsDirEntry {
                    path: entry.path(),
                    file_name,
                    is_dir: file_type.is_dir(),
                })
            })
            .collect()
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        let m = std::fs::metadata(path)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        #[cfg(not(target_os = "windows"))]
        {
            let dir = File::open(path)?;
            if !dir.metadata()?.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sync_directory: path is not a directory",
                ));
            }
            dir.sync_all()
        }

        // Windows cannot fsync directories — no-op, same as crate::file::fsync_directory.
        #[cfg(target_os = "windows")]
        {
            let _ = path;
            Ok(())
        }
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }
}

// ---------------------------------------------------------------------------
// Platform-specific file locking
// ---------------------------------------------------------------------------

#[cfg(unix)]
mod sys {
    use std::ffi::c_int;
    use std::fs::File;
    use std::io;
    use std::os::unix::io::AsRawFd;

    // Declare flock directly to avoid requiring libc as a direct dependency.
    const LOCK_EX: c_int = 2;

    // SAFETY: declaration matches the POSIX `flock` ABI on Unix targets.
    unsafe extern "C" {
        fn flock(fd: c_int, operation: c_int) -> c_int;
    }

    pub(super) fn lock_exclusive(file: &File) -> io::Result<()> {
        let fd = file.as_raw_fd();

        loop {
            // SAFETY: fd is a valid file descriptor owned by `file`.
            #[expect(unsafe_code, reason = "flock FFI call with valid fd")]
            let ret = unsafe { flock(fd, LOCK_EX) };

            if ret == 0 {
                return Ok(());
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
    }
}

#[cfg(windows)]
mod sys {
    use std::fs::File;
    use std::io;
    use std::os::windows::io::AsRawHandle;

    pub(super) fn lock_exclusive(file: &File) -> io::Result<()> {
        use std::ptr;

        // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
        const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;

        // SAFETY: declaration matches the Windows `LockFileEx` ABI and `Overlapped` layout.
        #[expect(non_snake_case, reason = "FFI name matches Windows API")]
        unsafe extern "system" {
            fn LockFileEx(
                h_file: *mut std::ffi::c_void,
                dw_flags: u32,
                dw_reserved: u32,
                n_number_of_bytes_to_lock_low: u32,
                n_number_of_bytes_to_lock_high: u32,
                lp_overlapped: *mut Overlapped,
            ) -> i32;
        }

        #[repr(C)]
        struct Overlapped {
            internal: usize,
            internal_high: usize,
            offset: u32,
            offset_high: u32,
            h_event: *mut std::ffi::c_void,
        }

        let handle = file.as_raw_handle();
        let mut overlapped = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            h_event: ptr::null_mut(),
        };

        // SAFETY: handle is a valid file handle owned by `file`.
        #[expect(unsafe_code, reason = "LockFileEx FFI call with valid handle")]
        let ret = unsafe {
            LockFileEx(
                handle as *mut std::ffi::c_void,
                LOCKFILE_EXCLUSIVE_LOCK,
                0,
                u32::MAX,
                u32::MAX,
                &mut overlapped,
            )
        };

        if ret == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
mod sys {
    use std::fs::File;
    use std::io;

    pub(super) fn lock_exclusive(_file: &File) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file locking is not supported on this platform",
        ))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn std_fs_create_read_write() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        // Create and write
        let path = dir.path().join("test.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        file.sync_all()?;
        drop(file);

        // Read back
        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(&path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello world");

        Ok(())
    }

    #[test]
    fn std_fs_directory_operations() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let nested = dir.path().join("a").join("b").join("c");
        fs.create_dir_all(&nested)?;
        assert!(fs.exists(&nested)?);

        // Create a file inside
        let file_path = nested.join("data.bin");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&file_path, &opts)?;
        file.write_all(b"data")?;
        drop(file);

        // read_dir
        let entries = fs.read_dir(&nested)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "data.bin");
        assert!(!entries[0].is_dir);

        // metadata
        let meta = fs.metadata(&file_path)?;
        assert!(meta.is_file);
        assert!(!meta.is_dir);
        assert_eq!(meta.len, 4);

        // remove_file
        fs.remove_file(&file_path)?;
        assert!(!fs.exists(&file_path)?);

        // remove_dir_all
        let top = dir.path().join("a");
        fs.remove_dir_all(&top)?;
        assert!(!fs.exists(&top)?);

        Ok(())
    }

    #[test]
    fn std_fs_rename() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&src, &opts)?;
        file.write_all(b"content")?;
        drop(file);

        fs.rename(&src, &dst)?;
        assert!(!fs.exists(&src)?);
        assert!(fs.exists(&dst)?);

        Ok(())
    }

    #[test]
    fn std_fs_sync_directory() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        // Should not error on valid directories
        fs.sync_directory(dir.path())?;
        Ok(())
    }

    #[test]
    fn fs_file_metadata() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("meta.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"12345")?;

        let meta = file.metadata()?;
        assert!(meta.is_file);
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    fn fs_file_set_len() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("truncate.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        file.set_len(5)?;

        let meta = file.metadata()?;
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn fs_file_lock_exclusive() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("lockfile");
        let opts = FsOpenOptions::new().write(true).create(true);
        let file = fs.open(&path, &opts)?;
        file.lock_exclusive()?;

        // Verifies flock() syscall succeeds without error. Testing actual
        // lock contention (try_lock from second thread) is out of scope for
        // the Fs trait definition — belongs in integration tests.
        Ok(())
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn fs_file_read_at() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("pread.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;

        // read_at at offset 6 should return "world"
        let mut buf = [0u8; 5];
        let n = file.read_at(&mut buf, 6)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        // read_at at offset 0 should return "hello"
        let n = file.read_at(&mut buf, 0)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        Ok(())
    }

    #[test]
    fn fs_open_options_default() {
        let opts = FsOpenOptions::default();
        assert!(!opts.read);
        assert!(!opts.write);
        assert!(!opts.create);
        assert!(!opts.create_new);
        assert!(!opts.truncate);
        assert!(!opts.append);
    }

    #[test]
    fn fs_open_options_builders() {
        let opts = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .create_new(false)
            .truncate(true)
            .append(false);
        assert!(opts.read);
        assert!(opts.write);
        assert!(opts.create);
        assert!(!opts.create_new);
        assert!(opts.truncate);
        assert!(!opts.append);
    }

    #[test]
    fn fs_file_sync_data() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        let path = dir.path().join("sync_data.bin");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"data")?;
        file.sync_data()?;

        Ok(())
    }

    #[test]
    fn fs_open_truncate_and_append() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        let path = dir.path().join("trunc.txt");

        // Create with content
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        drop(file);

        // Truncate on reopen
        let opts = FsOpenOptions::new().write(true).truncate(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hi")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 2);

        // Append mode
        let opts = FsOpenOptions::new().write(true).append(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"!")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 3);

        Ok(())
    }

    #[test]
    fn fs_dir_entry_fields() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;

        // Create a subdirectory and a file
        let sub = dir.path().join("subdir");
        fs.create_dir_all(&sub)?;
        let file_path = dir.path().join("file.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(&file_path, &opts)?;

        let mut entries = fs.read_dir(dir.path())?;
        entries.sort_by(|a, b| a.file_name.cmp(&b.file_name));

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].file_name, "file.txt");
        assert!(!entries[0].is_dir);
        assert_eq!(entries[1].file_name, "subdir");
        assert!(entries[1].is_dir);

        Ok(())
    }

    #[test]
    fn fs_metadata_directory() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let fs = StdFs;
        let meta = fs.metadata(dir.path())?;
        assert!(meta.is_dir);
        assert!(!meta.is_file);

        Ok(())
    }

    // Linux only: macOS (HFS+/APFS) rejects non-UTF-8 filenames at the FS layer.
    #[test]
    #[cfg(target_os = "linux")]
    fn read_dir_rejects_non_utf8_filename() -> io::Result<()> {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::tempdir()?;
        // Create a file with invalid UTF-8 bytes in its name.
        let bad_name = OsStr::from_bytes(&[0xff, 0xfe]);
        let bad_path = dir.path().join(bad_name);
        if std::fs::write(&bad_path, b"data").is_err() {
            // Filesystem rejected the non-UTF-8 filename (e.g. overlay,
            // container mounts, restrictive mount options) — test
            // precondition cannot be met, skip gracefully.
            return Ok(());
        }

        let fs = StdFs;
        match fs.read_dir(dir.path()) {
            Err(err) => {
                assert_eq!(err.kind(), io::ErrorKind::InvalidData);
                let msg = err.to_string();
                assert!(
                    msg.contains("non-UTF-8 filename"),
                    "unexpected error: {msg}"
                );
                assert!(
                    msg.contains(&dir.path().display().to_string()),
                    "error should include directory path: {msg}",
                );
            }
            Ok(_) => panic!("read_dir should fail on non-UTF-8 filename"),
        }
        Ok(())
    }

    /// Compile-time assertion: `Fs` is object-safe without specifying
    /// associated types — enables simple `Arc<dyn Fs>` for per-level routing.
    #[test]
    fn object_safety() -> io::Result<()> {
        let fs: Arc<dyn Fs> = Arc::new(StdFs);
        let dir = tempfile::tempdir()?;
        let bogus = dir.path().join("nonexistent");
        assert!(!fs.exists(&bogus)?);
        Ok(())
    }
}
