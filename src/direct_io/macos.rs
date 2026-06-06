// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! macOS-specific direct I/O: `F_NOCACHE`.
//!
//! macOS does not have `O_DIRECT`. Instead, `F_NOCACHE` is applied to an open file
//! descriptor via `fcntl`, instructing the kernel to bypass the unified buffer cache
//! for subsequent reads/writes on that descriptor. Unlike `O_DIRECT`, `F_NOCACHE`
//! imposes no alignment requirement on user buffers, file offsets, or I/O lengths.

use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::io::AsRawFd,
    path::Path,
    sync::OnceLock,
};

/// Opens an existing file for reading and enables `F_NOCACHE` on the descriptor.
pub fn open_read_direct(path: &Path) -> io::Result<File> {
    let file = File::open(path)?;
    apply_no_cache(&file);
    Ok(file)
}

/// Creates a new file for writing and enables `F_NOCACHE` on the descriptor.
/// Fails if the file already exists, matching `File::create_new`.
pub fn create_write_direct(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    apply_no_cache(&file);
    Ok(file)
}

/// Creates (or truncates) a file for writing and enables `F_NOCACHE` on the descriptor.
pub fn create_or_truncate_write_direct(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    apply_no_cache(&file);
    Ok(file)
}

/// Best-effort `F_NOCACHE` on the descriptor.
///
/// `F_NOCACHE` is an advisory cache hint: a descriptor without it is still fully
/// correct, just cached (i.e. exactly the buffered fallback the module documents
/// for filesystems that reject direct I/O). `fcntl(F_NOCACHE)` essentially never
/// fails on a regular open fd, but if it does we keep the open file rather than
/// hard-failing the flush/compaction, and warn once.
fn apply_no_cache(file: &File) {
    let fd = file.as_raw_fd();
    // SAFETY: fcntl with F_NOCACHE on a valid fd is safe. We hold a reference to `file`
    // for the duration of the call so the fd is guaranteed open.
    #[expect(unsafe_code, reason = "libc FFI")]
    let rc = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };

    if rc < 0 {
        log_f_nocache_unsupported_once(&io::Error::last_os_error());
    }
}

/// Warns a single time per process when `F_NOCACHE` could not be applied.
fn log_f_nocache_unsupported_once(e: &io::Error) {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        log::warn!(
            "F_NOCACHE not applied (first observed: {e}); proceeding with buffered I/O. \
             The use_direct_io_for_* config flags will have no cache-bypass effect on this file.",
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn open_read_direct_succeeds() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("probe");
        std::fs::write(&path, b"hello world")?;
        let _ = open_read_direct(&path)?;
        Ok(())
    }
}
