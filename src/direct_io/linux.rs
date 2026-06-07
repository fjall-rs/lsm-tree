// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Linux-specific direct I/O: `O_DIRECT`.
//!
//! `O_DIRECT` is set at open time. The kernel rejects subsequent `read`/`write` calls
//! whose user buffer pointer, file offset, and length are not all aligned to the
//! underlying block device's logical block size (queryable at runtime).

use std::{
    fs::{File, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::OnceLock,
};

// The numeric value of `O_DIRECT` varies by architecture (x86 = 0x4000, aarch64 =
// 0x10000, mips = 0x8000, powerpc = 0x20000, sparc = 0x100000, ...). `libc::O_DIRECT`
// resolves to the correct per-target value at compile time.
fn o_direct_flag() -> i32 {
    libc::O_DIRECT
}

/// Opens an existing file for reading with `O_DIRECT` set.
pub fn open_read_direct(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(o_direct_flag())
        .open(path)
}

/// Creates a new file for writing with `O_DIRECT` set. Fails if the file already exists,
/// matching the existing `File::create_new` semantics used by table/blob writers.
pub fn create_write_direct(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(o_direct_flag())
        .open(path)
}

/// Opens (or creates) a file for writing with `O_DIRECT` set, truncating if it exists.
/// Used by blob file writers that call `File::create` rather than `create_new`.
pub fn create_or_truncate_write_direct(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(o_direct_flag())
        .open(path)
}

/// Reports the logical block size that the kernel will accept for `O_DIRECT` I/O.
///
/// We use the system page size as a conservative upper bound (the kernel accepts
/// any power-of-two alignment at or above the device logical block size, and
/// page size is at least 512 B on Linux). Querying the device's exact logical
/// block size would require stat'ing the underlying block device, which is not
/// worth the complexity.
pub fn block_alignment() -> usize {
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        // SAFETY: sysconf is async-signal-safe and takes a constant; no aliasing concerns.
        #[expect(unsafe_code, reason = "libc FFI")]
        let page_size = unsafe { libc_sysconf_pagesize() };

        // Guard against a non-power-of-two return: `Layout::from_size_align` (used by
        // `AlignedBuffer::new`) panics on non-power-of-two alignment, and every direct-I/O
        // backend assumes alignment is a power of two. 4 KiB is the kernel-default page
        // size on every modern Linux target.
        let candidate = match usize::try_from(page_size) {
            Ok(v) if v > 0 => v,
            _ => 4_096,
        };
        if candidate >= 512 && candidate.is_power_of_two() {
            candidate
        } else {
            log::warn!(
                "sysconf(_SC_PAGESIZE) returned {candidate} which is not a power-of-two >= 512; falling back to 4 KiB",
            );
            4_096
        }
    })
}

/// Checks whether `O_DIRECT` is currently set on an opened file descriptor.
///
/// Used in tests to assert that the open path actually requested direct I/O.
/// Linux is the only backend where this is observable post-open (`fcntl(F_GETFL)`
/// returns the open flags); macOS has no equivalent query.
#[cfg(test)]
pub fn is_direct_io_enabled(file: &File) -> io::Result<bool> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    // SAFETY: fcntl(F_GETFL) on a valid fd is safe; the file is borrowed for the call.
    #[expect(unsafe_code, reason = "libc FFI")]
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };

    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok((flags & o_direct_flag()) != 0)
}

/// Clears `O_DIRECT` from an open file descriptor.
///
/// Used after a non-EOF direct read returns an unaligned byte count: continuing
/// with `O_DIRECT` would make the next implicit-offset read fail with `EINVAL`,
/// while the buffered path can safely continue from that offset.
pub fn disable_direct_io(file: &File) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    // SAFETY: fcntl(F_GETFL) on a valid fd is safe; the file is borrowed for the call.
    #[expect(unsafe_code, reason = "libc FFI")]
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };

    if flags < 0 {
        return Err(io::Error::last_os_error());
    }

    let new_flags = flags & !o_direct_flag();
    if new_flags == flags {
        return Ok(());
    }

    // SAFETY: fcntl(F_SETFL) updates status flags on a valid fd.
    #[expect(unsafe_code, reason = "libc FFI")]
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, new_flags) };

    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

// Thin wrapper around `sysconf(_SC_PAGESIZE)`. Returns `c_long` (the libc
// signature: `i32` on 32-bit targets, `i64` on 64-bit). Returns -1 on error.
#[expect(unsafe_code, reason = "libc FFI")]
unsafe fn libc_sysconf_pagesize() -> libc::c_long {
    libc::sysconf(libc::_SC_PAGESIZE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn block_alignment_is_power_of_two_and_at_least_512() {
        let a = block_alignment();
        assert!(a >= 512);
        assert!(a.is_power_of_two());
    }

    #[test]
    fn open_read_direct_sets_o_direct() -> io::Result<()> {
        // Some Linux filesystems (notably tmpfs) reject O_DIRECT. Skip on those.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("probe");

        // Write some data with a buffered handle first.
        {
            let mut f = std::fs::File::create(&path)?;
            f.write_all(&vec![0u8; block_alignment()])?;
            f.sync_all()?;
        }

        match open_read_direct(&path) {
            Ok(direct) => {
                assert!(is_direct_io_enabled(&direct)?);
            }
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                eprintln!("filesystem rejects O_DIRECT; skipping assertion");
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }
}
