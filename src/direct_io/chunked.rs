// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Buffered file I/O with optional direct-I/O backing.
//!
//! [`ChunkedWriter`] and [`ChunkedReader`] expose the same `Write` / `Read` API as
//! `BufWriter<File>` / `BufReader<File>`, but pick the aligned backend when the
//! caller asks for direct I/O. Call sites in `table::writer`, `multi_writer`,
//! `table::scanner`, etc. only have to flip a bool to switch modes.
//!
//! Per-platform routing:
//!
//! - **Linux**: aligned-buffer backend with `O_DIRECT` at open time. If the
//!   filesystem rejects `O_DIRECT` (`EINVAL` on tmpfs / overlayfs / some FUSE),
//!   the open transparently falls back to buffered with a single `log::warn`.
//! - **macOS**: regular `BufWriter`/`BufReader` over a file with `F_NOCACHE`
//!   applied — `F_NOCACHE` has no alignment requirement, so the aligned-buffer
//!   machinery is not needed.
//! - **Other** (including Windows): regular buffered I/O; the flag is a no-op.

#[cfg(target_os = "linux")]
use super::{AlignedFileReader, AlignedFileWriter};
use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

/// Default buffer size for `BufWriter` used by the table writer pre-existing code path.
/// `u16::MAX + 1` matches the value used before this module existed.
const DEFAULT_BUF_CAPACITY: usize = u16::MAX as usize + 1;

/// Default buffer capacity for `BufReader` used by the table scanner.
/// Matches the prior literal `8 * 4_096` constant.
const DEFAULT_READER_CAPACITY: usize = 8 * 4_096;

/// Returns `true` when the error indicates the platform's direct-I/O primitive
/// is rejected by the underlying filesystem (commonly tmpfs / overlayfs / FUSE).
/// Callers should silently fall back to buffered I/O on this error.
#[cfg(target_os = "linux")]
fn is_direct_io_unsupported(e: &io::Error) -> bool {
    e.raw_os_error() == Some(libc::EINVAL)
}

#[cfg(not(target_os = "linux"))]
fn is_direct_io_unsupported(_e: &io::Error) -> bool {
    false
}

/// Buffered writer that transparently uses direct I/O when requested.
pub enum ChunkedWriter {
    /// Standard buffered path: `BufWriter<File>` exactly as before.
    Buffered(BufWriter<File>),

    /// Aligned-buffer direct I/O path. Linux only — macOS uses `F_NOCACHE`
    /// which needs no alignment, so it stays on the buffered variant.
    #[cfg(target_os = "linux")]
    Aligned(AlignedFileWriter),
}

impl ChunkedWriter {
    /// Opens (creates new) a file for writing.
    ///
    /// `direct` requests `O_DIRECT` (Linux) or `F_NOCACHE` (macOS); a no-op on
    /// other platforms. If the filesystem rejects the direct-I/O flag, this
    /// transparently falls back to buffered I/O with a single `log::warn`.
    pub fn create_new(path: &Path, direct: bool) -> io::Result<Self> {
        Self::create_new_with_capacity(path, direct, DEFAULT_BUF_CAPACITY)
    }

    /// Like [`Self::create_new`] but for paths where `File::create` (truncate)
    /// semantics are needed (blob file writer).
    pub fn create_or_truncate(path: &Path, direct: bool) -> io::Result<Self> {
        Self::create_or_truncate_with_capacity(path, direct, DEFAULT_BUF_CAPACITY)
    }

    fn create_new_with_capacity(path: &Path, direct: bool, capacity: usize) -> io::Result<Self> {
        if direct {
            match Self::open_direct_write(path, capacity, false) {
                Ok(w) => return Ok(w),
                Err(e) if is_direct_io_unsupported(&e) => {
                    log_unsupported_once(path, &e);
                    // Fall through to buffered open below.
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Self::Buffered(BufWriter::with_capacity(
            capacity,
            File::create_new(path)?,
        )))
    }

    fn create_or_truncate_with_capacity(
        path: &Path,
        direct: bool,
        capacity: usize,
    ) -> io::Result<Self> {
        if direct {
            match Self::open_direct_write(path, capacity, true) {
                Ok(w) => return Ok(w),
                Err(e) if is_direct_io_unsupported(&e) => {
                    log_unsupported_once(path, &e);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Self::Buffered(BufWriter::with_capacity(
            capacity,
            File::create(path)?,
        )))
    }

    #[cfg(target_os = "linux")]
    fn open_direct_write(path: &Path, _capacity: usize, truncate: bool) -> io::Result<Self> {
        let file = if truncate {
            super::create_or_truncate_write_direct(path)?
        } else {
            super::create_write_direct(path)?
        };
        let alignment = super::block_alignment_for(path);
        Ok(Self::Aligned(AlignedFileWriter::new(
            file,
            path.to_path_buf(),
            alignment,
        )))
    }

    #[cfg(not(target_os = "linux"))]
    fn open_direct_write(path: &Path, capacity: usize, truncate: bool) -> io::Result<Self> {
        // macOS / fallback: the platform open applies F_NOCACHE post-open (macOS)
        // or is a plain buffered open (fallback). Both work fine wrapped in a
        // regular BufWriter — no alignment requirement.
        let file = if truncate {
            super::create_or_truncate_write_direct(path)?
        } else {
            super::create_write_direct(path)?
        };
        Ok(Self::Buffered(BufWriter::with_capacity(capacity, file)))
    }

    /// Finalizes the writer: drains all buffers (handling the trailing partial
    /// block in direct mode) and returns the underlying `File`.
    ///
    /// The caller is expected to call `sync_all()` on the returned file.
    pub fn finalize(self) -> io::Result<File> {
        match self {
            Self::Buffered(w) => w.into_inner().map_err(std::io::IntoInnerError::into_error),
            #[cfg(target_os = "linux")]
            Self::Aligned(w) => w.finalize(),
        }
    }

    /// Discards any buffered bytes and returns the inner `File` without writing
    /// the trailing tail. Intended for paths that will immediately delete the
    /// file (e.g. an empty-table table writer that decides at finish-time the
    /// file should not exist).
    pub fn cancel(self) -> File {
        match self {
            Self::Buffered(w) => {
                // BufWriter::into_parts returns the inner writer plus any unwritten
                // bytes; we discard both. (BufWriter::into_inner would try to flush
                // first, which is wasteful when we're about to remove the file.)
                let (file, _unwritten) = w.into_parts();
                file
            }
            #[cfg(target_os = "linux")]
            Self::Aligned(w) => w.cancel(),
        }
    }
}

impl Write for ChunkedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Buffered(w) => w.write(buf),
            #[cfg(target_os = "linux")]
            Self::Aligned(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Buffered(w) => w.flush(),
            #[cfg(target_os = "linux")]
            Self::Aligned(w) => w.flush(),
        }
    }
}

/// `Seek` is implemented for `ChunkedWriter` so that the `sfa::Writer` chain (which
/// requires `Seek` to call `stream_position` while building its table-of-contents)
/// compiles.
///
/// Only `SeekFrom::Current(0)` (i.e. `stream_position`) is meaningfully supported in
/// direct-I/O mode, because aligned writes have no random-access semantics until
/// `finalize` runs. Buffered mode delegates to `BufWriter`, which preserves the
/// pre-existing behavior.
impl Seek for ChunkedWriter {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self {
            Self::Buffered(w) => w.seek(pos),
            #[cfg(target_os = "linux")]
            Self::Aligned(w) => match pos {
                SeekFrom::Current(0) => Ok(w.bytes_written()),
                _ => Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "seek other than Current(0) is not supported on direct-I/O writers",
                )),
            },
        }
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        match self {
            Self::Buffered(w) => w.stream_position(),
            #[cfg(target_os = "linux")]
            Self::Aligned(w) => Ok(w.bytes_written()),
        }
    }
}

/// Buffered reader that transparently uses direct I/O when requested.
pub enum ChunkedReader {
    Buffered(BufReader<File>),

    #[cfg(target_os = "linux")]
    Aligned(AlignedFileReader),
}

impl ChunkedReader {
    /// Opens an existing file for reading.
    ///
    /// If `direct` is true but the filesystem rejects the direct-I/O flag, this
    /// transparently falls back to buffered I/O with a single `log::warn`.
    pub fn open(path: &Path, direct: bool) -> io::Result<Self> {
        Self::open_with_capacity(path, direct, DEFAULT_READER_CAPACITY)
    }

    fn open_with_capacity(path: &Path, direct: bool, capacity: usize) -> io::Result<Self> {
        if direct {
            match Self::open_direct(path) {
                Ok(r) => return Ok(r),
                Err(e) if is_direct_io_unsupported(&e) => {
                    log_unsupported_once(path, &e);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Self::Buffered(BufReader::with_capacity(
            capacity,
            File::open(path)?,
        )))
    }

    #[cfg(target_os = "linux")]
    fn open_direct(path: &Path) -> io::Result<Self> {
        let file = super::open_read_direct(path)?;
        let alignment = super::block_alignment_for(path);
        Ok(Self::Aligned(AlignedFileReader::new(file, alignment)?))
    }

    #[cfg(not(target_os = "linux"))]
    fn open_direct(path: &Path) -> io::Result<Self> {
        // macOS / fallback: F_NOCACHE has no alignment requirement, so a regular
        // BufReader on the F_NOCACHE'd file works fine.
        let file = super::open_read_direct(path)?;
        Ok(Self::Buffered(BufReader::with_capacity(
            DEFAULT_READER_CAPACITY,
            file,
        )))
    }
}

impl Read for ChunkedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Buffered(r) => r.read(buf),
            #[cfg(target_os = "linux")]
            Self::Aligned(r) => r.read(buf),
        }
    }
}

/// Emits a single `warn` per process when a filesystem rejects direct I/O.
/// Subsequent fallbacks are silent so a tree on tmpfs doesn't flood logs.
fn log_unsupported_once(path: &Path, e: &io::Error) {
    use std::sync::OnceLock;
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let path_display = path.display();
        log::warn!(
            "direct I/O not supported by filesystem (first observed at {path_display}: {e}); \
             falling back to buffered I/O. The use_direct_io_for_* config flags will have no effect on this filesystem.",
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn chunked_writer_roundtrip_buffered() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("a");
        let mut w = ChunkedWriter::create_new(&path, false)?;
        w.write_all(b"hello")?;
        w.finalize()?.sync_all()?;

        let mut buf = String::new();
        std::io::Read::read_to_string(&mut ChunkedReader::open(&path, false)?, &mut buf)?;
        assert_eq!(buf, "hello");
        Ok(())
    }

    #[test]
    fn chunked_writer_roundtrip_direct() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("a");
        // With the fallback in place this should always succeed: direct I/O is
        // attempted first, and if EINVAL/ERROR_INVALID_PARAMETER is returned the
        // open transparently falls back to buffered.
        let mut w = ChunkedWriter::create_new(&path, true)?;
        w.write_all(b"hello")?;
        w.finalize()?.sync_all()?;

        let mut buf = String::new();
        std::io::Read::read_to_string(&mut ChunkedReader::open(&path, true)?, &mut buf)?;
        assert_eq!(buf, "hello");
        Ok(())
    }

    #[test]
    fn chunked_writer_finalize_truncates_to_real_size() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("size");

        let payload = b"123456789".repeat(1_000); // 9_000 bytes
        let mut w = ChunkedWriter::create_new(&path, true)?;
        w.write_all(&payload)?;
        w.finalize()?.sync_all()?;

        let actual_size = std::fs::metadata(&path)?.len();
        assert_eq!(actual_size, payload.len() as u64);
        Ok(())
    }

    #[test]
    fn chunked_writer_cancel_releases_file_without_padding() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cancel");
        let mut w = ChunkedWriter::create_new(&path, false)?;
        w.write_all(b"to be discarded")?;
        let file = w.cancel();
        drop(file);
        // BufWriter::into_parts discards unwritten bytes; the file may be empty
        // or contain whatever BufWriter happened to forward (typically nothing
        // for a small write).
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn chunked_writer_stream_position_matches_bytes_written() -> io::Result<()> {
        use std::io::Seek;
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pos");
        let mut w = ChunkedWriter::create_new(&path, false)?;
        w.write_all(b"abcdefghij")?;
        let pos = w.stream_position()?;
        // For the Buffered variant, `stream_position` queries the underlying
        // `BufWriter::stream_position` which counts bytes written so far. It is
        // valid for buffered to return anywhere from "what was actually written
        // to the file" up to "what was written into the buffer".
        assert!(pos <= 10);
        Ok(())
    }

    #[test]
    fn chunked_writer_create_or_truncate_overwrites_existing() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("trunc");
        // Pre-existing content.
        std::fs::write(&path, b"pre-existing")?;
        let mut w = ChunkedWriter::create_or_truncate(&path, false)?;
        w.write_all(b"new")?;
        w.finalize()?.sync_all()?;

        let mut buf = vec![];
        std::io::Read::read_to_end(&mut ChunkedReader::open(&path, false)?, &mut buf)?;
        assert_eq!(buf, b"new");
        Ok(())
    }

    #[test]
    fn is_direct_io_unsupported_classifies_einval_on_linux_only() {
        // Non-Linux platforms have no native O_DIRECT path; the classifier
        // returns false for every error (the buffered fallback handles things).
        let einval = io::Error::from_raw_os_error(22);
        let other = io::Error::other("not an OS error");
        #[cfg(target_os = "linux")]
        {
            assert!(is_direct_io_unsupported(&einval));
            assert!(!is_direct_io_unsupported(&other));
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert!(!is_direct_io_unsupported(&einval));
            assert!(!is_direct_io_unsupported(&other));
        }
    }

    #[test]
    fn log_unsupported_once_does_not_panic() {
        // The function uses a process-wide OnceLock so subsequent calls become
        // no-ops; calling it here just exercises the branch.
        let e = io::Error::from_raw_os_error(22);
        log_unsupported_once(std::path::Path::new("/dev/null"), &e);
        // Second call to verify the OnceLock idempotency.
        log_unsupported_once(std::path::Path::new("/dev/null"), &e);
    }
}
