// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Pluggable filesystem abstraction for I/O backends.
//!
//! The [`Fs`] trait is intended to abstract the filesystem operations
//! that lsm-tree performs, allowing alternative backends such as
//! `io_uring`, in-memory filesystems for deterministic testing, or cloud
//! blob storage. Call-site migration is tracked in separate issues.
//!
//! The default implementation [`StdFs`] delegates to [`std::fs`] and
//! is a zero-sized type, so it adds no runtime overhead when used as a
//! monomorphized generic parameter.
//!
//! # Platform-specific backends
//!
//! - **Linux 5.6+**: `IoUringFs` — batched SQE submission via `io_uring`
//!   (feature-gated: `io-uring`)
//! - **Windows**: IOCP (`IoCompletionPort`) could provide similar batched
//!   completion semantics — not yet implemented, tracked for when Windows
//!   becomes a production target
//! - **macOS / BSD**: no batched I/O API exists (`dispatch_io` and `kqueue`
//!   do not help for storage I/O patterns); [`StdFs`] is the correct choice

mod std_fs;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod io_uring_fs;

pub use std_fs::StdFs;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_fs::{is_io_uring_available, IoUringFs};

use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

/// Options for opening a file through the [`Fs`] trait.
///
/// Mirrors the builder API of [`std::fs::OpenOptions`].
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors std::fs::OpenOptions which uses bool flags for each mode"
)]
#[derive(Clone, Debug)]
pub struct FsOpenOptions {
    /// Open for reading.
    pub read: bool,
    /// Open for writing.
    pub write: bool,
    /// Create the file if it does not exist.
    pub create: bool,
    /// Create a new file and fail if it already exists.
    pub create_new: bool,
    /// Truncate the file to zero length on open.
    pub truncate: bool,
    /// Open in append mode, so writes go to the end of the file.
    pub append: bool,
}

impl Default for FsOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl FsOpenOptions {
    /// Creates a new set of options with everything disabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
            create_new: false,
            truncate: false,
            append: false,
        }
    }

    /// Sets the `read` flag.
    #[must_use]
    pub const fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Sets the `write` flag.
    #[must_use]
    pub const fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Sets the `create` flag.
    #[must_use]
    pub const fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Sets the `create_new` flag.
    #[must_use]
    pub const fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Sets the `truncate` flag.
    #[must_use]
    pub const fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Sets the `append` flag.
    #[must_use]
    pub const fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }
}

/// Metadata about a file or directory.
#[derive(Clone, Debug)]
pub struct FsMetadata {
    /// Size in bytes. For directories the value is platform-dependent.
    pub len: u64,
    /// Whether this entry is a directory.
    pub is_dir: bool,
    /// Whether this entry is a regular file.
    pub is_file: bool,
}

/// A directory entry returned by [`Fs::read_dir`].
#[derive(Clone, Debug)]
pub struct FsDirEntry {
    /// Full path to the entry.
    pub path: PathBuf,
    /// File name component (without parent path).
    // String (not OsString) — lsm-tree uses numeric file names for tables/blobs.
    // StdFs::read_dir returns InvalidData for non-UTF-8 names (not lossy) since
    // any such name indicates filesystem corruption for this crate's usage.
    pub file_name: String,
    /// Whether this entry is a directory.
    pub is_dir: bool,
}

/// Filesystem operations on an open file handle.
///
/// Extends [`Read`] + [`Write`] + [`Seek`] with persistence and
/// metadata operations needed by the storage engine.
pub trait FsFile: Read + Write + Seek + Send + Sync {
    /// Flushes all OS-internal buffers and metadata to durable storage.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_all(&self) -> io::Result<()>;

    /// Flushes file data (but not necessarily metadata) to durable storage.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_data(&self) -> io::Result<()>;

    /// Returns metadata for this open file handle.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if metadata cannot be retrieved.
    fn metadata(&self) -> io::Result<FsMetadata>;

    /// Truncates or extends the file to the specified length.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the length change fails.
    fn set_len(&self, size: u64) -> io::Result<()>;

    /// Reads bytes from the file at the given offset without changing the
    /// file cursor position.
    ///
    /// Equivalent to `pread(2)` on Unix. Multiple threads can call this
    /// concurrently on the same file handle without synchronization.
    ///
    /// Implementations must provide *fill-or-EOF* semantics: on success,
    /// this method either fills `buf` completely and returns
    /// `Ok(buf.len())`, or returns `Ok(n)` with `n < buf.len()` only if
    /// the read has reached EOF. Callers may rely on a short read
    /// indicating EOF and therefore do not need a retry loop.
    ///
    /// Implementations are responsible for handling OS-level short reads
    /// and interrupts internally (for example, by retrying on `EINTR`)
    /// so that the above guarantee holds unless an error is returned.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the read fails.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Acquires an exclusive (write) lock on this file.
    ///
    /// Blocks until the lock is acquired.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if locking fails or is unsupported.
    fn lock_exclusive(&self) -> io::Result<()>;
}

/// Pluggable filesystem abstraction.
///
/// Intended to cover all filesystem operations that lsm-tree performs.
/// The default implementation [`StdFs`] delegates to [`std::fs`].
///
/// # Object safety
///
/// `Fs` is object-safe and can be used as `Arc<dyn Fs>` directly:
/// ```
/// # use lsm_tree::fs::{Fs, StdFs};
/// # use std::sync::Arc;
/// let _: Arc<dyn Fs> = Arc::new(StdFs);
/// ```
pub trait Fs: Send + Sync + 'static {
    /// Opens a file at `path` with the given options.
    ///
    /// Returns a boxed file handle. For syscall-backed implementations
    /// like [`StdFs`], the allocation and dynamic dispatch overhead is
    /// typically negligible compared to the underlying I/O operations.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened.
    // Box<dyn FsFile> is intentionally 'static (the default) — file handles are
    // owned values that do not borrow from the Fs instance that created them.
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>>;

    /// Recursively creates all directories leading to `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if directory creation fails.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Returns all entries in a directory (order is unspecified).
    ///
    /// Returns a `Vec` rather than a streaming iterator because
    /// `read_dir` is a cold-path operation (recovery, compaction file
    /// listing) where directory sizes are expected to remain small.
    /// Callers that need a specific order must sort the result.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be read or if any
    /// individual entry fails.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>>;

    /// Removes a single file.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be removed.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Recursively removes a directory and all of its contents.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be removed.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Renames a file or directory from `from` to `to`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the rename fails.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Returns metadata for the file or directory at `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if metadata cannot be retrieved.
    fn metadata(&self, path: &Path) -> io::Result<FsMetadata>;

    /// Ensures directory metadata is persisted to durable storage.
    ///
    /// On platforms that do not support directory fsync (e.g. Windows),
    /// this may be a no-op.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_directory(&self, path: &Path) -> io::Result<()>;

    /// Returns `Ok(true)` if a file or directory exists at `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the existence of `path` cannot be determined
    /// (for example, due to permission issues or transient backend failures).
    fn exists(&self, path: &Path) -> io::Result<bool>;
}
