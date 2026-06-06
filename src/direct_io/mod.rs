// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Direct (unbuffered) I/O primitives for compaction and flush.
//!
//! Bypassing the OS page cache during compaction prevents the sequential, read-once
//! scan from evicting hot user-read data. Per platform:
//!
//! - **Linux**: `O_DIRECT` at open time. Requires page-aligned user buffers, file
//!   offsets, and lengths; alignment is queried via `sysconf(_SC_PAGESIZE)`.
//! - **macOS**: `F_NOCACHE` applied via `fcntl` after open. No alignment requirement.
//! - **Other** (including Windows): no-op; opens fall back to buffered I/O. The
//!   `use_direct_io_for_*` config flags have no effect.

// Aligned-buffer machinery is always compiled (its unit tests run on every
// platform), but only wired into `ChunkedWriter`/`ChunkedReader` on Linux where
// O_DIRECT requires alignment. Elsewhere the types are dead code
// (`cfg_attr(... allow(dead_code))` below).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod aligned_buffer;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod aligned_reader;
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod aligned_writer;
mod chunked;

/// Aligned-buffer capacity (in alignment units) for the read and write paths.
///
/// At 4 KiB alignment this is 64 KiB — the amortization sweet spot for
/// compaction-grade sequential I/O.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub const BUFFER_BLOCKS: usize = 16;

#[cfg(test)]
mod syscall_assertions;

#[cfg(target_os = "linux")]
pub use aligned_reader::AlignedFileReader;
#[cfg(target_os = "linux")]
pub use aligned_writer::AlignedFileWriter;
pub use chunked::{ChunkedReader, ChunkedWriter};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
use macos as platform;

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod fallback;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
use fallback as platform;

use std::{fs::File, io, path::Path};

/// Test-only counter of direct-write opens (`create_write_direct` +
/// `create_or_truncate_write_direct`). Lets tests assert that the flush /
/// compaction *write* path actually requested direct I/O, not merely that the
/// finished files can be re-opened with `O_DIRECT` for reading.
#[cfg(test)]
pub(crate) static DIRECT_WRITE_OPEN_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Opens an existing file for reading with direct I/O enabled.
///
/// On Linux, `O_DIRECT` is set atomically at open. On macOS, this opens the
/// file and then applies `F_NOCACHE` to the descriptor. On other platforms,
/// this is equivalent to `File::open`.
pub fn open_read_direct(path: &Path) -> io::Result<File> {
    platform::open_read_direct(path)
}

/// Creates a new file for writing with direct I/O enabled. Fails if the file exists.
///
/// Equivalent to `File::create_new` on unsupported platforms.
pub fn create_write_direct(path: &Path) -> io::Result<File> {
    #[cfg(test)]
    DIRECT_WRITE_OPEN_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    platform::create_write_direct(path)
}

/// Creates or truncates a file for writing with direct I/O enabled.
///
/// Equivalent to `File::create` on unsupported platforms.
pub fn create_or_truncate_write_direct(path: &Path) -> io::Result<File> {
    #[cfg(test)]
    DIRECT_WRITE_OPEN_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    platform::create_or_truncate_write_direct(path)
}

/// Returns the alignment to use for direct I/O. Linux only: `sysconf(_SC_PAGESIZE)`.
/// Other platforms do not use the aligned-buffer path.
#[cfg(target_os = "linux")]
#[must_use]
pub fn block_alignment_for(_path: &Path) -> usize {
    platform::block_alignment()
}

/// Test-only helper: returns whether `O_DIRECT` is set on an open fd.
///
/// Linux only: `fcntl(F_GETFL)` exposes the open flags. macOS (`F_NOCACHE`)
/// has no equivalent post-open query.
#[cfg(all(test, target_os = "linux"))]
pub fn is_direct_io_enabled(file: &File) -> io::Result<bool> {
    platform::is_direct_io_enabled(file)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn block_alignment_for_returns_power_of_two_at_least_512() {
        let a = block_alignment_for(std::path::Path::new("."));
        assert!(a >= 512);
        assert!(a.is_power_of_two());
    }
}
