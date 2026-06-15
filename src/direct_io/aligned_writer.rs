// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{aligned_buffer::AlignedBuffer, BUFFER_BLOCKS};
use std::{
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Writer that emits aligned, block-sized chunks to an `O_DIRECT` file.
///
/// `O_DIRECT` requires the buffer pointer, file offset, and length to all be
/// multiples of the device's logical block size. This writer accumulates bytes
/// into an aligned heap buffer and only emits full, aligned chunks until
/// `finalize` is called.
///
/// `finalize` writes the (possibly partial) trailing block through a separate
/// buffered handle (no `O_DIRECT`). The alternative, a zero-padded block plus
/// `ftruncate`, leaves a brief window where the file is larger than its content.
///
/// ## Runtime fallback
///
/// The open-time probe in `chunked` catches filesystems that reject `O_DIRECT`
/// at open (tmpfs / overlayfs / many FUSE). A few accept the open but reject the
/// writes (`EINVAL`/`EOPNOTSUPP`), and a device whose logical block size exceeds
/// the page-size alignment would too. On such a runtime rejection the writer
/// drops `O_DIRECT` (via `fcntl`) and replays the write buffered from the last
/// durable boundary, so the flush/compaction finishes instead of failing. This
/// mirrors the reader's runtime fallback.
///
/// ## Error semantics
///
/// If any `Write::write` or `Write::flush` call returns an error, the writer enters
/// a *poisoned* state: all subsequent write/flush/finalize calls return an error
/// without touching the file. The on-disk file keeps exactly the bytes from the
/// last successful aligned spill, never extended beyond what reached disk. Drop in
/// the poisoned state is a no-op.
pub struct AlignedFileWriter {
    /// `Option` so `finalize` / `cancel` can take ownership of the file out of the
    /// struct even though `AlignedFileWriter` implements `Drop`. `None` only between
    /// the `take` and the function returning.
    file: Option<File>,
    /// Path of the file backing this writer. Needed by `finalize_in_place` to reopen
    /// a buffered handle for the trailing partial-block write.
    path: PathBuf,
    buffer: AlignedBuffer,
    /// Bytes currently buffered (between 0 and `buffer.capacity()`).
    buffer_pos: usize,
    /// Total real bytes written and accepted (never includes bytes from a failed
    /// spill; see `Self::write`).
    bytes_written: u64,
    /// File size at the last successful write completion. On any `write_all`
    /// failure we best-effort `set_len(bytes_on_disk)` so the on-disk file does
    /// not exceed the last successful boundary (the documented error contract).
    bytes_on_disk: u64,
    alignment: usize,
    /// Set to `true` after `finalize`/`cancel` runs so a double-finalize is a no-op.
    finalized: bool,
    /// Set to `true` after any I/O failure. Subsequent ops fail without touching the file.
    poisoned: bool,
}

impl AlignedFileWriter {
    /// Wraps an already-opened file (assumed to have direct-I/O enabled).
    /// `path` is retained so `finalize` can reopen the file with a buffered handle
    /// for the trailing partial-block write.
    #[must_use]
    pub fn new(file: File, path: PathBuf, alignment: usize) -> Self {
        debug_assert!(
            alignment.is_power_of_two(),
            "AlignedFileWriter alignment ({alignment}) must be a non-zero power of two",
        );
        let capacity = alignment.saturating_mul(BUFFER_BLOCKS).max(alignment);
        Self {
            file: Some(file),
            path,
            buffer: AlignedBuffer::new(capacity, alignment),
            buffer_pos: 0,
            bytes_written: 0,
            bytes_on_disk: 0,
            alignment,
            finalized: false,
            poisoned: false,
        }
    }

    /// Best-effort truncate back to the last successful write boundary after a
    /// `write_all` partial-write failure. Logs and returns silently on failure;
    /// the writer is already poisoned at this point.
    fn truncate_to_last_boundary(file: &File, path: &Path, target: u64) {
        if let Err(e) = file.set_len(target) {
            log::warn!(
                "AlignedFileWriter: best-effort truncate of {} to {target} after I/O failure failed: {e:?}",
                path.display(),
            );
        }
    }

    /// Writes `buf` through the `O_DIRECT` handle, falling back to buffered I/O
    /// if the kernel rejects the direct write at runtime with `EINVAL`/`EOPNOTSUPP`.
    ///
    /// This covers filesystems that accept `O_DIRECT` at open but reject writes
    /// (some FUSE/overlay setups) and devices whose logical block size exceeds the
    /// page-size alignment, cases the open-time probe cannot detect. `boundary` is
    /// the last durable file length: on a rejection we truncate back to it (undoing
    /// any partial write) and replay `buf` buffered from that offset, so nothing is
    /// lost or double-written. The handle is no longer `O_DIRECT` afterwards, so
    /// later writes stay buffered too.
    ///
    /// On the happy path (no rejection) this is just `file.write_all(buf)`.
    fn write_all_direct_or_fallback(
        file: &mut File,
        path: &Path,
        boundary: u64,
        buf: &[u8],
    ) -> io::Result<()> {
        #[cfg(all(target_os = "linux", debug_assertions))]
        let direct_result = if take_forced_write_einval() {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        } else {
            file.write_all(buf)
        };
        #[cfg(not(all(target_os = "linux", debug_assertions)))]
        let direct_result = file.write_all(buf);

        match direct_result {
            Ok(()) => return Ok(()),
            Err(e) if super::is_runtime_direct_io_unsupported(&e) => {
                log_runtime_fallback_once(path, &e);
            }
            Err(e) => return Err(e),
        }

        // Direct write rejected at runtime: undo any partial write, drop O_DIRECT,
        // and replay the (already-aligned, but now alignment-irrelevant) buffer
        // buffered from the last durable boundary.
        file.set_len(boundary)?;
        disable_direct_io_for_writer(file)?;
        file.seek(SeekFrom::Start(boundary))?;
        file.write_all(buf)
    }

    /// Returns an immutable reference to the underlying file, or `None` after
    /// `finalize`/`cancel` consumed it. Used by callers that need to e.g. query
    /// the file's metadata while construction is still in progress.
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub fn file(&self) -> Option<&File> {
        self.file.as_ref()
    }

    /// Returns the total real bytes accepted so far, which equals the final file
    /// size (some may still be buffered, not yet on disk).
    #[must_use]
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Drains the trailing partial block and returns the inner `File`.
    ///
    /// Any complete aligned chunks still buffered are written via the direct
    /// handle; the final sub-alignment tail (if any) goes through a separate
    /// buffered (non-`O_DIRECT`) handle, so the file ends at exactly the real
    /// byte count. No zero padding is persisted and no `set_len` is needed.
    pub fn finalize(mut self) -> io::Result<File> {
        self.finalize_in_place()?;
        #[expect(
            clippy::expect_used,
            reason = "self is consumed; file was Some at construction and finalize_in_place did not take it"
        )]
        Ok(self.file.take().expect("file should still be present"))
    }

    /// Releases the inner `File` without writing the trailing partial block and
    /// without truncating. The on-disk content equals what the last successful
    /// `spill_aligned` left.
    ///
    /// Intended for the "we're about to delete this file anyway" path
    /// (`table::writer::Writer::finish` with `item_count == 0`).
    pub fn cancel(mut self) -> File {
        // Mark finalized so Drop doesn't attempt I/O after we hand the file back.
        self.finalized = true;
        #[expect(
            clippy::expect_used,
            reason = "self is consumed; file is still Some unless finalize was called, which is not the case here"
        )]
        self.file.take().expect("file should still be present")
    }

    fn finalize_in_place(&mut self) -> io::Result<()> {
        if self.finalized {
            return Ok(());
        }
        self.finalized = true;

        if self.poisoned {
            // Don't touch the file in any way: leave it at the last successful
            // spill boundary so recovery can deal with it.
            return Ok(());
        }

        if self.buffer_pos == 0 {
            // Buffer is empty: nothing to flush, file is already aligned.
            return Ok(());
        }

        // All complete aligned chunks have already spilled to disk via the direct
        // handle. What's left in the buffer is the sub-alignment tail (usually
        // 0 < buffer_pos < alignment, but possibly a multiple of alignment if the
        // last spill was deferred; both are handled below).
        let aligned_tail_len = (self.buffer_pos / self.alignment) * self.alignment;
        if aligned_tail_len > 0 {
            // `file` is always `Some` here: the only paths that take it
            // (`finalize`/`cancel`) set `finalized`, which short-circuits at the
            // top of this function. Treat `None` as a hard error rather than
            // silently dropping the tail (which would mismatch `finalize`'s take).
            let Some(file) = self.file.as_mut() else {
                self.poisoned = true;
                return Err(io::Error::other(
                    "AlignedFileWriter: file handle missing during finalize",
                ));
            };
            #[expect(
                clippy::indexing_slicing,
                reason = "aligned_tail_len <= buffer_pos <= capacity"
            )]
            if let Err(e) = Self::write_all_direct_or_fallback(
                file,
                &self.path,
                self.bytes_on_disk,
                &self.buffer.as_slice()[..aligned_tail_len],
            ) {
                Self::truncate_to_last_boundary(file, &self.path, self.bytes_on_disk);
                self.poisoned = true;
                return Err(e);
            }
            self.bytes_on_disk += aligned_tail_len as u64;
        }

        let unaligned_tail_len = self.buffer_pos - aligned_tail_len;
        if unaligned_tail_len > 0 {
            // Drop the direct-I/O handle before opening the buffered one: avoids
            // any chance of overlapping cached/uncached state on the same file.
            drop(self.file.take());

            // Open a buffered handle (no O_DIRECT) in append mode so the write
            // lands at the current end-of-file, which is the aligned boundary
            // the direct handle stopped at.
            let mut buffered = match std::fs::OpenOptions::new().append(true).open(&self.path) {
                Ok(f) => f,
                Err(e) => {
                    self.poisoned = true;
                    return Err(e);
                }
            };
            #[expect(
                clippy::indexing_slicing,
                reason = "buffer_pos bounded by capacity from the write/spill invariants"
            )]
            if let Err(e) = buffered.write_all(
                &self.buffer.as_slice()[aligned_tail_len..aligned_tail_len + unaligned_tail_len],
            ) {
                Self::truncate_to_last_boundary(&buffered, &self.path, self.bytes_on_disk);
                self.poisoned = true;
                return Err(e);
            }
            self.bytes_on_disk += unaligned_tail_len as u64;
            self.file = Some(buffered);
        }

        self.buffer_pos = 0;
        Ok(())
    }
}

impl io::Write for AlignedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.poisoned {
            return Err(io::Error::other(
                "AlignedFileWriter is poisoned from a previous I/O failure",
            ));
        }
        if self.finalized {
            return Err(io::Error::other(
                "AlignedFileWriter is finalized; further writes are not allowed",
            ));
        }

        let capacity = self.buffer.capacity();
        let space = capacity - self.buffer_pos;
        let to_copy = buf.len().min(space);

        // Indices bounded by `to_copy.min(buf.len(), space)`, all known to fit.
        #[expect(
            clippy::indexing_slicing,
            reason = "to_copy is bounded by both buf.len() and the remaining buffer capacity"
        )]
        {
            self.buffer.as_mut_slice()[self.buffer_pos..self.buffer_pos + to_copy]
                .copy_from_slice(&buf[..to_copy]);
        }
        let new_buffer_pos = self.buffer_pos + to_copy;
        self.buffer_pos = new_buffer_pos;

        if new_buffer_pos == capacity {
            // Try to spill the now-full buffer. If the spill fails the in-memory
            // copy we just did is logically invalid: undo `buffer_pos` and do NOT
            // credit `bytes_written`, then poison. Bytes already on disk from
            // previous successful spills remain intact (and `spill_aligned`
            // has best-effort truncated any partial write back).
            if let Err(e) = self.spill_aligned() {
                self.buffer_pos -= to_copy;
                self.poisoned = true;
                return Err(e);
            }
        }
        self.bytes_written += to_copy as u64;

        Ok(to_copy)
    }

    /// Drains the complete aligned chunks accumulated so far. The unaligned tail
    /// (between 0 and `alignment - 1` bytes) stays in the buffer and is only
    /// written by `finalize`.
    ///
    /// This is a deliberate divergence from `std::io::Write::flush`'s
    /// "all intermediately buffered contents reach their destination" contract:
    /// `O_DIRECT` rejects sub-alignment writes, so we cannot honor that contract
    /// until we know we're done writing.
    fn flush(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "AlignedFileWriter is poisoned from a previous I/O failure",
            ));
        }
        if self.finalized {
            // After finalize/cancel the buffer is drained and the file may be
            // taken; nothing to flush. Mirrors the `finalized` guard in `write`.
            return Ok(());
        }

        let aligned_len = (self.buffer_pos / self.alignment) * self.alignment;
        if aligned_len == 0 {
            return Ok(());
        }

        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        #[expect(
            clippy::indexing_slicing,
            reason = "aligned_len <= buffer_pos <= capacity"
        )]
        if let Err(e) = Self::write_all_direct_or_fallback(
            file,
            &self.path,
            self.bytes_on_disk,
            &self.buffer.as_slice()[..aligned_len],
        ) {
            Self::truncate_to_last_boundary(file, &self.path, self.bytes_on_disk);
            self.poisoned = true;
            return Err(e);
        }
        self.bytes_on_disk += aligned_len as u64;

        // Shift any remaining bytes to the front of the buffer.
        let tail = self.buffer_pos - aligned_len;
        if tail > 0 {
            let buf = self.buffer.as_mut_slice();
            buf.copy_within(aligned_len..aligned_len + tail, 0);
        }
        self.buffer_pos = tail;

        Ok(())
    }
}

impl AlignedFileWriter {
    fn spill_aligned(&mut self) -> io::Result<()> {
        // Caller guarantees the buffer is full and full == multiple of alignment.
        debug_assert_eq!(self.buffer_pos % self.alignment, 0);

        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        #[expect(
            clippy::indexing_slicing,
            reason = "buffer_pos == capacity here per the debug_assert above"
        )]
        if let Err(e) = Self::write_all_direct_or_fallback(
            file,
            &self.path,
            self.bytes_on_disk,
            &self.buffer.as_slice()[..self.buffer_pos],
        ) {
            Self::truncate_to_last_boundary(file, &self.path, self.bytes_on_disk);
            return Err(e);
        }
        self.bytes_on_disk += self.buffer_pos as u64;
        self.buffer_pos = 0;
        Ok(())
    }
}

/// Clears `O_DIRECT` from the writer's fd so the rest of the file is written
/// buffered. No-op on non-Linux (the aligned path is Linux-only).
#[cfg(target_os = "linux")]
fn disable_direct_io_for_writer(file: &File) -> io::Result<()> {
    super::linux::disable_direct_io(file)
}

#[cfg(not(target_os = "linux"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "matches the Linux helper signature so the write path has one call site"
)]
fn disable_direct_io_for_writer(_file: &File) -> io::Result<()> {
    Ok(())
}

/// Warns a single time per process when a write falls back from direct to
/// buffered I/O at runtime, so a misconfigured filesystem doesn't flood logs.
fn log_runtime_fallback_once(path: &Path, e: &io::Error) {
    use std::sync::OnceLock;
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        log::warn!(
            "direct I/O write rejected at runtime (first observed at {}: {e}); \
             disabling O_DIRECT and continuing with buffered I/O for affected files.",
            path.display(),
        );
    });
}

// Debug-only (Linux) test hook: when armed via `arm_forced_write_einval`, the
// next direct write simulates an `EINVAL` rejection so the runtime
// buffered-fallback path can be exercised deterministically without a real
// O_DIRECT-rejecting filesystem. One-shot per arming; never compiled into
// release builds.
#[cfg(all(target_os = "linux", debug_assertions))]
thread_local! {
    static FORCE_WRITE_EINVAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(all(target_os = "linux", debug_assertions))]
fn take_forced_write_einval() -> bool {
    FORCE_WRITE_EINVAL.with(|c| {
        let armed = c.get();
        if armed {
            c.set(false);
        }
        armed
    })
}

#[cfg(all(test, target_os = "linux", debug_assertions))]
fn arm_forced_write_einval() {
    FORCE_WRITE_EINVAL.with(|c| c.set(true));
}

impl Drop for AlignedFileWriter {
    fn drop(&mut self) {
        // If the caller didn't finalize/cancel and we're not poisoned, drain the
        // tail. A failure here can leave the file missing its sub-alignment tail
        // (up to one alignment unit), so log loudly: data integrity is at stake
        // and the caller can't react. Drop-time finalization is best-effort and
        // does not fsync; production paths must call `finalize()` explicitly.
        if !self.finalized && !self.poisoned {
            if let Err(e) = self.finalize_in_place() {
                log::error!(
                    "AlignedFileWriter dropped without explicit finalize and finalize_in_place failed: {e:?}; \
                     file may be missing up to one alignment unit of trailing bytes",
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use test_log::test;

    fn alignment_for_test() -> usize {
        // Use a small alignment so tests can exercise sub-block tails without
        // requiring megabytes of data. Power of two >= 512 is what real platforms
        // would return.
        512
    }

    fn write_via_aligned(path: &std::path::Path, data: &[u8], alignment: usize) -> io::Result<()> {
        // Open without direct I/O for the test. The writer logic doesn't require
        // it; O_DIRECT only adds kernel-level alignment enforcement, which we
        // don't need to verify the buffering logic.
        let file = std::fs::File::create(path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), alignment);
        writer.write_all(data)?;
        writer.finalize()?.sync_all()?;
        Ok(())
    }

    #[test]
    fn aligned_writer_exact_alignment_boundary() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("exact");
        let align = alignment_for_test();
        let data = vec![0xAB_u8; align * 3];

        write_via_aligned(&path, &data, align)?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(data, roundtrip);
        assert_eq!(roundtrip.len() as u64, data.len() as u64);
        Ok(())
    }

    #[test]
    fn aligned_writer_unaligned_tail_truncates_correctly() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("tail");
        let align = alignment_for_test();
        // 2 full blocks + 17 bytes of tail.
        let data = (0..(align * 2 + 17))
            .map(|i| (i as u8).wrapping_mul(31))
            .collect::<Vec<_>>();

        write_via_aligned(&path, &data, align)?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(data.len(), roundtrip.len());
        assert_eq!(data, roundtrip);
        Ok(())
    }

    #[test]
    fn aligned_writer_smaller_than_one_block() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("small");
        let align = alignment_for_test();
        let data = b"hello world".to_vec();

        write_via_aligned(&path, &data, align)?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(data, roundtrip);
        Ok(())
    }

    #[test]
    fn aligned_writer_empty() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("empty");
        let align = alignment_for_test();

        write_via_aligned(&path, &[], align)?;

        let meta = std::fs::metadata(&path)?;
        assert_eq!(meta.len(), 0);
        Ok(())
    }

    #[test]
    fn aligned_writer_many_small_writes() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("many");
        let align = alignment_for_test();

        let file = std::fs::File::create(&path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
        let total = align * 5 + 23;
        for i in 0..total {
            writer.write_all(&[(i & 0xFF) as u8])?;
        }
        writer.finalize()?.sync_all()?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(roundtrip.len(), total);
        for (i, &b) in roundtrip.iter().enumerate() {
            assert_eq!(b, (i & 0xFF) as u8, "mismatch at offset {i}");
        }
        Ok(())
    }

    #[test]
    fn aligned_writer_flush_preserves_unaligned_tail() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("flush");
        let align = alignment_for_test();

        let file = std::fs::File::create(&path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
        let data: Vec<u8> = (0..(align + 100)).map(|i| (i & 0xFF) as u8).collect();
        writer.write_all(&data)?;
        // Flush should emit the first aligned block; the 100-byte tail stays buffered.
        writer.flush()?;

        // File size is exactly one aligned block at this point (no truncation yet).
        let mid_len = std::fs::metadata(&path)?.len();
        assert_eq!(mid_len, align as u64);

        writer.finalize()?.sync_all()?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(data, roundtrip);
        Ok(())
    }

    #[test]
    fn aligned_writer_drop_without_finalize_does_not_lose_data() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("drop");
        let align = alignment_for_test();
        let data = b"unflushed data".to_vec();

        {
            let file = std::fs::File::create(&path)?;
            let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
            writer.write_all(&data)?;
            // Drop without calling finalize.
        }

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(data, roundtrip);
        Ok(())
    }

    #[test]
    fn aligned_writer_finalize_is_idempotent() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("idem");
        let align = alignment_for_test();

        let file = std::fs::File::create(&path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
        writer.write_all(b"abc")?;
        writer.finalize_in_place()?;
        writer.finalize_in_place()?;
        // Final on already-finalized: should also succeed.
        let f = writer.finalize()?;
        f.sync_all()?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(b"abc", &*roundtrip);
        Ok(())
    }

    #[test]
    fn aligned_writer_cancel_does_not_pad_or_truncate() -> io::Result<()> {
        // cancel is the "we're going to delete this file anyway" path used by
        // Writer::finish on empty-table. It must not extend the file with padding
        // (which would waste an fs call and could leave behind a poisoned file if
        // the caller's later remove_file fails).
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cancel");
        let align = alignment_for_test();

        let file = std::fs::File::create(&path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
        // Write less than one aligned block: nothing has been spilled to disk yet.
        writer.write_all(b"some bytes that never get spilled")?;
        let file = writer.cancel();
        drop(file);

        // The on-disk file is empty (no spill happened, no finalize, no padding).
        assert_eq!(std::fs::metadata(&path)?.len(), 0);
        Ok(())
    }

    /// Drives the spill path by writing more than one buffer capacity worth of
    /// bytes, then verifies `bytes_written` and the on-disk file are consistent.
    /// The buffer capacity is `BUFFER_BLOCKS * alignment`, so we need to push
    /// at least that much to force `spill_aligned`.
    #[test]
    fn aligned_writer_triggers_spill_at_capacity_boundary() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("spill");
        let align = alignment_for_test();
        let capacity = align * BUFFER_BLOCKS;
        // Two full buffer-capacity spills plus a partial tail.
        let data: Vec<u8> = (0..(capacity * 2 + align + 7))
            .map(|i| (i & 0xFF) as u8)
            .collect();

        let file = std::fs::File::create(&path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
        writer.write_all(&data)?;
        assert_eq!(writer.bytes_written(), data.len() as u64);
        assert!(writer.file().is_some());
        writer.finalize()?.sync_all()?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(data, roundtrip);
        Ok(())
    }

    /// Open the backing file read-only so any write at the kernel level fails
    /// with `EBADF`. This deterministically exercises the poison path without
    /// having to inject a custom `Write` impl.
    #[test]
    fn aligned_writer_poisons_on_write_failure_and_refuses_subsequent_ops() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("ro");
        // Create the file as empty so the read-only open succeeds.
        drop(std::fs::File::create(&path)?);
        let ro = std::fs::OpenOptions::new().read(true).open(&path)?;

        let align = alignment_for_test();
        let capacity = align * BUFFER_BLOCKS;
        let mut writer = AlignedFileWriter::new(ro, path.to_path_buf(), align);
        // First write fills exactly the buffer and triggers `spill_aligned`,
        // which calls `write_all` on the read-only handle and fails.
        let data = vec![0u8; capacity];
        assert!(writer.write_all(&data).is_err());
        // Subsequent writes return Err without touching the file.
        assert!(writer.write(b"more").is_err());
        // Flush is also refused.
        assert!(writer.flush().is_err());
        // Finalize is a no-op in the poisoned state (returns Ok, file unchanged).
        let f = writer.finalize()?;
        drop(f);
        assert_eq!(std::fs::metadata(&path)?.len(), 0);
        Ok(())
    }

    /// Verifies the documented invariant: after a write failure, the on-disk
    /// file size does not exceed the last successful write boundary. We can
    /// approximate this by opening the file read-only (so the very first spill
    /// fails) and confirming the file remains at its pre-spill length of zero.
    #[test]
    fn aligned_writer_truncates_to_last_boundary_on_failure() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("trunc");
        drop(std::fs::File::create(&path)?);
        let ro = std::fs::OpenOptions::new().read(true).open(&path)?;

        let align = alignment_for_test();
        let capacity = align * BUFFER_BLOCKS;
        let mut writer = AlignedFileWriter::new(ro, path.to_path_buf(), align);
        let data = vec![0u8; capacity];
        let _ = writer.write_all(&data);
        // bytes_on_disk should still be 0 (no successful spill).
        // Indirectly: the file size must match.
        assert_eq!(std::fs::metadata(&path)?.len(), 0);
        Ok(())
    }

    /// The runtime fallback: when a direct write is rejected mid-stream
    /// (simulated via the debug-only injection), the writer drops `O_DIRECT` and
    /// replays buffered from the last boundary, so the data still round-trips
    /// intact and the file ends at exactly the real byte count. Linux + debug
    /// only (the injection is compiled out otherwise).
    #[cfg(all(target_os = "linux", debug_assertions))]
    #[test]
    fn aligned_writer_falls_back_to_buffered_on_runtime_einval() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("runtime-fallback");
        let align = alignment_for_test();
        let capacity = align * BUFFER_BLOCKS;
        // Enough to force a spill (the first direct write, which the injection
        // rejects) plus a sub-alignment tail.
        let data: Vec<u8> = (0..(capacity + align + 7))
            .map(|i| (i & 0xFF) as u8)
            .collect();

        let file = std::fs::File::create(&path)?;
        let mut writer = AlignedFileWriter::new(file, path.to_path_buf(), align);
        super::arm_forced_write_einval();
        writer.write_all(&data)?;
        writer.finalize()?.sync_all()?;

        let mut roundtrip = vec![];
        std::fs::File::open(&path)?.read_to_end(&mut roundtrip)?;
        assert_eq!(
            roundtrip, data,
            "data must survive the runtime buffered fallback"
        );
        Ok(())
    }
}
