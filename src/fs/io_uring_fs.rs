// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! `io_uring`-backed [`Fs`] implementation for high-throughput I/O on Linux.
//!
//! Requires the `io-uring` feature flag and Linux 5.6+. Uses a dedicated
//! I/O thread that owns the `io_uring` ring instance. Submissions from
//! multiple threads are batched opportunistically — when several threads
//! submit I/O concurrently, their SQEs are combined into a single
//! `io_uring_enter` syscall.
//!
//! Hot-path operations (read, write, fsync) go through the ring.
//! Cold-path operations (mkdir, readdir, stat, rename, unlink) delegate
//! to [`std::fs`] since they do not benefit from `io_uring`.

use super::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use crate::HashMap;
use io_uring::{opcode, types, IoUring};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// Default number of `io_uring` submission queue entries.
const DEFAULT_SQ_ENTRIES: u32 = 256;

/// Probes whether `io_uring` is supported on the running kernel.
///
/// Creates a minimal 2-entry ring and immediately drops it. This tests
/// kernel support without hitting `memlock` rlimits that a full-sized
/// ring might exceed in constrained environments (containers, etc.).
/// [`IoUringFs::new`] may still fail if the default ring size exceeds
/// the process's resource limits.
#[must_use]
pub fn is_io_uring_available() -> bool {
    IoUring::new(2).is_ok()
}

// ---------------------------------------------------------------------------
// IoUringFs
// ---------------------------------------------------------------------------

/// `io_uring`-backed [`Fs`] implementation.
///
/// Routes hot-path I/O operations (read, write, fsync) through a
/// dedicated `io_uring` ring thread. Directory and metadata operations
/// delegate to [`std::fs`] since they do not benefit from `io_uring`.
///
/// Multiple `IoUringFs` clones and all [`IoUringFile`] handles opened
/// through them share the same ring thread.
///
/// # Example
///
/// ```no_run
/// use lsm_tree::fs::IoUringFs;
///
/// let fs = IoUringFs::new().expect("io_uring not available");
/// // Use as Config::new_with_fs(path, fs)
/// ```
pub struct IoUringFs {
    inner: Arc<RingThread>,
}

impl IoUringFs {
    /// Creates a new `IoUringFs` with the default ring size (256 entries).
    ///
    /// # Errors
    ///
    /// Returns an error if `io_uring` is not available on this kernel.
    pub fn new() -> io::Result<Self> {
        Self::with_ring_size(DEFAULT_SQ_ENTRIES)
    }

    /// Creates a new `IoUringFs` with the specified submission queue size.
    ///
    /// Larger rings allow more in-flight operations before the SQ fills.
    /// Powers of two are most efficient (the kernel rounds up regardless).
    ///
    /// # Errors
    ///
    /// Returns an error if `io_uring` is not available on this kernel.
    pub fn with_ring_size(sq_entries: u32) -> io::Result<Self> {
        let inner = RingThread::spawn(sq_entries)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

impl Clone for IoUringFs {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for IoUringFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoUringFs").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Fs for IoUringFs
// ---------------------------------------------------------------------------

impl Fs for IoUringFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let file = OpenOptions::new()
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .create_new(opts.create_new)
            .truncate(opts.truncate)
            .append(opts.append)
            .open(path)?;

        // When opened in append mode, io_uring writes use an explicit offset
        // so the kernel's O_APPEND semantics don't apply. Initialize the
        // cursor to EOF so that Write trait calls append correctly.
        // Note: concurrent appends from separate handles are NOT atomic
        // (unlike O_APPEND). This is acceptable — lsm-tree uses single-
        // writer-per-file for SSTs, journals, and blob files.
        let cursor = if opts.append {
            file.metadata()?.len()
        } else {
            0
        };

        Ok(Box::new(IoUringFile {
            file,
            cursor: AtomicU64::new(cursor),
            is_append: opts.append,
            ring: Arc::clone(&self.inner),
        }))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        // Delegate to std::fs — directory listing doesn't benefit from io_uring.
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
        let dir = File::open(path)?;
        if !dir.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sync_directory: path is not a directory",
            ));
        }
        self.inner.submit_fsync(dir.as_raw_fd(), false)?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }
}

// ---------------------------------------------------------------------------
// IoUringFile
// ---------------------------------------------------------------------------

/// File handle that routes I/O through an `io_uring` ring thread.
///
/// Wraps a [`std::fs::File`] for fd ownership and cold-path operations
/// (metadata, truncate, lock), while routing reads, writes, and fsyncs
/// through the shared `io_uring` ring.
pub struct IoUringFile {
    /// Underlying [`std::fs::File`] — owns the fd, used for metadata, `set_len`, lock.
    file: File,

    /// Tracked cursor position for [`Read`]/[`Write`]/[`Seek`] impls.
    /// Only accessed via `get_mut()` (those traits take `&mut self`) or
    /// not at all ([`FsFile::read_at`] uses an explicit offset).
    /// `AtomicU64` could be replaced with plain `u64` (which is already
    /// `Sync`), but is kept for consistency with the interior-mutability
    /// pattern and to allow potential future shared cursor access.
    cursor: AtomicU64,

    /// Whether the file was opened in append mode. When true, writes
    /// always go to current EOF regardless of cursor/seek position.
    is_append: bool,

    /// Shared reference to the ring thread.
    ring: Arc<RingThread>,
}

impl FsFile for IoUringFile {
    fn sync_all(&self) -> io::Result<()> {
        self.ring.submit_fsync(self.file.as_raw_fd(), false)?;
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.ring.submit_fsync(self.file.as_raw_fd(), true)?;
        Ok(())
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        let m = self.file.metadata()?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    // Fill-or-EOF: loop until buf is full or we hit EOF (0-byte read).
    // Retries on EINTR internally so callers can rely on short read = EOF.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let fd = self.file.as_raw_fd();
        let mut total_read: usize = 0;

        while total_read < buf.len() {
            let remaining = buf.get_mut(total_read..).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "read_at offset out of bounds")
            })?;
            let current_offset = offset.checked_add(total_read as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "read_at offset overflow")
            })?;

            let n = loop {
                match self.ring.submit_read(fd, remaining, current_offset) {
                    Ok(n) => break n,
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            };

            if n == 0 {
                break; // EOF
            }
            total_read += n as usize;
        }

        Ok(total_read)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        // Delegate to the platform-specific FsFile impl for std::fs::File.
        FsFile::lock_exclusive(&self.file)
    }
}

impl Read for IoUringFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let cursor = self.cursor.get_mut();
        let n = self.ring.submit_read(self.file.as_raw_fd(), buf, *cursor)?;
        *cursor += u64::from(n);
        Ok(n as usize)
    }
}

impl Write for IoUringFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let cursor = self.cursor.get_mut();
        // In append mode, write at current EOF to match O_APPEND semantics.
        // fstat per write is ~100ns — negligible for journal/SST append patterns.
        // Cursor-based tracking would break if seek() is called before write().
        if self.is_append {
            *cursor = self.file.metadata()?.len();
        }
        let n = self
            .ring
            .submit_write(self.file.as_raw_fd(), buf, *cursor)?;
        *cursor += u64::from(n);
        Ok(n as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        // No userspace buffer to flush — data goes directly to the kernel
        // via io_uring. Use sync_data()/sync_all() for durable persistence.
        Ok(())
    }
}

impl Seek for IoUringFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let cursor = self.cursor.get_mut();
        let new_pos = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(n) => if n >= 0 {
                cursor.checked_add(n.unsigned_abs())
            } else {
                cursor.checked_sub(n.unsigned_abs())
            }
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "seek position out of range")
            })?,
            SeekFrom::End(n) => {
                let len = self.file.metadata()?.len();
                if n >= 0 {
                    len.checked_add(n.unsigned_abs())
                } else {
                    len.checked_sub(n.unsigned_abs())
                }
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "seek position out of range")
                })?
            }
        };
        // Note: new_pos may exceed i64::MAX (kernel loff_t range). This
        // matches std::fs::File::seek which also returns u64. The kernel
        // will reject out-of-range offsets at the actual I/O syscall.
        *cursor = new_pos;
        Ok(new_pos)
    }
}

// ---------------------------------------------------------------------------
// Ring thread internals
// ---------------------------------------------------------------------------

/// Newtype wrapper for sending a `*mut u8` across threads.
///
/// # Safety
///
/// The caller must ensure the pointed-to memory remains valid until the
/// `io_uring` operation completes. This is upheld because the submitting
/// thread blocks on an `mpsc::Receiver` and cannot drop the buffer until
/// the CQE is received.
struct UnsafeSendMutPtr(*mut u8);

/// Newtype wrapper for sending a `*const u8` across threads.
///
/// See [`UnsafeSendMutPtr`] for safety contract.
struct UnsafeSendConstPtr(*const u8);

// SAFETY: see struct-level docs. The raw pointers are guaranteed valid
// for the duration of the io_uring op because the caller blocks until
// the CQE is received.
#[expect(unsafe_code, reason = "marking raw-pointer wrapper as Send")]
unsafe impl Send for UnsafeSendMutPtr {}

#[expect(unsafe_code, reason = "marking raw-pointer wrapper as Send")]
unsafe impl Send for UnsafeSendConstPtr {}

/// An I/O operation to submit to the ring.
enum OpKind {
    Read {
        fd: i32,
        buf: UnsafeSendMutPtr,
        len: u32,
        offset: u64,
    },
    Write {
        fd: i32,
        buf: UnsafeSendConstPtr,
        len: u32,
        offset: u64,
    },
    Fsync {
        fd: i32,
        datasync: bool,
    },
}

/// A submitted operation with its result channel.
struct Op {
    kind: OpKind,
    result_tx: mpsc::SyncSender<i32>,
}

/// Dedicated thread that owns the `io_uring` ring.
///
/// Operations are submitted via bounded `mpsc::SyncSender` (sized to match
/// the ring) and results are returned through per-operation channels.
struct RingThread {
    tx: Mutex<Option<mpsc::SyncSender<Op>>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RingThread {
    fn spawn(sq_entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(sq_entries)?;
        // Bound the submission channel to ring capacity — provides
        // natural backpressure when callers outpace the I/O thread.
        let (tx, rx) = mpsc::sync_channel(sq_entries as usize);

        // If event_loop panics after submitting SQEs, those SQEs still
        // reference caller buffers. catch_unwind + abort is used, and
        // pending is wrapped in ManuallyDrop inside event_loop so that
        // SyncSenders are NOT dropped during unwind — callers stay blocked
        // until abort kills the process.
        let handle = thread::Builder::new()
            .name("lsm-io-uring".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::event_loop(ring, rx);
                }));
                if result.is_err() {
                    log::error!("io_uring ring thread panicked; aborting to avoid UB");
                    std::process::abort();
                }
            })?;

        Ok(Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
        })
    }

    /// Main event loop for the I/O thread.
    ///
    /// 1. Block on `recv()` when idle (no in-flight ops).
    /// 2. Batch additional ops via `try_recv()`.
    /// 3. Submit to kernel and wait for at least one completion.
    /// 4. Dispatch CQE results to callers.
    // Coverage: error paths (EINTR, fatal ring failure, SQ overflow, channel
    // disconnect with pending ops) require kernel fault injection to exercise.
    // The happy path IS covered by all IoUringFs tests.
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "rx is moved into the spawned thread — must be owned"
    )]
    fn event_loop(mut ring: IoUring, rx: mpsc::Receiver<Op>) {
        // ManuallyDrop ensures that on panic, pending's SyncSenders are NOT
        // dropped during stack unwinding. This keeps callers blocked on their
        // result channels until catch_unwind + abort kills the process,
        // preventing them from dropping buffers that the kernel may still access.
        let mut pending =
            std::mem::ManuallyDrop::new(HashMap::<u64, mpsc::SyncSender<i32>>::default());
        let mut next_id: u64 = 0;

        loop {
            // Phase 1: collect operations.
            let first = if pending.is_empty() {
                match rx.recv() {
                    Ok(op) => Some(op),
                    Err(mpsc::RecvError) => break,
                }
            } else {
                match rx.try_recv() {
                    Ok(op) => Some(op),
                    Err(mpsc::TryRecvError::Empty) => None,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        if pending.is_empty() {
                            break;
                        }
                        None
                    }
                }
            };

            if let Some(op) = first {
                Self::enqueue(&mut ring, &mut pending, &mut next_id, op);

                // Batch: drain as many additional ops as available.
                while let Ok(op) = rx.try_recv() {
                    Self::enqueue(&mut ring, &mut pending, &mut next_id, op);
                }
            }

            if pending.is_empty() {
                continue;
            }

            // Phase 2: submit to kernel, retry on EINTR.
            // Errno constants are inlined to avoid a libc dependency
            // (consistent with StdFs which uses raw FFI for flock).
            loop {
                match ring.submit_and_wait(1) {
                    Ok(_) => break,
                    Err(ref e) if e.raw_os_error() == Some(4 /* EINTR */) => {}
                    Err(e) => {
                        // Fatal ring error. Previously submitted SQEs may
                        // still be in-flight referencing caller buffers.
                        // Draining `pending` would unblock callers and let
                        // them drop those buffers — UB if the kernel still
                        // touches them. Abort to avoid unsoundness.
                        log::error!(
                            "io_uring submit_and_wait failed: {e}; aborting process to avoid UB"
                        );
                        std::process::abort();
                    }
                }
            }

            // Phase 3: harvest completions.
            for cqe in ring.completion() {
                let id = cqe.user_data();
                if let Some(tx) = pending.remove(&id) {
                    let _ = tx.send(cqe.result());
                }
            }
        }

        // Normal exit (channel closed) — no in-flight SQEs remain, safe to
        // drop pending's SyncSenders. Without this, ManuallyDrop would leak.
        #[expect(unsafe_code, reason = "ManuallyDrop cleanup on normal exit path")]
        // SAFETY: we only reach here after the loop breaks (channel disconnected),
        // meaning no more SQEs can be submitted and all completions are harvested.
        unsafe {
            std::mem::ManuallyDrop::drop(&mut pending);
        }
    }

    /// Builds an SQE from `op` and pushes it onto the submission queue.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn enqueue(
        ring: &mut IoUring,
        pending: &mut HashMap<u64, mpsc::SyncSender<i32>>,
        next_id: &mut u64,
        op: Op,
    ) {
        let id = *next_id;
        *next_id = next_id.wrapping_add(1);

        let sqe = match op.kind {
            OpKind::Read {
                fd,
                buf,
                len,
                offset,
            } => opcode::Read::new(types::Fd(fd), buf.0, len)
                .offset(offset)
                .build()
                .user_data(id),

            OpKind::Write {
                fd,
                buf,
                len,
                offset,
            } => opcode::Write::new(types::Fd(fd), buf.0, len)
                .offset(offset)
                .build()
                .user_data(id),

            OpKind::Fsync { fd, datasync } => {
                let mut entry = opcode::Fsync::new(types::Fd(fd));
                if datasync {
                    entry = entry.flags(types::FsyncFlags::DATASYNC);
                }
                entry.build().user_data(id)
            }
        };

        // SAFETY: SQE references memory that the calling thread keeps alive
        // (blocked on the result channel — see UnsafeSend safety contract).
        #[expect(unsafe_code, reason = "io_uring SQE push")]
        unsafe {
            while ring.submission().push(&sqe).is_err() {
                // SQ full — wait for at least one completion to free a slot.
                // Since the Fs API is synchronous, callers are already blocking;
                // backpressure here is natural, not an error.
                loop {
                    match ring.submit_and_wait(1) {
                        Ok(_) => break,
                        Err(ref e) if e.raw_os_error() == Some(4 /* EINTR */) => {}
                        Err(e) => {
                            // Fatal ring error — same as Phase 2 handler.
                            log::error!(
                                "io_uring submit_and_wait failed in SQ retry: {e}; aborting"
                            );
                            std::process::abort();
                        }
                    }
                }
                for cqe in ring.completion() {
                    let cid = cqe.user_data();
                    if let Some(tx) = pending.remove(&cid) {
                        let _ = tx.send(cqe.result());
                    }
                }
            }
        }

        pending.insert(id, op.result_tx);
    }

    // -- Submission helpers --------------------------------------------------

    /// Submits a pread to the ring and blocks until completion.
    fn submit_read(&self, fd: i32, buf: &mut [u8], offset: u64) -> io::Result<u32> {
        // SQE length is u32, but CQE result is i32 — cap at i32::MAX
        // to ensure the byte count is always representable. In practice
        // LSM block reads are 4-64 KB, so the cap is never reached.
        let len: u32 = i32::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX"))?
            .unsigned_abs();
        let (tx, rx) = mpsc::sync_channel(1);
        let op = Op {
            kind: OpKind::Read {
                fd,
                buf: UnsafeSendMutPtr(buf.as_mut_ptr()),
                len,
                offset,
            },
            result_tx: tx,
        };
        self.send_and_wait(op, &rx)
    }

    /// Submits a pwrite to the ring and blocks until completion.
    fn submit_write(&self, fd: i32, buf: &[u8], offset: u64) -> io::Result<u32> {
        // SQE length is u32, but CQE result is i32 — cap at i32::MAX
        // to ensure the byte count is always representable. In practice
        // LSM block writes are 4-64 KB, so the cap is never reached.
        let len: u32 = i32::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX"))?
            .unsigned_abs();
        let (tx, rx) = mpsc::sync_channel(1);
        let op = Op {
            kind: OpKind::Write {
                fd,
                buf: UnsafeSendConstPtr(buf.as_ptr()),
                len,
                offset,
            },
            result_tx: tx,
        };
        self.send_and_wait(op, &rx)
    }

    /// Submits an fsync or fdatasync and blocks until completion.
    fn submit_fsync(&self, fd: i32, datasync: bool) -> io::Result<u32> {
        let (tx, rx) = mpsc::sync_channel(1);
        let op = Op {
            kind: OpKind::Fsync { fd, datasync },
            result_tx: tx,
        };
        self.send_and_wait(op, &rx)
    }

    /// Sends an operation to the ring thread and blocks on the result.
    ///
    /// Returns the non-negative CQE result as `u32`. Negative results
    /// (kernel errors) are converted to [`io::Error`].
    fn send_and_wait(&self, op: Op, rx: &mpsc::Receiver<i32>) -> io::Result<u32> {
        // Mutex guards Option<Sender> for clean shutdown (Drop sets to None).
        // Lock is held only for send() duration (~ns) — negligible vs I/O
        // latency (~µs). A lock-free channel would eliminate this but adds
        // an external dependency for no measurable gain.
        self.tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread shut down"))?
            .send(op)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;

        let result = rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;

        if result >= 0 {
            // CQE result is non-negative — `as u32` is lossless.
            #[expect(clippy::cast_sign_loss, reason = "guarded by result >= 0 check above")]
            Ok(result as u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        }
    }
}

impl Drop for RingThread {
    // Coverage: poison recovery branches require panic injection to reach.
    // The normal (non-poison) path is exercised by every test that drops IoUringFs.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn drop(&mut self) {
        // Drop the sender to close the channel — this unblocks the event
        // loop's recv() and lets it drain remaining in-flight ops.
        // Handle poison gracefully: during shutdown we only need to clear
        // the sender and join the thread, regardless of prior panics.
        let tx = match self.tx.get_mut() {
            Ok(tx) => tx,
            Err(poisoned) => poisoned.into_inner(),
        };
        *tx = None;

        let handle_slot = match self.handle.get_mut() {
            Ok(h) => h,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(handle) = handle_slot.take() {
            if handle.join().is_err() {
                log::error!("io_uring ring thread panicked during shutdown");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Arc;
    // Shadows #[test] to enable log capture in test output.
    use test_log::test;

    /// Returns an `IoUringFs`, skipping only if the kernel lacks io_uring.
    /// Constructor bugs (e.g. broken `RingThread::spawn`) will panic the
    /// test instead of silently skipping.
    fn try_io_uring() -> Option<IoUringFs> {
        if !is_io_uring_available() {
            eprintln!("skipping: io_uring not supported by kernel");
            return None;
        }
        // Kernel supports io_uring — constructor failures are real bugs.
        Some(IoUringFs::new().expect("io_uring available but IoUringFs::new() failed"))
    }

    #[test]
    fn probe_availability() {
        // Just exercises the probe — result depends on the kernel.
        let available = is_io_uring_available();
        eprintln!("io_uring available: {available}");
    }

    #[test]
    fn create_read_write() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        file.sync_all()?;
        drop(file);

        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(&path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello world");

        Ok(())
    }

    #[test]
    fn read_at_pread_semantics() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("pread.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        file.sync_data()?;

        let mut buf = [0u8; 5];
        let n = file.read_at(&mut buf, 6)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        let n = file.read_at(&mut buf, 0)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        Ok(())
    }

    #[test]
    fn directory_operations() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let nested = dir.path().join("a").join("b").join("c");
        fs.create_dir_all(&nested)?;
        assert!(fs.exists(&nested)?);

        let file_path = nested.join("data.bin");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&file_path, &opts)?;
        file.write_all(b"data")?;
        drop(file);

        let entries: Vec<_> = fs.read_dir(&nested)?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "data.bin");

        let meta = fs.metadata(&file_path)?;
        assert!(meta.is_file);
        assert_eq!(meta.len, 4);

        fs.remove_file(&file_path)?;
        assert!(!fs.exists(&file_path)?);

        let top = dir.path().join("a");
        fs.remove_dir_all(&top)?;
        assert!(!fs.exists(&top)?);

        Ok(())
    }

    #[test]
    fn rename() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

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
    fn sync_directory() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        fs.sync_directory(dir.path())?;
        Ok(())
    }

    #[test]
    fn file_metadata() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

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
    fn file_set_len() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

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
    fn lock_exclusive() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("lockfile");
        let opts = FsOpenOptions::new().write(true).create(true);
        let file = fs.open(&path, &opts)?;
        file.lock_exclusive()?;

        Ok(())
    }

    #[test]
    fn truncate_and_append() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("trunc.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        drop(file);

        let opts = FsOpenOptions::new().write(true).truncate(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hi")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 2);

        let opts = FsOpenOptions::new().write(true).append(true);
        let mut file = fs.open(&path, &opts)?;
        // Seek to start, then write — append mode must ignore seek and
        // write at EOF regardless of cursor position.
        file.seek(SeekFrom::Start(0))?;
        file.write_all(b"!")?;
        drop(file);

        // Verify append went to EOF (len=3), not to start (which would
        // overwrite "hi" and keep len=2).
        let mut file = fs.open(&path, &FsOpenOptions::new().read(true))?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hi!");
        assert_eq!(fs.metadata(&path)?.len, 3);

        Ok(())
    }

    #[test]
    fn seek_operations() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("seek.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;

        // Seek to start and re-read
        file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; 5];
        file.read_exact(&mut buf)?;
        assert_eq!(&buf, b"hello");

        // Seek from current (+1 to skip space)
        file.seek(SeekFrom::Current(1))?;
        file.read_exact(&mut buf)?;
        assert_eq!(&buf, b"world");

        // Seek from end
        let pos = file.seek(SeekFrom::End(-5))?;
        assert_eq!(pos, 6);

        Ok(())
    }

    #[test]
    fn concurrent_read_at() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("concurrent.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        // Write 1000 bytes: each byte = (offset % 256)
        #[expect(clippy::cast_possible_truncation, reason = "% 256 guarantees 0..=255")]
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        file.write_all(&data)?;
        file.sync_all()?;

        let file = Arc::new(file);
        let mut handles = Vec::new();

        for chunk_start in (0..1000).step_by(100) {
            let file = Arc::clone(&file);
            handles.push(thread::spawn(move || -> io::Result<()> {
                let mut buf = [0u8; 100];
                let n = file.read_at(&mut buf, chunk_start as u64)?;
                assert_eq!(n, 100);
                for (i, &byte) in buf.iter().enumerate() {
                    #[expect(clippy::cast_possible_truncation, reason = "% 256 guarantees 0..=255")]
                    let expected = ((chunk_start + i) % 256) as u8;
                    assert_eq!(byte, expected);
                }
                Ok(())
            }));
        }

        for h in handles {
            match h.join() {
                Ok(result) => result?,
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "thread panicked")),
            }
        }

        Ok(())
    }

    #[test]
    fn metadata_directory() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let meta = fs.metadata(dir.path())?;
        assert!(meta.is_dir);
        assert!(!meta.is_file);

        Ok(())
    }

    #[test]
    fn object_safety() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let fs: Arc<dyn Fs> = Arc::new(fs);
        let dir = tempfile::tempdir()?;
        let bogus = dir.path().join("nonexistent");
        assert!(!fs.exists(&bogus)?);
        Ok(())
    }

    #[test]
    fn empty_buffer_returns_zero() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("empty_buf.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"data")?;

        // read_at with empty buffer
        let n = file.read_at(&mut [], 0)?;
        assert_eq!(n, 0);

        // Read::read with empty buffer
        let n = file.read(&mut [])?;
        assert_eq!(n, 0);

        // Write::write with empty buffer
        let n = file.write(&[])?;
        assert_eq!(n, 0);

        // flush is a no-op
        file.flush()?;

        Ok(())
    }

    #[test]
    fn sync_directory_rejects_file() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("not_a_dir.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        fs.open(&path, &opts)?;

        match fs.sync_directory(&path) {
            Ok(()) => panic!("sync_directory on a file should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
        }

        Ok(())
    }

    #[test]
    fn seek_overflow_returns_error() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("seek_overflow.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"data")?;

        // Seek to near u64::MAX, then seek forward — should overflow.
        file.seek(SeekFrom::Start(u64::MAX - 1))?;
        match file.seek(SeekFrom::Current(2)) {
            Ok(_) => panic!("seek past u64::MAX should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
        }

        // SeekFrom::Current negative past zero — should underflow.
        file.seek(SeekFrom::Start(0))?;
        match file.seek(SeekFrom::Current(-1)) {
            Ok(_) => panic!("seek before zero should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
        }

        // SeekFrom::End negative past zero — should underflow.
        match file.seek(SeekFrom::End(-100)) {
            Ok(_) => panic!("seek before zero should fail"),
            Err(err) => assert_eq!(err.kind(), io::ErrorKind::InvalidInput),
        }

        Ok(())
    }

    #[test]
    fn debug_impl() {
        let Some(fs) = try_io_uring() else {
            return;
        };
        let debug = format!("{fs:?}");
        assert!(debug.contains("IoUringFs"));
    }

    #[test]
    fn with_ring_size() -> io::Result<()> {
        if !is_io_uring_available() {
            eprintln!("skipping: io_uring not supported by kernel");
            return Ok(());
        }
        let fs = IoUringFs::with_ring_size(64)
            .expect("io_uring available but with_ring_size(64) failed");
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("ring64.bin");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"ok")?;
        file.sync_all()?;
        assert_eq!(fs.metadata(&path)?.len, 2);
        Ok(())
    }

    #[test]
    fn seek_negative_from_current() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("seek_neg.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"abcdefghij")?;

        // Seek to position 8, then back 3
        file.seek(SeekFrom::Start(8))?;
        let pos = file.seek(SeekFrom::Current(-3))?;
        assert_eq!(pos, 5);

        let mut buf = [0u8; 5];
        file.read_exact(&mut buf)?;
        assert_eq!(&buf, b"fghij");

        Ok(())
    }

    #[test]
    fn clone_shares_ring() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let fs2 = fs.clone();
        let dir = tempfile::tempdir()?;

        // Both clones should work with the same ring thread.
        let p1 = dir.path().join("a.txt");
        let p2 = dir.path().join("b.txt");
        let opts = FsOpenOptions::new().write(true).create(true);

        let mut f1 = fs.open(&p1, &opts)?;
        let mut f2 = fs2.open(&p2, &opts)?;
        f1.write_all(b"one")?;
        f2.write_all(b"two")?;
        f1.sync_all()?;
        f2.sync_all()?;

        assert_eq!(fs.metadata(&p1)?.len, 3);
        assert_eq!(fs2.metadata(&p2)?.len, 3);

        Ok(())
    }
}
