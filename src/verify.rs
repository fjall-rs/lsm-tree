// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! checksum verification for tables and blob files.
//!
//! This module provides comprehensive file integrity verification with:
//! - **Streaming I/O**: Memory-efficient buffered reading
//! - **Parallel verification**: Multi-threaded file verification
//! - **Progress reporting**: Real-time verification status callbacks
//! - **Rate limiting**: Control I/O bandwidth consumption
//! - **Cancellation**: Graceful abort of long-running verifications
//! - **Per-level filtering**: Verify specific LSM-tree levels

use crate::{version::Version, vlog::BlobFileId, Checksum, TableId};
use std::{
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use xxhash_rust::xxh3::Xxh3;

/// Default buffer size for streaming file reads (1 MiB).
const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024;

/// Default number of verification threads.
const DEFAULT_PARALLELISM: usize = 4;

/// Identifies a corrupted file.
#[derive(Debug, Clone)]
pub struct CorruptedFile {
    /// Path to the corrupted file.
    pub path: PathBuf,

    /// Expected checksum stored in the manifest.
    pub expected: Checksum,

    /// Actual checksum computed from the file.
    pub actual: Checksum,

    /// File size in bytes.
    pub file_size: u64,
}

/// An I/O error that occurred during verification.
#[derive(Debug, Clone)]
pub struct VerificationIoError {
    /// Path to the file that caused the error.
    pub path: PathBuf,

    /// Error message.
    pub message: String,
}

/// File type being verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Table (SST) file.
    Table,
    /// Blob file (value log).
    BlobFile,
}

/// Progress information for verification callbacks.
#[derive(Debug, Clone)]
pub struct VerificationProgress {
    /// Number of files verified so far.
    pub files_verified: usize,

    /// Total number of files to verify.
    pub files_total: usize,

    /// Bytes verified so far.
    pub bytes_verified: u64,

    /// Total bytes to verify.
    pub bytes_total: u64,

    /// Current file being verified (if any).
    pub current_file: Option<PathBuf>,

    /// Type of current file being verified.
    pub current_file_type: Option<FileType>,

    /// Number of corrupted files found so far.
    pub corrupted_count: usize,

    /// Number of I/O errors encountered so far.
    pub error_count: usize,

    /// Elapsed time since verification started.
    pub elapsed: Duration,

    /// Estimated time remaining (if calculable).
    pub estimated_remaining: Option<Duration>,

    /// Current verification rate in bytes per second.
    pub bytes_per_second: f64,
}

/// Configuration options for checksum verification.
#[derive(Debug, Clone)]
pub struct VerificationOptions {
    /// Number of parallel verification threads.
    ///
    /// Default: 4
    pub parallelism: usize,

    /// Buffer size for streaming file reads in bytes.
    ///
    /// Larger buffers reduce syscall overhead but use more memory.
    /// Default: 1 MiB
    pub buffer_size: usize,

    /// Rate limit for I/O in bytes per second.
    ///
    /// Set to 0 for unlimited (default).
    pub rate_limit_bytes_per_sec: u64,

    /// Whether to verify table (SST) files.
    ///
    /// Default: true
    pub verify_tables: bool,

    /// Whether to verify blob files (value log).
    ///
    /// Default: true
    pub verify_blob_files: bool,

    /// Specific levels to verify (empty = all levels).
    ///
    /// Default: empty (verify all levels)
    pub levels: Vec<usize>,

    /// Whether to stop on first corruption found.
    ///
    /// Default: false (continue and report all corruptions)
    pub stop_on_first_corruption: bool,

    /// Whether to stop on first I/O error.
    ///
    /// Default: false (continue and report all errors)
    pub stop_on_first_error: bool,
}

impl Default for VerificationOptions {
    fn default() -> Self {
        Self {
            parallelism: DEFAULT_PARALLELISM,
            buffer_size: DEFAULT_BUFFER_SIZE,
            rate_limit_bytes_per_sec: 0,
            verify_tables: true,
            verify_blob_files: true,
            levels: Vec::new(),
            stop_on_first_corruption: false,
            stop_on_first_error: false,
        }
    }
}

impl VerificationOptions {
    /// Creates new verification options with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of parallel verification threads.
    #[must_use]
    pub fn parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// Sets the buffer size for streaming reads.
    #[must_use]
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size.max(4096);
        self
    }

    /// Sets the I/O rate limit in bytes per second.
    ///
    /// Set to 0 for unlimited.
    #[must_use]
    pub fn rate_limit(mut self, bytes_per_sec: u64) -> Self {
        self.rate_limit_bytes_per_sec = bytes_per_sec;
        self
    }

    /// Sets whether to verify table files.
    #[must_use]
    pub fn verify_tables(mut self, verify: bool) -> Self {
        self.verify_tables = verify;
        self
    }

    /// Sets whether to verify blob files.
    #[must_use]
    pub fn verify_blob_files(mut self, verify: bool) -> Self {
        self.verify_blob_files = verify;
        self
    }

    /// Sets specific levels to verify.
    ///
    /// Pass an empty slice to verify all levels.
    #[must_use]
    pub fn levels(mut self, levels: &[usize]) -> Self {
        self.levels = levels.to_vec();
        self
    }

    /// Sets whether to stop on first corruption.
    #[must_use]
    pub fn stop_on_first_corruption(mut self, stop: bool) -> Self {
        self.stop_on_first_corruption = stop;
        self
    }

    /// Sets whether to stop on first I/O error.
    #[must_use]
    pub fn stop_on_first_error(mut self, stop: bool) -> Self {
        self.stop_on_first_error = stop;
        self
    }
}

/// Result of checksum verification.
#[derive(Debug, Default)]
pub struct VerificationResult {
    /// Number of table files verified.
    pub tables_verified: usize,

    /// Number of blob files verified.
    pub blob_files_verified: usize,

    /// Total bytes verified.
    pub bytes_verified: u64,

    /// List of corrupted table files.
    pub corrupted_tables: Vec<(TableId, CorruptedFile)>,

    /// List of corrupted blob files.
    pub corrupted_blob_files: Vec<(BlobFileId, CorruptedFile)>,

    /// I/O errors encountered during verification.
    pub io_errors: Vec<VerificationIoError>,

    /// Total verification duration.
    pub duration: Duration,

    /// Whether verification was cancelled.
    pub was_cancelled: bool,
}

impl VerificationResult {
    /// Returns `true` if no corruption was detected and no errors occurred.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.corrupted_tables.is_empty()
            && self.corrupted_blob_files.is_empty()
            && self.io_errors.is_empty()
    }

    /// Returns `true` if no corruption was detected (ignoring I/O errors).
    #[must_use]
    pub fn no_corruption(&self) -> bool {
        self.corrupted_tables.is_empty() && self.corrupted_blob_files.is_empty()
    }

    /// Returns the total number of corrupted files.
    #[must_use]
    pub fn corrupted_count(&self) -> usize {
        self.corrupted_tables.len() + self.corrupted_blob_files.len()
    }

    /// Returns the total number of files verified.
    #[must_use]
    pub fn files_verified(&self) -> usize {
        self.tables_verified + self.blob_files_verified
    }

    /// Returns the verification throughput in bytes per second.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "Throughput calculation is approximate"
    )]
    pub fn throughput_bytes_per_sec(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.bytes_verified as f64 / secs
        } else {
            0.0
        }
    }
}

/// A handle to cancel an ongoing verification.
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Creates a new cancellation token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signals cancellation to the verification process.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns `true` if cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal file verification task.
#[derive(Clone)]
struct VerificationTask {
    path: PathBuf,
    expected_checksum: Checksum,
    file_size: u64,
    file_type: FileType,
    id: u64, // TableId or BlobFileId
}

/// Internal verification result for a single file.
#[derive(Debug)]
enum TaskResult {
    Ok {
        file_type: FileType,
        bytes: u64,
    },
    Corrupted {
        file_type: FileType,
        id: u64,
        corrupted: CorruptedFile,
    },
    IoError {
        path: PathBuf,
        message: String,
    },
}

/// Rate limiter for controlling I/O bandwidth.
struct RateLimiter {
    bytes_per_sec: u64,
    bytes_this_window: AtomicU64,
    window_start: Mutex<Instant>,
}

impl RateLimiter {
    fn new(bytes_per_sec: u64) -> Self {
        Self {
            bytes_per_sec,
            bytes_this_window: AtomicU64::new(0),
            window_start: Mutex::new(Instant::now()),
        }
    }

    fn acquire(&self, bytes: u64) {
        if self.bytes_per_sec == 0 {
            return; // No rate limiting
        }

        loop {
            let current_bytes = self.bytes_this_window.fetch_add(bytes, Ordering::AcqRel);
            let total_bytes = current_bytes + bytes;

            if total_bytes <= self.bytes_per_sec {
                return; // Within budget
            }

            // Check if we need to wait for the next window
            let mut window_start = self.window_start.lock().expect("lock poisoned");
            let elapsed = window_start.elapsed();

            if elapsed >= Duration::from_secs(1) {
                // Reset window
                *window_start = Instant::now();
                self.bytes_this_window.store(0, Ordering::Release);
            } else {
                // Wait for next window
                let wait_time = Duration::from_secs(1) - elapsed;
                drop(window_start);
                thread::sleep(wait_time);
            }
        }
    }
}

/// Computes the checksum of a file using streaming I/O.
fn compute_file_checksum_streaming(
    path: &Path,
    buffer_size: usize,
    rate_limiter: Option<&RateLimiter>,
    cancel_token: &CancellationToken,
) -> std::io::Result<Option<Checksum>> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(buffer_size, file);
    let mut hasher = Xxh3::new();
    let mut buffer = vec![0u8; buffer_size];

    loop {
        if cancel_token.is_cancelled() {
            return Ok(None); // Cancelled
        }

        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break; // EOF
        }

        if let Some(limiter) = rate_limiter {
            limiter.acquire(bytes_read as u64);
        }

        if let Some(slice) = buffer.get(..bytes_read) {
            hasher.update(slice);
        }
    }

    Ok(Some(Checksum::from_raw(hasher.digest128())))
}

/// Verifies a single file and returns the result.
fn verify_single_file(
    task: &VerificationTask,
    buffer_size: usize,
    rate_limiter: Option<&RateLimiter>,
    cancel_token: &CancellationToken,
) -> TaskResult {
    match compute_file_checksum_streaming(&task.path, buffer_size, rate_limiter, cancel_token) {
        Ok(Some(actual)) => {
            if actual == task.expected_checksum {
                TaskResult::Ok {
                    file_type: task.file_type,
                    bytes: task.file_size,
                }
            } else {
                TaskResult::Corrupted {
                    file_type: task.file_type,
                    id: task.id,
                    corrupted: CorruptedFile {
                        path: task.path.clone(),
                        expected: task.expected_checksum,
                        actual,
                        file_size: task.file_size,
                    },
                }
            }
        }
        Ok(None) => {
            // Cancelled - treat as OK to avoid false positives
            TaskResult::Ok {
                file_type: task.file_type,
                bytes: 0,
            }
        }
        Err(e) => TaskResult::IoError {
            path: task.path.clone(),
            message: e.to_string(),
        },
    }
}

/// Collects verification tasks from a version.
fn collect_verification_tasks(
    version: &Version,
    options: &VerificationOptions,
) -> Vec<VerificationTask> {
    let mut tasks = Vec::new();

    // Collect table tasks
    if options.verify_tables {
        for (level_idx, level) in version.iter_levels().enumerate() {
            // Skip levels not in the filter (if filter is non-empty)
            if !options.levels.is_empty() && !options.levels.contains(&level_idx) {
                continue;
            }

            for run in level.iter() {
                for table in run.iter() {
                    tasks.push(VerificationTask {
                        path: (*table.path).clone(),
                        expected_checksum: table.checksum(),
                        file_size: table.file_size(),
                        file_type: FileType::Table,
                        id: table.id(),
                    });
                }
            }
        }
    }

    // Collect blob file tasks
    if options.verify_blob_files {
        for blob_file in version.blob_files.iter() {
            let file_size = std::fs::metadata(blob_file.path())
                .map(|m| m.len())
                .unwrap_or(0);

            tasks.push(VerificationTask {
                path: blob_file.path().to_path_buf(),
                expected_checksum: blob_file.checksum(),
                file_size,
                file_type: FileType::BlobFile,
                id: blob_file.id(),
            });
        }
    }

    tasks
}

/// Verifies checksums with full configuration options.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "Progress tracking counters are safe within realistic bounds"
)]
pub(crate) fn verify_version_with_options<F>(
    version: &Version,
    options: &VerificationOptions,
    cancel_token: &CancellationToken,
    progress_callback: Option<F>,
) -> VerificationResult
where
    F: Fn(VerificationProgress) + Send + Sync + 'static,
{
    let start_time = Instant::now();
    let tasks = collect_verification_tasks(version, options);

    if tasks.is_empty() {
        return VerificationResult {
            duration: start_time.elapsed(),
            ..Default::default()
        };
    }

    let total_bytes: u64 = tasks.iter().map(|t| t.file_size).sum();
    let total_files = tasks.len();

    // Shared state for progress tracking
    let bytes_verified = Arc::new(AtomicU64::new(0));
    let files_verified = Arc::new(AtomicU64::new(0));
    let corrupted_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    // Results collection
    let results: Arc<Mutex<Vec<TaskResult>>> =
        Arc::new(Mutex::new(Vec::with_capacity(tasks.len())));

    // Rate limiter
    let rate_limiter = if options.rate_limit_bytes_per_sec > 0 {
        Some(Arc::new(RateLimiter::new(options.rate_limit_bytes_per_sec)))
    } else {
        None
    };

    // Early stop flags
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Progress callback wrapper
    let progress_callback = progress_callback.map(Arc::new);

    // Determine actual parallelism
    let parallelism = options.parallelism.min(tasks.len()).max(1);

    // Partition tasks among threads
    let chunk_size = tasks.len().div_ceil(parallelism);
    let task_chunks: Vec<Vec<VerificationTask>> = tasks
        .chunks(chunk_size)
        .map(<[VerificationTask]>::to_vec)
        .collect();

    // Spawn worker threads
    let handles: Vec<_> = task_chunks
        .into_iter()
        .map(|chunk| {
            let results = Arc::clone(&results);
            let bytes_verified = Arc::clone(&bytes_verified);
            let files_verified = Arc::clone(&files_verified);
            let corrupted_count = Arc::clone(&corrupted_count);
            let error_count = Arc::clone(&error_count);
            let rate_limiter = rate_limiter.clone();
            let cancel_token = cancel_token.clone();
            let stop_flag = Arc::clone(&stop_flag);
            let progress_callback = progress_callback.clone();
            let buffer_size = options.buffer_size;
            let stop_on_first_corruption = options.stop_on_first_corruption;
            let stop_on_first_error = options.stop_on_first_error;

            thread::spawn(move || {
                for task in chunk {
                    // Check for cancellation or early stop
                    if cancel_token.is_cancelled() || stop_flag.load(Ordering::Acquire) {
                        break;
                    }

                    let result = verify_single_file(
                        &task,
                        buffer_size,
                        rate_limiter.as_deref(),
                        &cancel_token,
                    );

                    // Update counters
                    match &result {
                        TaskResult::Ok { bytes, .. } => {
                            bytes_verified.fetch_add(*bytes, Ordering::AcqRel);
                        }
                        TaskResult::Corrupted { corrupted, .. } => {
                            bytes_verified.fetch_add(corrupted.file_size, Ordering::AcqRel);
                            corrupted_count.fetch_add(1, Ordering::AcqRel);
                            if stop_on_first_corruption {
                                stop_flag.store(true, Ordering::Release);
                            }
                        }
                        TaskResult::IoError { .. } => {
                            error_count.fetch_add(1, Ordering::AcqRel);
                            if stop_on_first_error {
                                stop_flag.store(true, Ordering::Release);
                            }
                        }
                    }
                    files_verified.fetch_add(1, Ordering::AcqRel);

                    // Store result
                    results.lock().expect("lock poisoned").push(result);

                    // Report progress
                    if let Some(ref callback) = progress_callback {
                        let verified_bytes = bytes_verified.load(Ordering::Acquire);
                        let verified_files = files_verified.load(Ordering::Acquire) as usize;
                        let elapsed = start_time.elapsed();
                        let bytes_per_sec = if elapsed.as_secs_f64() > 0.0 {
                            verified_bytes as f64 / elapsed.as_secs_f64()
                        } else {
                            0.0
                        };

                        let estimated_remaining = if bytes_per_sec > 0.0 && verified_bytes > 0 {
                            let remaining_bytes = total_bytes.saturating_sub(verified_bytes);
                            Some(Duration::from_secs_f64(
                                remaining_bytes as f64 / bytes_per_sec,
                            ))
                        } else {
                            None
                        };

                        callback(VerificationProgress {
                            files_verified: verified_files,
                            files_total: total_files,
                            bytes_verified: verified_bytes,
                            bytes_total: total_bytes,
                            current_file: Some(task.path.clone()),
                            current_file_type: Some(task.file_type),
                            corrupted_count: corrupted_count.load(Ordering::Acquire) as usize,
                            error_count: error_count.load(Ordering::Acquire) as usize,
                            elapsed,
                            estimated_remaining,
                            bytes_per_second: bytes_per_sec,
                        });
                    }
                }
            })
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        let _ = handle.join();
    }

    // Collect final results
    let task_results = Arc::try_unwrap(results)
        .expect("all threads should have finished")
        .into_inner()
        .expect("lock poisoned");

    let mut final_result = VerificationResult {
        duration: start_time.elapsed(),
        bytes_verified: bytes_verified.load(Ordering::Acquire),
        was_cancelled: cancel_token.is_cancelled(),
        ..Default::default()
    };

    for result in task_results {
        match result {
            TaskResult::Ok { file_type, .. } => match file_type {
                FileType::Table => final_result.tables_verified += 1,
                FileType::BlobFile => final_result.blob_files_verified += 1,
            },
            TaskResult::Corrupted {
                file_type,
                id,
                corrupted,
            } => match file_type {
                FileType::Table => {
                    final_result.tables_verified += 1;
                    final_result.corrupted_tables.push((id, corrupted));
                }
                FileType::BlobFile => {
                    final_result.blob_files_verified += 1;
                    final_result.corrupted_blob_files.push((id, corrupted));
                }
            },
            TaskResult::IoError { path, message } => {
                final_result
                    .io_errors
                    .push(VerificationIoError { path, message });
            }
        }
    }

    final_result
}
