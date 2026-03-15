// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{checksum::Checksum, table::TableId};
use std::path::PathBuf;

/// Describes a single integrity error found during verification.
#[derive(Debug)]
#[non_exhaustive]
pub enum IntegrityError {
    /// Full-file checksum mismatch for an SST table.
    SstFileCorrupted {
        /// Table ID
        table_id: TableId,
        /// Path to the corrupted file
        path: PathBuf,
        /// Checksum stored in the manifest
        expected: Checksum,
        /// Checksum computed from disk
        got: Checksum,
    },

    /// Full-file checksum mismatch for a blob file.
    BlobFileCorrupted {
        /// Blob file ID
        blob_file_id: u64,
        /// Path to the corrupted file
        path: PathBuf,
        /// Checksum stored in the manifest
        expected: Checksum,
        /// Checksum computed from disk
        got: Checksum,
    },

    /// I/O error while reading a file during verification.
    IoError {
        /// Path to the file that could not be read
        path: PathBuf,
        /// The underlying I/O error
        error: std::io::Error,
    },
}

impl std::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SstFileCorrupted {
                table_id,
                path,
                expected,
                got,
            } => write!(
                f,
                "SST table {table_id} corrupted at {}: expected {expected}, got {got}",
                path.display()
            ),
            Self::BlobFileCorrupted {
                blob_file_id,
                path,
                expected,
                got,
            } => write!(
                f,
                "blob file {blob_file_id} corrupted at {}: expected {expected}, got {got}",
                path.display()
            ),
            Self::IoError { path, error } => {
                write!(f, "I/O error reading {}: {}", path.display(), error)
            }
        }
    }
}

impl std::error::Error for IntegrityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IoError { error, .. } => Some(error),
            _ => None,
        }
    }
}

/// Result of an integrity verification scan.
///
/// The `sst_files_checked` and `blob_files_checked` counters reflect
/// the number of files *attempted* — including those that produced I/O
/// errors. This lets callers reconcile the total against the manifest
/// even when some files were unreadable.
#[derive(Debug)]
#[non_exhaustive]
pub struct IntegrityReport {
    /// Number of SST table files checked (includes I/O errors).
    pub sst_files_checked: usize,

    /// Number of blob files checked (includes I/O errors).
    pub blob_files_checked: usize,

    /// Integrity errors found during verification.
    pub errors: Vec<IntegrityError>,
}

impl IntegrityReport {
    /// Returns `true` if no errors were found.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Total number of files checked (SST + blob).
    #[must_use]
    pub fn files_checked(&self) -> usize {
        self.sst_files_checked + self.blob_files_checked
    }
}

/// Computes a streaming XXH3 128-bit checksum for a file without loading it entirely into memory.
fn stream_checksum(path: &std::path::Path) -> std::io::Result<Checksum> {
    use std::io::Read;

    let mut reader = std::io::BufReader::new(std::fs::File::open(path)?);
    let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(Checksum::from_raw(hasher.digest128()))
}

/// Verifies full-file checksums for all SST and blob files in the given tree.
///
/// Each file's content is read from disk and hashed with XXHash-3 128-bit,
/// then compared against the checksum stored in the version manifest.
///
/// This detects silent bit-rot, partial writes, and other on-disk corruption.
///
/// Per-file errors (e.g., unreadable files, checksum mismatches) are collected
/// into [`IntegrityReport::errors`] — the scan always runs to completion.
#[must_use]
pub fn verify_integrity(tree: &impl crate::AbstractTree) -> IntegrityReport {
    let version = tree.current_version();

    let mut report = IntegrityReport {
        sst_files_checked: 0,
        blob_files_checked: 0,
        errors: Vec::new(),
    };

    // Verify all SST table files
    for table in version.iter_tables() {
        let path = &*table.path;
        let expected = table.checksum();

        match stream_checksum(path) {
            Ok(got) if got != expected => {
                report.errors.push(IntegrityError::SstFileCorrupted {
                    table_id: table.id(),
                    path: path.to_path_buf(),
                    expected,
                    got,
                });
            }
            Ok(_) => {}
            Err(e) => {
                report.errors.push(IntegrityError::IoError {
                    path: path.to_path_buf(),
                    error: e,
                });
            }
        }

        report.sst_files_checked += 1;
    }

    // Verify all blob files
    for blob_file in version.blob_files.iter() {
        let path = blob_file.path();
        let expected = blob_file.checksum();

        match stream_checksum(path) {
            Ok(got) if got != expected => {
                report.errors.push(IntegrityError::BlobFileCorrupted {
                    blob_file_id: blob_file.id(),
                    path: path.to_path_buf(),
                    expected,
                    got,
                });
            }
            Ok(_) => {}
            Err(e) => {
                report.errors.push(IntegrityError::IoError {
                    path: path.to_path_buf(),
                    error: e,
                });
            }
        }

        report.blob_files_checked += 1;
    }

    report
}
