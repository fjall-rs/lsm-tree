// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{
    ffi::{OsStr, OsString},
    io,
    io::{Read, Seek, Write},
    path::{Path, PathBuf},
};

/// Minimal metadata needed by the storage layer.
#[derive(Clone, Copy, Debug)]
pub struct Metadata {
    len: u64,
    is_dir: bool,
}

impl Metadata {
    /// Returns the file length in bytes.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Returns whether the file length is zero.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns whether this entry is a directory.
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.is_dir
    }
}

/// File abstraction for pluggable storage backends.
pub trait FileLike: Read + Write + Seek + Send + Sync {
    /// Reads bytes at a given offset without changing the current cursor.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    /// Flushes file contents to durable storage.
    fn sync_all(&self) -> io::Result<()>;
    /// Retrieves minimal file metadata.
    fn metadata(&self) -> io::Result<Metadata>;
}

/// Filesystem abstraction for pluggable storage backends.
pub trait FileSystem: Send + Sync + std::panic::RefUnwindSafe + std::panic::UnwindSafe {
    /// File handle type for this filesystem.
    type File: FileLike;

    /// Opens an existing file for reading.
    fn open(path: &Path) -> io::Result<Self::File>;
    /// Creates or truncates a file for writing.
    fn create(path: &Path) -> io::Result<Self::File>;
    /// Creates a new file, failing if it already exists.
    fn create_new(path: &Path) -> io::Result<Self::File>;
    /// Reads a file into memory.
    fn read(path: &Path) -> io::Result<Vec<u8>>;
    /// Reads a UTF-8 file into a string.
    fn read_to_string(path: &Path) -> io::Result<String>;
    /// Lists directory entries.
    fn read_dir(path: &Path) -> io::Result<Vec<DirEntry>>;
    /// Creates a directory and all missing parents.
    fn create_dir_all(path: &Path) -> io::Result<()>;
    /// Removes a file.
    fn remove_file(path: &Path) -> io::Result<()>;
    /// Removes a directory and all its contents.
    fn remove_dir_all(path: &Path) -> io::Result<()>;
    /// Checks whether a path exists.
    fn exists(path: &Path) -> io::Result<bool>;
}

/// Lightweight directory entry used by [`FileSystem`].
#[derive(Clone, Debug)]
pub struct DirEntry {
    path: PathBuf,
    file_name: OsString,
    is_dir: bool,
}

impl DirEntry {
    /// Returns the full path for this entry.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the file name for this entry.
    #[must_use]
    pub fn file_name(&self) -> &OsStr {
        &self.file_name
    }

    /// Returns whether the entry is a directory.
    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.is_dir
    }
}

/// `std::fs`-backed filesystem implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct StdFileSystem;

impl FileSystem for StdFileSystem {
    type File = std::fs::File;

    fn open(path: &Path) -> io::Result<Self::File> {
        Self::File::open(path)
    }

    fn create(path: &Path) -> io::Result<Self::File> {
        Self::File::create(path)
    }

    fn create_new(path: &Path) -> io::Result<Self::File> {
        Self::File::create_new(path)
    }

    fn read(path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn read_to_string(path: &Path) -> io::Result<String> {
        std::fs::read_to_string(path)
    }

    fn read_dir(path: &Path) -> io::Result<Vec<DirEntry>> {
        std::fs::read_dir(path)?
            .map(|entry| {
                entry.and_then(|entry| {
                    let file_name = entry.file_name();
                    let file_type = entry.file_type()?;
                    Ok(DirEntry {
                        path: entry.path(),
                        file_name,
                        is_dir: file_type.is_dir(),
                    })
                })
            })
            .collect()
    }

    fn create_dir_all(path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn remove_file(path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn exists(path: &Path) -> io::Result<bool> {
        path.try_exists()
    }
}

impl FileLike for std::fs::File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            FileExt::read_at(self, buf, offset)
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.seek_read(buf, offset)
        }

        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("unsupported platform");
            unimplemented!();
        }
    }

    fn sync_all(&self) -> io::Result<()> {
        std::fs::File::sync_all(self)
    }

    fn metadata(&self) -> io::Result<Metadata> {
        let metadata = std::fs::File::metadata(self)?;
        Ok(Metadata {
            len: metadata.len(),
            is_dir: metadata.is_dir(),
        })
    }
}
