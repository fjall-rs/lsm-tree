// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    fs::{Fs, FsFile},
    Slice,
};
use std::{io::Write, path::Path};

pub const MAGIC_BYTES: [u8; 4] = [b'L', b'S', b'M', 3];

pub const TABLES_FOLDER: &str = "tables";
pub const BLOBS_FOLDER: &str = "blobs";
pub const CURRENT_VERSION_FILE: &str = "current";

/// Reads bytes from a file at the given offset without changing the cursor.
///
/// Uses [`FsFile::read_at`] (equivalent to `pread(2)`) so multiple threads
/// can call this concurrently on the same file handle.
pub fn read_exact(file: &dyn FsFile, offset: u64, size: usize) -> std::io::Result<Slice> {
    // SAFETY: This slice builder starts uninitialized, but we know its length
    //
    // We use FsFile::read_at which gives us the number of bytes read.
    // If that number does not match the slice length, the function errors,
    // so the (partially) uninitialized buffer is discarded.
    //
    // Additionally, generally, block loads furthermore do a checksum check which
    // would likely catch the buffer being wrong somehow.
    #[expect(unsafe_code, reason = "see safety")]
    let mut builder = unsafe { Slice::builder_unzeroed(size) };

    // Single call is correct: FsFile::read_at has fill-or-EOF semantics —
    // implementations handle EINTR/short-read retry internally.
    let bytes_read = file.read_at(&mut builder, offset)?;

    if bytes_read != size {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("read_exact({bytes_read}) at {offset} did not read enough bytes {size}; file has length {}", file.metadata()?.len),
        ));
    }

    Ok(builder.freeze().into())
}

/// Atomically rewrites a file.
pub fn rewrite_atomic(path: &Path, content: &[u8], fs: &dyn Fs) -> std::io::Result<()> {
    #[expect(
        clippy::expect_used,
        reason = "every file should have a parent directory"
    )]
    let folder = path.parent().expect("should have a parent");

    // NOTE: tempfile crate uses std::fs internally; migrating temp-file
    // creation to Fs would require a custom implementation.
    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.flush()?;
    temp_file.as_file_mut().sync_all()?;
    temp_file.persist(path)?;

    // Suppress unused-variable warning on Windows where the post-persist
    // sync block is skipped (directory fsync is unsupported).
    let _ = &fs;

    #[cfg(not(target_os = "windows"))]
    {
        use crate::fs::FsOpenOptions;

        let file = fs.open(path, &FsOpenOptions::new().read(true))?;
        FsFile::sync_all(&*file)?;

        #[expect(
            clippy::expect_used,
            reason = "files should always have a parent directory"
        )]
        let folder = path.parent().expect("should have parent folder");
        fs.sync_directory(folder)?;
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory(path: &Path, fs: &dyn Fs) -> std::io::Result<()> {
    fs.sync_directory(path)
}

#[cfg(target_os = "windows")]
pub fn fsync_directory(_path: &Path, _fs: &dyn Fs) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;
    use crate::fs::StdFs;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn read_exact_short_read_returns_error() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("short.bin");
        {
            let mut f = File::create(&path)?;
            f.write_all(b"hello")?; // 5 bytes
        }

        let file = File::open(&path)?;
        // Request 10 bytes from a 5-byte file → short read → UnexpectedEof
        let err = read_exact(&file, 0, 10).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

        Ok(())
    }

    #[test]
    fn atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent", &StdFs)?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
