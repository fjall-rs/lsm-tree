// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    fs::{FileLike, FileSystem},
    Slice,
};
use std::{io::Write, path::Path};

pub const MAGIC_BYTES: [u8; 4] = [b'L', b'S', b'M', 3];

pub const TABLES_FOLDER: &str = "tables";
pub const BLOBS_FOLDER: &str = "blobs";
pub const CURRENT_VERSION_FILE: &str = "current";

/// Reads bytes from a file using `pread`.
pub fn read_exact(file: &impl FileLike, offset: u64, size: usize) -> std::io::Result<Slice> {
    // SAFETY: This slice builder starts uninitialized, but we know its length
    //
    // We use read_at/seek_read which give us the number of bytes read
    // If that number does not match the slice length, the function errors,
    // so the (partially) uninitialized buffer is discarded
    //
    // Additionally, generally, block loads furthermore do a checksum check which
    // would likely catch the buffer being wrong somehow
    #[expect(unsafe_code, reason = "see safety")]
    let mut builder = unsafe { Slice::builder_unzeroed(size) };

    {
        let bytes_read = file.read_at(&mut builder, offset)?;

        if bytes_read != size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "read_exact({bytes_read}) at {offset} did not read enough bytes {size}; file has length {}",
                    file.metadata()?.len(),
                ),
            ));
        }
    }

    Ok(builder.freeze().into())
}

/// Atomically rewrites a file.
pub fn rewrite_atomic<F: FileSystem>(path: &Path, content: &[u8]) -> std::io::Result<()> {
    #[expect(
        clippy::expect_used,
        reason = "every file should have a parent directory"
    )]
    let folder = path.parent().expect("should have a parent");

    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.flush()?;
    temp_file.as_file_mut().sync_all()?;
    temp_file.persist(path)?;

    // TODO: not sure why it fails on Windows...
    #[cfg(not(target_os = "windows"))]
    {
        let file = F::open(path)?;
        file.sync_all()?;

        #[expect(
            clippy::expect_used,
            reason = "files should always have a parent directory"
        )]
        let folder = path.parent().expect("should have parent folder");
        fsync_directory::<F>(folder)?;
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory<F: FileSystem>(path: &Path) -> std::io::Result<()> {
    let file = F::open(path)?;
    debug_assert!(file.metadata()?.is_dir());
    file.sync_all()
}

#[cfg(target_os = "windows")]
pub fn fsync_directory<F: FileSystem>(_path: &Path) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::FileSystem;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = <crate::fs::StdFileSystem as FileSystem>::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic::<crate::fs::StdFileSystem>(&path, b"newcontent")?;

        let content = crate::fs::StdFileSystem::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
