// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use std::{io::Write, path::Path};

pub const MAGIC_BYTES: [u8; 4] = [b'L', b'S', b'M', 2];

pub const MANIFEST_FILE: &str = "manifest";
pub const SEGMENTS_FOLDER: &str = "segments";
pub const LEVELS_MANIFEST_FILE: &str = "levels";
pub const BLOBS_FOLDER: &str = "blobs";

/// Atomically rewrites a file
pub fn rewrite_atomic(path: &Path, content: &[u8]) -> std::io::Result<()> {
    // NOTE: Nothing we can do
    #[allow(clippy::expect_used)]
    let folder = path.parent().expect("should have a parent");

    let mut temp_file = tempfile::NamedTempFile::new_in(folder)?;
    temp_file.write_all(content)?;
    temp_file.persist(path)?;

    // TODO: not sure why it fails on Windows...
    #[cfg(not(target_os = "windows"))]
    {
        let file = std::fs::File::open(path)?;
        file.sync_all()?;
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn fsync_directory(path: &Path) -> std::io::Result<()> {
    let file = std::fs::File::open(path)?;
    debug_assert!(file.metadata()?.is_dir());
    file.sync_all()
}

#[cfg(target_os = "windows")]
pub fn fsync_directory(path: &Path) -> std::io::Result<()> {
    // Cannot fsync directory on Windows
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use test_log::test;

    #[test]
    fn atomic_rewrite() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        {
            let mut file = File::create(&path)?;
            write!(file, "asdasdasdasdasd")?;
        }

        rewrite_atomic(&path, b"newcontent")?;

        let content = std::fs::read_to_string(&path)?;
        assert_eq!("newcontent", content);

        Ok(())
    }
}
