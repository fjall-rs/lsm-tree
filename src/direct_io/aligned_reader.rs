// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{aligned_buffer::AlignedBuffer, BUFFER_BLOCKS};
use std::{fs::File, io};

/// Buffered reader that issues aligned reads to a direct-I/O-opened file.
///
/// Each refill reads `BUFFER_BLOCKS * alignment` bytes (or less if the file is shorter)
/// from the current file offset. Callers may then consume bytes at any granularity;
/// the reader hands them out from the aligned buffer.
///
/// `O_DIRECT` reads beyond end-of-file return a short read (fewer bytes than
/// requested). Short reads can also happen before EOF, so the reader tracks the
/// known file length and only latches `eof` once the logical file position reaches
/// that length.
pub struct AlignedFileReader {
    file: File,
    buffer: AlignedBuffer,
    alignment: usize,
    /// Immutable file length observed when the compaction input was opened.
    file_len: u64,
    /// Kernel file offset after the most recent refill.
    file_pos: u64,
    /// Number of valid bytes currently in the buffer (between 0 and `buffer.capacity()`).
    valid_len: usize,
    /// Next byte position to return to the caller, within `buffer[..valid_len]`.
    cursor: usize,
    /// Set after the reader reaches `file_len` or observes `read() == 0`.
    eof: bool,
}

impl AlignedFileReader {
    /// Wraps an already-opened file (assumed to have direct-I/O enabled).
    ///
    /// # Errors
    ///
    /// Returns an error if `alignment` is zero or the file length cannot be read.
    pub fn new(file: File, alignment: usize) -> io::Result<Self> {
        if alignment == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "direct-I/O alignment must be non-zero",
            ));
        }
        let capacity = alignment.saturating_mul(BUFFER_BLOCKS).max(alignment);
        let file_len = file.metadata()?.len();
        Ok(Self {
            file,
            buffer: AlignedBuffer::new(capacity, alignment),
            alignment,
            file_len,
            file_pos: 0,
            valid_len: 0,
            cursor: 0,
            eof: false,
        })
    }

    fn refill(&mut self) -> io::Result<()> {
        use io::Read;

        self.cursor = 0;
        self.valid_len = 0;

        if self.eof {
            return Ok(());
        }

        if self.file_pos >= self.file_len {
            self.eof = true;
            return Ok(());
        }

        let read_len = self.refill_read_len();
        let read_buf = self
            .buffer
            .as_mut_slice()
            .get_mut(..read_len)
            .ok_or_else(|| {
                io::Error::other("direct-I/O refill length exceeded aligned buffer capacity")
            })?;
        let n = loop {
            match self.file.read(read_buf) {
                Ok(n) => break n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        };

        if n == 0 {
            self.eof = true;
            if self.file_pos < self.file_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "direct-I/O read returned EOF at offset {} before known file length {}",
                        self.file_pos, self.file_len,
                    ),
                ));
            }
            return Ok(());
        }

        self.valid_len = n;

        let n_u64 = u64::try_from(n).map_err(|_| {
            io::Error::other("direct-I/O read length does not fit into a u64 file offset")
        })?;
        self.file_pos = self.file_pos.checked_add(n_u64).ok_or_else(|| {
            io::Error::other("direct-I/O read advanced past the maximum u64 file offset")
        })?;

        if self.file_pos >= self.file_len {
            self.eof = true;
        } else if !n.is_multiple_of(self.alignment) {
            disable_direct_io_for_remainder(&self.file)?;
        }
        Ok(())
    }

    fn refill_read_len(&self) -> usize {
        forced_max_read_len(self.buffer.capacity(), self.alignment)
            .unwrap_or_else(|| self.buffer.capacity())
    }
}

#[cfg(target_os = "linux")]
fn disable_direct_io_for_remainder(file: &File) -> io::Result<()> {
    super::linux::disable_direct_io(file)
}

#[cfg(not(target_os = "linux"))]
#[expect(
    clippy::unnecessary_wraps,
    reason = "matches the Linux helper signature so refill can use one call site"
)]
fn disable_direct_io_for_remainder(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(debug_assertions)]
const TEST_MAX_READ_BYTES_ENV: &str = "LSM_TREE_TEST_DIRECT_IO_MAX_READ_BYTES";

#[cfg(debug_assertions)]
fn forced_max_read_len(capacity: usize, alignment: usize) -> Option<usize> {
    let limit = std::env::var(TEST_MAX_READ_BYTES_ENV)
        .ok()?
        .parse::<usize>()
        .ok()?
        .min(capacity);
    let aligned = (limit / alignment) * alignment;
    if aligned == 0 {
        None
    } else {
        Some(aligned)
    }
}

#[cfg(not(debug_assertions))]
fn forced_max_read_len(_capacity: usize, _alignment: usize) -> Option<usize> {
    None
}

impl io::Read for AlignedFileReader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }

        if self.cursor == self.valid_len {
            self.refill()?;
            if self.valid_len == 0 {
                return Ok(0);
            }
        }

        let available = self.valid_len - self.cursor;
        let n = dst.len().min(available);
        // Bounded by the min above; both slices are `n` bytes and indices are valid.
        #[expect(
            clippy::indexing_slicing,
            reason = "n is bounded by both dst.len() and (valid_len - cursor), so all indices are valid"
        )]
        {
            dst[..n].copy_from_slice(&self.buffer.as_slice()[self.cursor..self.cursor + n]);
        }
        self.cursor += n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use test_log::test;

    fn alignment_for_test() -> usize {
        512
    }

    fn write_data(path: &std::path::Path, data: &[u8]) -> io::Result<()> {
        let mut f = std::fs::File::create(path)?;
        f.write_all(data)?;
        f.sync_all()?;
        Ok(())
    }

    #[cfg(debug_assertions)]
    struct ForcedMaxReadGuard {
        previous: Option<std::ffi::OsString>,
    }

    #[cfg(debug_assertions)]
    impl ForcedMaxReadGuard {
        fn set(max_read_bytes: usize) -> Self {
            let previous = std::env::var_os(TEST_MAX_READ_BYTES_ENV);
            std::env::set_var(TEST_MAX_READ_BYTES_ENV, max_read_bytes.to_string());
            Self { previous }
        }
    }

    #[cfg(debug_assertions)]
    impl Drop for ForcedMaxReadGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(TEST_MAX_READ_BYTES_ENV, previous);
            } else {
                std::env::remove_var(TEST_MAX_READ_BYTES_ENV);
            }
        }
    }

    #[test]
    fn aligned_reader_roundtrip_exact_alignment() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("exact");
        let align = alignment_for_test();
        let data: Vec<u8> = (0..(align * 4)).map(|i| (i & 0xFF) as u8).collect();
        write_data(&path, &data)?;

        let file = std::fs::File::open(&path)?;
        let mut reader = AlignedFileReader::new(file, align)?;

        let mut out = vec![];
        reader.read_to_end(&mut out)?;
        assert_eq!(data, out);
        Ok(())
    }

    #[test]
    fn aligned_reader_roundtrip_unaligned_length() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("unaligned");
        let align = alignment_for_test();
        let data: Vec<u8> = (0..(align * 2 + 173)).map(|i| (i & 0xFF) as u8).collect();
        write_data(&path, &data)?;

        let file = std::fs::File::open(&path)?;
        let mut reader = AlignedFileReader::new(file, align)?;

        let mut out = vec![];
        reader.read_to_end(&mut out)?;
        assert_eq!(data, out);
        Ok(())
    }

    #[test]
    fn aligned_reader_many_small_reads() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("small");
        let align = alignment_for_test();
        let total = align * 3 + 5;
        let data: Vec<u8> = (0..total).map(|i| (i & 0xFF) as u8).collect();
        write_data(&path, &data)?;

        let file = std::fs::File::open(&path)?;
        let mut reader = AlignedFileReader::new(file, align)?;

        for (i, expected) in data.iter().enumerate() {
            let mut byte = [0u8; 1];
            let n = reader.read(&mut byte)?;
            assert_eq!(n, 1, "short read at offset {i}");
            assert_eq!(byte[0], *expected, "mismatch at offset {i}");
        }
        let mut after_end = [0u8; 1];
        assert_eq!(reader.read(&mut after_end)?, 0);
        Ok(())
    }

    #[test]
    fn aligned_reader_empty_file() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("empty");
        let align = alignment_for_test();
        write_data(&path, &[])?;

        let file = std::fs::File::open(&path)?;
        let mut reader = AlignedFileReader::new(file, align)?;

        let mut buf = vec![];
        reader.read_to_end(&mut buf)?;
        assert!(buf.is_empty());
        Ok(())
    }

    #[cfg(debug_assertions)]
    #[test]
    fn aligned_reader_continues_after_aligned_short_read_before_eof() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("short-before-eof");
        let align = alignment_for_test();
        let total = align * (BUFFER_BLOCKS + 3) + 17;
        let data: Vec<u8> = (0..total).map(|i| (i & 0xFF) as u8).collect();
        write_data(&path, &data)?;

        let _guard = ForcedMaxReadGuard::set(align);
        let file = std::fs::File::open(&path)?;
        let mut reader = AlignedFileReader::new(file, align)?;

        let mut out = vec![0; align];
        reader.read_exact(&mut out)?;
        assert!(
            !reader.eof,
            "short aligned read before EOF must not latch EOF"
        );
        assert_eq!(
            reader.file_pos,
            u64::try_from(align).map_err(|_| io::Error::other("alignment overflows u64"))?,
        );

        reader.read_to_end(&mut out)?;
        assert_eq!(out, data);
        assert!(reader.eof);
        Ok(())
    }

    /// A short read at the known file length is EOF, even when the file's tail is
    /// not alignment-sized.
    #[test]
    fn aligned_reader_latches_eof_on_short_read_at_known_file_end() -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("short");
        let align = alignment_for_test();
        // File size is *exactly less* than one buffer capacity worth of data,
        // forcing the first refill into a short read.
        let total = align * 2 + 13; // 1037 bytes, well below 512*16 = 8192 capacity.
        let data: Vec<u8> = (0..total).map(|i| (i & 0xFF) as u8).collect();
        write_data(&path, &data)?;

        let file = std::fs::File::open(&path)?;
        let mut reader = AlignedFileReader::new(file, align)?;

        let mut out = vec![];
        reader.read_to_end(&mut out)?;
        assert_eq!(out, data);
        assert!(
            reader.eof,
            "reader should have latched EOF after the short read"
        );
        Ok(())
    }
}
