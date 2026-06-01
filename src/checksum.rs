// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ChecksumType {
    Xxh3,
}

impl From<ChecksumType> for u8 {
    fn from(val: ChecksumType) -> Self {
        match val {
            ChecksumType::Xxh3 => 0,
        }
    }
}

/// An 128-bit checksum
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Checksum(u128);

impl From<sfa::Checksum> for Checksum {
    fn from(value: sfa::Checksum) -> Self {
        Self(value.into_u128())
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Checksum {
    /// Wraps a checksum value.
    #[must_use]
    pub fn from_raw(value: u128) -> Self {
        Self(value)
    }

    /// Returns the raw 128-bit integer.
    #[must_use]
    pub fn into_u128(self) -> u128 {
        self.0
    }

    pub(crate) fn check(&self, expected: Self) -> crate::Result<()> {
        if self.0 == expected.0 {
            Ok(())
        } else {
            Err(crate::Error::ChecksumMismatch {
                expected,
                got: *self,
            })
        }
    }
}

pub struct ChecksummedWriter<W: std::io::Write> {
    inner: W,
    hasher: xxhash_rust::xxh3::Xxh3Default,
}

impl<W: std::io::Write + std::io::Seek> std::io::Seek for ChecksummedWriter<W> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl<W: std::io::Write> ChecksummedWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            inner: writer,
            hasher: xxhash_rust::xxh3::Xxh3Default::new(),
        }
    }

    pub fn checksum(&self) -> Checksum {
        Checksum::from_raw(self.hasher.digest128())
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Consumes the wrapper and returns the inner writer plus the accumulated checksum.
    ///
    /// Used at finalization to take ownership of the underlying file for the platform
    /// `set_len` + `sync_all` step required by direct-I/O writers.
    pub fn into_inner(self) -> (W, Checksum) {
        let checksum = Checksum::from_raw(self.hasher.digest128());
        (self.inner, checksum)
    }
}

impl<W: std::io::Write> std::io::Write for ChecksummedWriter<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Hash only the bytes the inner writer accepted. Hashing `buf` first would
        // double-count bytes on retries when `inner.write` returns `n < buf.len()`
        // (which `AlignedFileWriter` deliberately does once its internal buffer
        // fills): the default `Write::write_all` retries with `&buf[n..]`, and each
        // retry would re-hash an overlapping prefix.
        let n = self.inner.write(buf)?;
        #[expect(
            clippy::indexing_slicing,
            reason = "n <= buf.len() per Write::write contract"
        )]
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use test_log::test;

    // Mock writer that returns short writes on demand, exercising the same path
    // `AlignedFileWriter` triggers when its internal aligned buffer fills.
    struct ShortWriter {
        accumulated: Vec<u8>,
        chunk_size: usize,
    }

    impl Write for ShortWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let n = buf.len().min(self.chunk_size);
            #[expect(clippy::indexing_slicing, reason = "n <= buf.len()")]
            self.accumulated.extend_from_slice(&buf[..n]);
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn checksum_matches_actual_bytes_under_short_writes() -> std::io::Result<()> {
        let payload: Vec<u8> = (0..10_000u32).map(|i| (i & 0xFF) as u8).collect();

        // Reference: hash the payload directly.
        let mut reference_hasher = xxhash_rust::xxh3::Xxh3Default::new();
        reference_hasher.update(&payload);
        let reference = reference_hasher.digest128();

        // Pipe through ChecksummedWriter -> ShortWriter (returns 64-byte chunks).
        // `Write::write_all` will issue ~157 retries; the buggy implementation
        // would re-hash later bytes on each retry.
        let short_writer = ShortWriter {
            accumulated: Vec::new(),
            chunk_size: 64,
        };
        let mut cw = ChecksummedWriter::new(short_writer);
        cw.write_all(&payload)?;

        let (inner, observed) = cw.into_inner();
        assert_eq!(inner.accumulated, payload);
        assert_eq!(observed.into_u128(), reference);
        Ok(())
    }

    #[test]
    fn checksum_check_matches_and_mismatches() {
        let a = Checksum::from_raw(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
        let b = Checksum::from_raw(0x1234_u128);
        // Equal pair: ok.
        a.check(a).unwrap();
        // Different pair: returns ChecksumMismatch with both sides preserved.
        let err = a.check(b).unwrap_err();
        match err {
            crate::Error::ChecksumMismatch { expected, got } => {
                assert_eq!(expected, b);
                assert_eq!(got, a);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn checksum_display_is_debug() {
        let c = Checksum::from_raw(42);
        assert_eq!(format!("{c}"), format!("{c:?}"));
    }

    #[test]
    fn checksum_writer_inner_mut_returns_underlying() -> std::io::Result<()> {
        let mut cw = ChecksummedWriter::new(Vec::<u8>::new());
        cw.write_all(b"abc")?;
        // Drive the inner writer directly via inner_mut; those bytes bypass the
        // hasher.
        cw.inner_mut().extend_from_slice(b"xyz");
        let (inner, ck) = cw.into_inner();
        assert_eq!(inner, b"abcxyz");

        // The recorded checksum covers only "abc", not "abcxyz".
        let mut h = xxhash_rust::xxh3::Xxh3Default::new();
        h.update(b"abc");
        assert_eq!(ck.into_u128(), h.digest128());
        Ok(())
    }

    #[test]
    fn checksum_type_u8_round() {
        let u: u8 = ChecksumType::Xxh3.into();
        assert_eq!(u, 0);
    }
}
