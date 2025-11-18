// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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
}

impl<W: std::io::Write> std::io::Write for ChecksummedWriter<W> {
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        self.inner.write(buf)
    }
}
