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
    pub(crate) fn from_raw(value: u128) -> Self {
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
