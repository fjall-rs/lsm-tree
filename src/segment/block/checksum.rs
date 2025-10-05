// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// An 128-bit checksum
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Checksum(u128);

impl std::ops::Deref for Checksum {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<sfa::Checksum> for Checksum {
    fn from(value: sfa::Checksum) -> Self {
        Self(value.into_u128())
    }
}

impl Checksum {
    #[must_use]
    pub fn from_raw(value: u128) -> Self {
        Self(value)
    }
}
