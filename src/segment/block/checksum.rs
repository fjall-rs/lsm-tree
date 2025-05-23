// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// An 64-bit checksum
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Checksum(u64);

impl std::ops::Deref for Checksum {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Checksum {
    #[must_use]
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }
}
