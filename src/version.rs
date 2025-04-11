// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

/// Disk format version
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Version {
    /// Version for 1.x.x releases
    V1,

    /// Version for 2.x.x releases
    V2,

    /// Version for 3.x.x releases
    V3,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u8::from(*self))
    }
}

impl From<Version> for u8 {
    fn from(value: Version) -> Self {
        match value {
            Version::V1 => 1,
            Version::V2 => 2,
            Version::V3 => 3,
        }
    }
}

impl TryFrom<u8> for Version {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            3 => Ok(Self::V3),
            _ => Err(()),
        }
    }
}
