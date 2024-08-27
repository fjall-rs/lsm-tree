// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    serde::{Deserializable, Serializable},
    SerializeError,
};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Write;

/// Disk format version
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Version {
    /// Version for 1.x.x releases
    V1,

    /// Version for 2.x.x releases
    V2,
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", u16::from(*self))
    }
}

impl From<Version> for u16 {
    fn from(value: Version) -> Self {
        match value {
            Version::V1 => 1,
            Version::V2 => 2,
        }
    }
}

impl TryFrom<u16> for Version {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::V1),
            2 => Ok(Self::V2),
            _ => Err(()),
        }
    }
}

const MAGIC_BYTES: [u8; 3] = [b'L', b'S', b'M'];

impl Version {
    // NOTE: Used in tests
    #[allow(unused)]
    pub(crate) fn len() -> u8 {
        5
    }
}

impl Deserializable for Version {
    fn deserialize<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::DeserializeError> {
        let mut header = [0; MAGIC_BYTES.len()];
        reader.read_exact(&mut header)?;

        if header != MAGIC_BYTES {
            return Err(crate::DeserializeError::InvalidHeader("Manifest"));
        }

        let version = reader.read_u16::<BigEndian>()?;
        Ok(Version::try_from(version).expect("invalid version"))
    }
}

impl Serializable for Version {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        writer.write_all(&MAGIC_BYTES)?;
        writer.write_u16::<BigEndian>((*self).into())?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test_log::test]
    #[allow(clippy::expect_used)]
    pub fn version_serialize() -> crate::Result<()> {
        let mut bytes = vec![];
        Version::V1.serialize(&mut bytes)?;
        assert_eq!(bytes, &[b'L', b'S', b'M', 0, 1]);
        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::expect_used)]
    pub fn version_serialize_2() -> crate::Result<()> {
        let mut bytes = vec![];
        Version::V2.serialize(&mut bytes)?;
        assert_eq!(bytes, &[b'L', b'S', b'M', 0, 2]);
        Ok(())
    }

    #[test_log::test]
    #[allow(clippy::expect_used)]
    pub fn version_deserialize_success() {
        let bytes = &[b'L', b'S', b'M', 0, 1];
        let version = Version::deserialize(&mut &bytes[..]).unwrap();
        assert_eq!(version, Version::V1);
    }

    #[test_log::test]
    #[allow(clippy::expect_used)]
    pub fn version_deserialize_success_2() {
        let bytes = &[b'L', b'S', b'M', 0, 2];
        let version = Version::deserialize(&mut &bytes[..]).unwrap();
        assert_eq!(version, Version::V2);
    }

    #[test_log::test]
    #[allow(clippy::expect_used)]
    pub fn version_serde_round_trip() {
        let mut buf = vec![];
        Version::V1.serialize(&mut buf).expect("can't fail");

        let mut cursor = Cursor::new(buf);
        let version = Version::deserialize(&mut cursor).unwrap();
        assert_eq!(version, Version::V1);
    }

    #[test_log::test]
    #[allow(clippy::expect_used)]
    pub fn version_len() {
        let mut buf = vec![];
        Version::V1.serialize(&mut buf).expect("can't fail");
        assert_eq!(Version::len() as usize, buf.len());
    }
}
