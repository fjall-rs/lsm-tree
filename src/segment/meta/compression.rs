use crate::{
    serde::{Deserializable, Serializable},
    DeserializeError, SerializeError,
};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub enum CompressionType {
    None,

    #[cfg(feature = "lz4")]
    Lz4,

    /// zlib/DEFLATE compression, with an adjustable level
    /// between
    #[cfg(feature = "miniz")]
    Miniz(u8),
}

impl Serializable for CompressionType {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), SerializeError> {
        match self {
            Self::None => {
                writer.write_u8(0)?;
            }

            #[cfg(feature = "lz4")]
            Self::Lz4 => {
                writer.write_u8(1)?;
            }

            #[cfg(feature = "miniz")]
            Self::Miniz(level) => {
                assert!(*level <= 10, "invalid miniz compression level");

                writer.write_u8(2)?;
                writer.write_u8(*level)?;
            }
        };

        Ok(())
    }
}

impl Deserializable for CompressionType {
    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, DeserializeError> {
        let tag = reader.read_u8()?;

        match tag {
            0 => Ok(Self::None),

            #[cfg(feature = "lz4")]
            1 => Ok(Self::Lz4),

            #[cfg(feature = "miniz")]
            2 => {
                let level = reader.read_u8()?;

                assert!(level <= 10, "invalid miniz compression level");

                Ok(Self::Miniz(level))
            }

            tag => Err(DeserializeError::InvalidTag(("CompressionType", tag))),
        }
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::None => "no compression",

                #[cfg(feature = "lz4")]
                Self::Lz4 => "lz4",

                #[cfg(feature = "miniz")]
                Self::Miniz(_) => "miniz",
            }
        )
    }
}
