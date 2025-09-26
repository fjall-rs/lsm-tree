// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[cfg(feature = "zlib")]
const ZLIB_MAX_LEVEL: u8 = 9;

/// Compression algorithm to use
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[allow(clippy::module_name_repetitions)]
pub enum CompressionType {
    /// No compression
    ///
    /// Not recommended.
    None,

    /// LZ4 compression
    ///
    /// Recommended for use cases with a focus
    /// on speed over compression ratio.
    #[cfg(feature = "lz4")]
    Lz4,

    /// Zlib compression
    #[cfg(feature = "zlib")]
    Zlib(u8),
}

impl Encode for CompressionType {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        match self {
            Self::None => {
                writer.write_u8(0)?;
            }

            #[cfg(feature = "lz4")]
            Self::Lz4 => {
                writer.write_u8(1)?;
            }

            #[cfg(feature = "zlib")]
            Self::Zlib(level) => {
                if *level > ZLIB_MAX_LEVEL {
                    return Err(EncodeError::InvalidCompressionLevel((
                        Self::Zlib(*level),
                        *level as i32,
                    )));
                }
                writer.write_u8(2)?;
                writer.write_u8(*level)?;
            }
        }

        Ok(())
    }
}

impl Decode for CompressionType {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let tag = reader.read_u8()?;

        match tag {
            0 => Ok(Self::None),

            #[cfg(feature = "lz4")]
            1 => Ok(Self::Lz4),

            #[cfg(feature = "zlib")]
            2 => {
                let level = reader.read_u8()?;
                if level > ZLIB_MAX_LEVEL {
                    return Err(DecodeError::InvalidCompressionLevel((
                        Self::Zlib(level),
                        level as i32,
                    )));
                }
                Ok(Self::Zlib(level))
            }
            tag => Err(DecodeError::InvalidTag(("CompressionType", tag))),
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

                #[cfg(feature = "zlib")]
                Self::Zlib(level) => {
                    return write!(f, "zlib (level {})", level);
                }
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn compression_serialize_none() {
        let serialized = CompressionType::None.encode_into_vec();
        assert_eq!(1, serialized.len());
    }

    #[cfg(feature = "lz4")]
    mod lz4 {
        use super::*;
        use test_log::test;

        #[test]
        fn compression_serialize_none() {
            let serialized = CompressionType::Lz4.encode_into_vec();
            assert_eq!(1, serialized.len());
        }
    }

    #[cfg(feature = "zlib")]
    mod zlib {
        use super::*;
        use test_log::test;

        #[test]
        fn compression_serialize_zlib() {
            for level in 0..=ZLIB_MAX_LEVEL {
                let serialized = CompressionType::Zlib(level).encode_into_vec();
                assert_eq!(2, serialized.len());
            }
        }

        #[test]
        fn compression_serialize_zlib_invalid_level() {
            let err = CompressionType::Zlib(ZLIB_MAX_LEVEL + 1).encode_into_vec_err();
            assert!(matches!(
                err,
                Err(EncodeError::InvalidCompressionLevel((
                    CompressionType::Zlib(10),
                    10
                )))
            ));
        }
    }
}
