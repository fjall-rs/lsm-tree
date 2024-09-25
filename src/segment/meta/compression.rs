// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Decode, DecodeError, Encode, EncodeError};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Compression algorithm to use.
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

    /// zlib/DEFLATE compression
    ///
    /// Compression level (0-10) can be adjusted.
    ///
    /// - 0 disables compression
    /// - 1 optimizes for speed
    /// - 6 compromises between speed and space, good default
    /// - 9 optimizes for space
    /// - 10 may save even more space than 9, but the speed trade off may not be worth it
    #[cfg(feature = "miniz")]
    Miniz(u8),

    /// zstd Compression
    ///
    /// Compression level (-128-22) can be adjusted.
    ///
    /// - -128~ -1 is fast compression level
    /// - A level of `0` uses zstd's default (currently `3`).
    /// - 1~19 normal compression level, higher is slower (1 is fastest, 3 is default, 12 is as fast as gzip level 6)
    /// - 20~22 ultra compression level, increase memory on both compression and decompression
    #[cfg(feature = "zstd")]
    Zstd(i32),
}

impl Encode for CompressionType {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), EncodeError> {
        match self {
            Self::None => {
                writer.write_u8(0)?;
                writer.write_u8(0)?; // NOTE: Pad to 2 bytes
            }

            #[cfg(feature = "lz4")]
            Self::Lz4 => {
                writer.write_u8(1)?;
                writer.write_u8(0)?; // NOTE: Pad to 2 bytes
            }

            #[cfg(feature = "miniz")]
            Self::Miniz(level) => {
                assert!(*level <= 10, "invalid miniz compression level");

                writer.write_u8(2)?;
                writer.write_u8(*level)?;
            }

            #[cfg(feature = "zstd")]
            Self::Zstd(level) => {
                assert!(
                    *level >= -128 && *level <= 22,
                    "invalid zstd compression level"
                );
                writer.write_u8(3)?;
                // TODO: this is dependent on endianness
                writer.write_u8(*level as u8)?;
            }
        };

        Ok(())
    }
}

impl Decode for CompressionType {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, DecodeError> {
        let tag = reader.read_u8()?;

        match tag {
            0 => {
                assert_eq!(0, reader.read_u8()?, "Invalid compression");
                Ok(Self::None)
            }

            #[cfg(feature = "lz4")]
            1 => {
                assert_eq!(0, reader.read_u8()?, "Invalid compression");
                Ok(Self::Lz4)
            }

            #[cfg(feature = "miniz")]
            2 => {
                let level = reader.read_u8()?;

                assert!(level <= 10, "invalid miniz compression level");

                Ok(Self::Miniz(level))
            }

            #[cfg(feature = "zstd")]
            3 => {
                // TODO: this is dependent on endianness
                let level = reader.read_u8()? as i8 as i32;

                assert!(
                    level >= -128 && level <= 22,
                    "invalid zstd compression level"
                );

                Ok(Self::Zstd(level))
            }

            tag => Err(DecodeError::InvalidTag(("CompressionType", tag))),
        }
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "no compression"),

            #[cfg(feature = "lz4")]
            Self::Lz4 => write!(f, "lz4"),

            #[cfg(feature = "miniz")]
            Self::Miniz(_) => write!(f, "miniz"),

            #[cfg(feature = "zstd")]
            Self::Zstd(level) => write!(f, "zstd({})", level),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test_log::test]
    fn compression_serialize_none() -> crate::Result<()> {
        let serialized = CompressionType::None.encode_into_vec()?;
        assert_eq!(2, serialized.len());
        assert_eq!(vec![0u8, 0u8], serialized);
        Ok(())
    }

    #[cfg(feature = "lz4")]
    mod lz4 {
        use super::*;

        #[test_log::test]
        fn compression_serialize_lz4() -> crate::Result<()> {
            let serialized = CompressionType::Lz4.encode_into_vec()?;
            assert_eq!(2, serialized.len());
            assert_eq!(vec![1u8, 0u8], serialized);
            Ok(())
        }
    }

    #[cfg(feature = "miniz")]
    mod miniz {
        use super::*;

        #[test_log::test]
        fn compression_serialize_miniz() -> crate::Result<()> {
            for lvl in 0..10 {
                let serialized = CompressionType::Miniz(lvl).encode_into_vec()?;
                assert_eq!(2, serialized.len());
                assert_eq!(vec![2u8, lvl], serialized);
            }
            Ok(())
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd {
        use super::*;

        #[test_log::test]
        fn compression_serialize_zstd() -> crate::Result<()> {
            for lvl in -128..22 {
                let serialized = CompressionType::Zstd(lvl).encode_into_vec()?;
                assert_eq!(2, serialized.len());
                assert_eq!(vec![3u8, lvl as u8], serialized);
            }
            Ok(())
        }
    }
}
