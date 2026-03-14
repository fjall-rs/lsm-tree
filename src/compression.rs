// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Decode, Encode};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Compression algorithm to use
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
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

    /// Zstd compression
    ///
    /// Provides significantly better compression ratios than LZ4
    /// with reasonable decompression speed (~1.5 GB/s).
    ///
    /// Compression level can be adjusted (1-22, default 3):
    /// - 1 optimizes for speed
    /// - 3 is a good default (recommended)
    /// - 9+ optimizes for compression ratio
    ///
    /// Recommended for cold/archival data where compression ratio
    /// matters more than raw speed.
    #[cfg(feature = "zstd")]
    Zstd(i32),
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::None => "none",

                #[cfg(feature = "lz4")]
                Self::Lz4 => "lz4",

                #[cfg(feature = "zstd")]
                Self::Zstd(_) => "zstd",
            }
        )
    }
}

impl Encode for CompressionType {
    fn encode_into<W: Write>(&self, writer: &mut W) -> Result<(), crate::Error> {
        match self {
            Self::None => {
                writer.write_u8(0)?;
            }

            #[cfg(feature = "lz4")]
            Self::Lz4 => {
                writer.write_u8(1)?;
            }

            #[cfg(feature = "zstd")]
            Self::Zstd(level) => {
                if !(1..=22).contains(level) {
                    return Err(crate::Error::Io(std::io::Error::other(format!(
                        "invalid zstd compression level {level}, expected 1..=22"
                    ))));
                }

                writer.write_u8(3)?;
                #[expect(clippy::cast_possible_truncation, reason = "validated 1..=22 above")]
                writer.write_i8(*level as i8)?;
            }
        }

        Ok(())
    }
}

impl Decode for CompressionType {
    fn decode_from<R: Read>(reader: &mut R) -> Result<Self, crate::Error> {
        let tag = reader.read_u8()?;

        match tag {
            0 => Ok(Self::None),

            #[cfg(feature = "lz4")]
            1 => Ok(Self::Lz4),

            #[cfg(feature = "zstd")]
            3 => {
                let level = i32::from(reader.read_i8()?);
                if !(1..=22).contains(&level) {
                    return Err(crate::Error::Io(std::io::Error::other(format!(
                        "invalid zstd compression level {level}, expected 1..=22"
                    ))));
                }
                Ok(Self::Zstd(level))
            }

            tag => Err(crate::Error::InvalidTag(("CompressionType", tag))),
        }
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
        fn compression_serialize_lz4() {
            let serialized = CompressionType::Lz4.encode_into_vec();
            assert_eq!(1, serialized.len());
        }
    }

    #[cfg(feature = "zstd")]
    mod zstd {
        use super::*;
        use test_log::test;

        #[test]
        fn compression_serialize_zstd() {
            let serialized = CompressionType::Zstd(3).encode_into_vec();
            assert_eq!(2, serialized.len());
        }

        #[test]
        fn compression_roundtrip_zstd() {
            for level in [1, 3, 9, 19] {
                let original = CompressionType::Zstd(level);
                let serialized = original.encode_into_vec();
                let decoded =
                    CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
                assert_eq!(original, decoded);
            }
        }

        #[test]
        fn compression_display_zstd() {
            assert_eq!(format!("{}", CompressionType::Zstd(3)), "zstd");
        }

        #[test]
        fn compression_zstd_rejects_invalid_level() {
            for invalid_level in [0, 23, -1, 200] {
                let mut buf = vec![];
                let result = CompressionType::Zstd(invalid_level).encode_into(&mut buf);
                assert!(result.is_err(), "level {invalid_level} should be rejected");
            }
        }
    }
}
