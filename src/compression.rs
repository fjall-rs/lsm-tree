// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::coding::{Decode, Encode};
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[cfg(feature = "zstd")]
use std::sync::Arc;

/// Pre-trained zstd dictionary for improved compression of small blocks.
///
/// Zstd dictionaries significantly improve compression ratios for blocks
/// in the 4–64 KiB range typical of LSM-trees, especially when data has
/// recurring patterns (e.g., structured keys, repeated prefixes,
/// JSON/MessagePack values).
///
/// The dictionary is identified by a 32-bit ID derived from its content
/// (truncated xxh3 hash). This ID is stored alongside compressed blocks
/// so readers can detect dictionary mismatches.
///
/// # Example
///
/// ```ignore
/// use lsm_tree::ZstdDictionary;
///
/// let samples: &[u8] = &training_data;
/// let dict = ZstdDictionary::new(samples);
/// ```
#[cfg(feature = "zstd")]
#[derive(Clone)]
pub struct ZstdDictionary {
    id: u32,
    raw: Arc<[u8]>,
}

#[cfg(feature = "zstd")]
impl ZstdDictionary {
    /// Creates a new dictionary from raw bytes.
    ///
    /// The raw bytes should be a pre-trained zstd dictionary (e.g., output
    /// of `zstd::dict::from_continuous` or `zstd --train`). The dictionary
    /// ID is computed as a truncated xxh3 hash of the content.
    #[must_use]
    pub fn new(raw: &[u8]) -> Self {
        Self {
            id: compute_dict_id(raw),
            raw: Arc::from(raw),
        }
    }

    /// Returns the dictionary ID (truncated xxh3 hash of the raw bytes).
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Returns the raw dictionary bytes.
    #[must_use]
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }
}

#[cfg(feature = "zstd")]
impl std::fmt::Debug for ZstdDictionary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdDictionary")
            .field("id", &format_args!("{:#010x}", self.id))
            .field("size", &self.raw.len())
            .finish()
    }
}

/// Compute a 32-bit dictionary ID from raw bytes via truncated xxh3.
#[cfg(feature = "zstd")]
#[expect(
    clippy::cast_possible_truncation,
    reason = "intentionally truncated to 32-bit fingerprint"
)]
fn compute_dict_id(raw: &[u8]) -> u32 {
    xxhash_rust::xxh3::xxh3_64(raw) as u32
}

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
    // NOTE: Uses i32 (not a validated newtype) to match upstream's public API and
    // the zstd crate's compress(data, level: i32) signature. Validated levels are
    // produced by CompressionType::zstd() and Decode::decode_from; direct construction
    // via CompressionType::Zstd(level) must uphold the 1..=22 invariant.
    #[cfg(feature = "zstd")]
    Zstd(i32),

    /// Zstd compression with a pre-trained dictionary
    ///
    /// Uses a pre-trained dictionary for significantly better compression
    /// ratios on small blocks (4–64 KiB), especially when data has recurring
    /// patterns.
    ///
    /// `level` is the compression level (1–22), `dict_id` identifies the
    /// dictionary (truncated xxh3 hash of the dictionary bytes). The actual
    /// dictionary must be provided via [`Config`] or the relevant writer/reader.
    #[cfg(feature = "zstd")]
    ZstdDict {
        /// Compression level (1–22)
        level: i32,

        /// Dictionary fingerprint for mismatch detection
        dict_id: u32,
    },
}

impl CompressionType {
    /// Validate a zstd compression level.
    ///
    /// Accepts levels in the range 1..=22 and returns an error otherwise.
    #[cfg(feature = "zstd")]
    fn validate_zstd_level(level: i32) -> crate::Result<()> {
        if !(1..=22).contains(&level) {
            // NOTE: Uses Error::other (not ErrorKind::InvalidInput) to match
            // upstream's error style and minimize fork divergence.
            return Err(crate::Error::Io(std::io::Error::other(format!(
                "invalid zstd compression level {level}, expected 1..=22"
            ))));
        }
        Ok(())
    }

    /// Create a zstd compression configuration with a checked level.
    ///
    /// This is the recommended way to construct a `CompressionType::Zstd`
    /// value, as it validates the level before any I/O occurs.
    ///
    /// # Errors
    ///
    /// Returns an error if `level` is outside the valid range `1..=22`.
    #[cfg(feature = "zstd")]
    pub fn zstd(level: i32) -> crate::Result<Self> {
        Self::validate_zstd_level(level)?;
        Ok(Self::Zstd(level))
    }

    /// Create a zstd dictionary compression configuration with checked level.
    ///
    /// The `dict_id` should come from [`ZstdDictionary::id`] to ensure
    /// consistency between the compression type stored on disk and the
    /// dictionary used at runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if `level` is outside the valid range `1..=22`.
    #[cfg(feature = "zstd")]
    pub fn zstd_dict(level: i32, dict_id: u32) -> crate::Result<Self> {
        Self::validate_zstd_level(level)?;
        Ok(Self::ZstdDict { level, dict_id })
    }
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

                #[cfg(feature = "zstd")]
                Self::ZstdDict { .. } => "zstd+dict",
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
                writer.write_u8(3)?;
                // Catch invalid levels in debug builds (e.g. direct Zstd(999) construction).
                // Not a runtime error — encoding must stay infallible for encode_into_vec().
                debug_assert!(
                    (1..=22).contains(level),
                    "zstd level {level} outside valid range 1..=22"
                );
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "level range 1..=22 fits i8"
                )]
                writer.write_i8(*level as i8)?;
            }

            #[cfg(feature = "zstd")]
            Self::ZstdDict { level, dict_id } => {
                writer.write_u8(4)?;
                debug_assert!(
                    (1..=22).contains(level),
                    "zstd level {level} outside valid range 1..=22"
                );
                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "level range 1..=22 fits i8"
                )]
                writer.write_i8(*level as i8)?;
                byteorder::WriteBytesExt::write_u32::<byteorder::LittleEndian>(writer, *dict_id)?;
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
                // Reuse the shared validation logic to ensure consistent checks.
                Self::validate_zstd_level(level)?;
                Ok(Self::Zstd(level))
            }

            #[cfg(feature = "zstd")]
            4 => {
                let level = i32::from(reader.read_i8()?);
                Self::validate_zstd_level(level)?;
                let dict_id = byteorder::ReadBytesExt::read_u32::<byteorder::LittleEndian>(reader)?;
                Ok(Self::ZstdDict { level, dict_id })
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
                let result = CompressionType::zstd(invalid_level);
                assert!(result.is_err(), "level {invalid_level} should be rejected");
            }
        }

        #[test]
        fn compression_zstd_decode_rejects_invalid_level() {
            // Serialize a valid zstd value, then corrupt the level byte
            let valid = CompressionType::Zstd(3).encode_into_vec();
            assert_eq!(valid.len(), 2);

            // Flip level byte to 0 (out of range 1..=22)
            let corrupted = vec![valid[0], 0];
            let result = CompressionType::decode_from(&mut &corrupted[..]);
            assert!(result.is_err(), "level 0 should be rejected on decode");

            // Flip level byte to 23 (out of range)
            let corrupted = vec![valid[0], 23];
            let result = CompressionType::decode_from(&mut &corrupted[..]);
            assert!(result.is_err(), "level 23 should be rejected on decode");
        }

        #[test]
        fn compression_serialize_zstd_dict() {
            let serialized = CompressionType::ZstdDict {
                level: 3,
                dict_id: 0xDEAD_BEEF,
            }
            .encode_into_vec();
            // tag=4, level=3 as i8, dict_id=0xDEAD_BEEF in little-endian
            assert_eq!(serialized, [4, 3, 0xEF, 0xBE, 0xAD, 0xDE]);
        }

        #[test]
        fn compression_roundtrip_zstd_dict() {
            for level in [1, 3, 9, 19] {
                for dict_id in [0, 1, 0xDEAD_BEEF, u32::MAX] {
                    let original = CompressionType::ZstdDict { level, dict_id };
                    let serialized = original.encode_into_vec();
                    let decoded =
                        CompressionType::decode_from(&mut &serialized[..]).expect("decode failed");
                    assert_eq!(original, decoded);
                }
            }
        }

        #[test]
        fn compression_display_zstd_dict() {
            assert_eq!(
                format!(
                    "{}",
                    CompressionType::ZstdDict {
                        level: 3,
                        dict_id: 42
                    }
                ),
                "zstd+dict"
            );
        }

        #[test]
        fn compression_zstd_dict_rejects_invalid_level() {
            for invalid_level in [0, 23, -1, 200] {
                let result = CompressionType::zstd_dict(invalid_level, 42);
                assert!(result.is_err(), "level {invalid_level} should be rejected");
            }
        }

        #[test]
        fn compression_zstd_dict_decode_rejects_invalid_level() {
            // Serialize a valid ZstdDict, then corrupt the level byte to 0
            let mut buf = CompressionType::ZstdDict {
                level: 3,
                dict_id: 42,
            }
            .encode_into_vec();
            assert_eq!(buf[0], 4); // tag
            buf[1] = 0; // corrupt level to 0 (out of range 1..=22)

            let result = CompressionType::decode_from(&mut &buf[..]);
            assert!(result.is_err(), "level 0 should be rejected on decode");
        }

        #[test]
        fn zstd_dictionary_id_deterministic() {
            let dict_bytes = b"sample dictionary content for testing";
            let d1 = ZstdDictionary::new(dict_bytes);
            let d2 = ZstdDictionary::new(dict_bytes);
            assert_eq!(d1.id(), d2.id());
        }

        #[test]
        fn zstd_dictionary_different_content_different_id() {
            let d1 = ZstdDictionary::new(b"dictionary one");
            let d2 = ZstdDictionary::new(b"dictionary two");
            assert_ne!(d1.id(), d2.id());
        }

        #[test]
        fn zstd_dictionary_raw_roundtrip() {
            let raw = b"my dictionary bytes";
            let dict = ZstdDictionary::new(raw);
            assert_eq!(dict.raw(), raw);
        }

        #[test]
        fn zstd_dictionary_debug_format() {
            let dict = ZstdDictionary::new(b"test");
            let debug = format!("{dict:?}");
            assert!(debug.contains("ZstdDictionary"));
            assert!(debug.contains("size: 4"));
        }
    }
}
