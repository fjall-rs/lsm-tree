// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, BlockHandle, DataBlock};
use crate::fs::FsFile;
use crate::{
    checksum::ChecksumType, coding::Decode, comparator::default_comparator,
    table::block::BlockType, CompressionType, KeyRange, SeqNo, TableId,
};
use byteorder::{LittleEndian, ReadBytesExt};
use std::ops::Deref;

/// Nanosecond timestamp.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Timestamp(u128);

impl Deref for Timestamp {
    type Target = u128;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Timestamp> for u128 {
    fn from(val: Timestamp) -> Self {
        val.0
    }
}

impl From<u128> for Timestamp {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Debug)]
pub struct ParsedMeta {
    pub id: TableId,
    pub created_at: Timestamp,
    pub data_block_count: u64,
    pub index_block_count: u64,
    pub key_range: KeyRange,
    pub(super) seqnos: (SeqNo, SeqNo),

    /// Highest seqno from KV entries only (excludes range tombstones).
    ///
    /// Falls back to `seqnos.1` (overall max) for tables written before
    /// this field was introduced, which is conservative but correct.
    pub(super) highest_kv_seqno: SeqNo,
    pub file_size: u64,
    pub item_count: u64,
    pub tombstone_count: u64,
    pub weak_tombstone_count: u64,
    pub weak_tombstone_reclaimable: u64,

    pub data_block_compression: CompressionType,
    pub index_block_compression: CompressionType,
}

macro_rules! read_u8 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)
            .unwrap_or_else(|| panic!("meta property {:?} should exist", $name));

        let mut bytes = &bytes.value[..];
        bytes.read_u8()?
    }};
}

macro_rules! read_u64 {
    ($block:expr, $name:expr, $cmp:expr) => {{
        let bytes = $block
            .point_read($name, SeqNo::MAX, $cmp)
            .unwrap_or_else(|| panic!("meta property {:?} should exist", $name));

        let mut bytes = &bytes.value[..];
        bytes.read_u64::<LittleEndian>()?
    }};
}

/// Validates that `kv_seqno` does not exceed `max_seqno`.
///
/// KV-only seqno must be ≤ overall max (which includes both KV and RT seqnos).
/// A value above `max_seqno` indicates on-disk corruption.
fn validated_kv_seqno(kv_seqno: SeqNo, max_seqno: SeqNo) -> crate::Result<SeqNo> {
    if kv_seqno > max_seqno {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "seqno#kv_max exceeds seqno#max",
        )
        .into());
    }
    Ok(kv_seqno)
}

impl ParsedMeta {
    #[expect(clippy::expect_used, clippy::too_many_lines)]
    pub fn load_with_handle(
        file: &dyn FsFile,
        handle: &BlockHandle,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
    ) -> crate::Result<Self> {
        let block = Block::from_file(
            file,
            *handle,
            CompressionType::None,
            encryption,
            #[cfg(zstd_any)]
            None,
        )?;

        if block.header.block_type != BlockType::Meta {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        let block = DataBlock::new(block);

        // Metadata keys are always lexicographic, so use the default comparator.
        let cmp = default_comparator();

        #[expect(clippy::indexing_slicing)]
        {
            let table_version = block
                .point_read(b"table_version", SeqNo::MAX, &cmp)
                .expect("Table version should exist")
                .value;

            assert_eq!(
                [3u8],
                &*table_version,
                "unspported table version {}",
                table_version[0],
            );
        }

        {
            let hash_type = block
                .point_read(b"filter_hash_type", SeqNo::MAX, &cmp)
                .expect("Filter hash type should exist")
                .value;

            assert_eq!(
                &[u8::from(ChecksumType::Xxh3)],
                &*hash_type,
                "invalid hash type: {:?}",
                std::str::from_utf8(&hash_type),
            );
        }

        {
            let hash_type = block
                .point_read(b"checksum_type", SeqNo::MAX, &cmp)
                .expect("Checksum type should exist")
                .value;

            assert_eq!(
                &[u8::from(ChecksumType::Xxh3)],
                &*hash_type,
                "invalid checksum type: {:?}",
                std::str::from_utf8(&hash_type),
            );
        }

        assert_eq!(
            read_u8!(block, b"restart_interval#index", &cmp),
            1,
            "index block restart intervals >1 are not supported for this version",
        );

        let id = read_u64!(block, b"table_id", &cmp);
        let item_count = read_u64!(block, b"item_count", &cmp);
        let tombstone_count = read_u64!(block, b"tombstone_count", &cmp);
        let data_block_count = read_u64!(block, b"block_count#data", &cmp);
        let index_block_count = read_u64!(block, b"block_count#index", &cmp);
        let _filter_block_count = read_u64!(block, b"block_count#filter", &cmp);
        let file_size = read_u64!(block, b"file_size", &cmp);
        let weak_tombstone_count = read_u64!(block, b"weak_tombstone_count", &cmp);
        let weak_tombstone_reclaimable = read_u64!(block, b"weak_tombstone_reclaimable", &cmp);

        let created_at = {
            let bytes = block
                .point_read(b"created_at", SeqNo::MAX, &cmp)
                .expect("created_at timestamp should exist");

            let mut bytes = &bytes.value[..];
            bytes.read_u128::<LittleEndian>()?.into()
        };

        let key_range = KeyRange::new((
            block
                .point_read(b"key#min", SeqNo::MAX, &cmp)
                .expect("key min should exist")
                .value,
            block
                .point_read(b"key#max", SeqNo::MAX, &cmp)
                .expect("key max should exist")
                .value,
        ));

        let seqnos = {
            let min = {
                let bytes = block
                    .point_read(b"seqno#min", SeqNo::MAX, &cmp)
                    .expect("seqno min should exist")
                    .value;
                let mut bytes = &bytes[..];
                bytes.read_u64::<LittleEndian>()?
            };

            let max = {
                let bytes = block
                    .point_read(b"seqno#max", SeqNo::MAX, &cmp)
                    .expect("seqno max should exist")
                    .value;
                let mut bytes = &bytes[..];
                bytes.read_u64::<LittleEndian>()?
            };

            (min, max)
        };

        // Optional field introduced for table-skip optimization.
        // Old tables lack this key; fall back to overall max seqno
        // (conservative: table-skip compares rt.seqno > highest_kv_seqno,
        // so falling back to the higher overall max just disables the
        // optimization for legacy tables — correct but not optimal).
        // If the key exists but is truncated, propagate the I/O error to
        // surface metadata corruption rather than silently falling back.
        let highest_kv_seqno =
            if let Some(item) = block.point_read(b"seqno#kv_max", SeqNo::MAX, &cmp) {
                let mut bytes = &item.value[..];
                validated_kv_seqno(bytes.read_u64::<LittleEndian>()?, seqnos.1)?
            } else {
                seqnos.1
            };

        let data_block_compression = {
            let bytes = block
                .point_read(b"compression#data", SeqNo::MAX, &cmp)
                .expect("size should exist");

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        let index_block_compression = {
            let bytes = block
                .point_read(b"compression#index", SeqNo::MAX, &cmp)
                .expect("size should exist");

            let mut bytes = &bytes.value[..];
            CompressionType::decode_from(&mut bytes)?
        };

        Ok(Self {
            id,
            created_at,
            data_block_count,
            index_block_count,
            key_range,
            seqnos,
            highest_kv_seqno,
            file_size,
            item_count,
            tombstone_count,
            weak_tombstone_count,
            weak_tombstone_reclaimable,
            data_block_compression,
            index_block_compression,
        })
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    reason = "test code"
)]
mod tests {
    use super::*;

    #[test]
    fn validated_kv_seqno_within_bounds() {
        assert_eq!(validated_kv_seqno(5, 10).unwrap(), 5);
    }

    #[test]
    fn validated_kv_seqno_equal_to_max() {
        assert_eq!(validated_kv_seqno(10, 10).unwrap(), 10);
    }

    #[test]
    fn validated_kv_seqno_zero() {
        assert_eq!(validated_kv_seqno(0, 10).unwrap(), 0);
    }

    #[test]
    fn validated_kv_seqno_exceeds_max_returns_error() {
        let err = validated_kv_seqno(11, 10).unwrap_err();
        assert!(matches!(err, crate::Error::Io(e) if e.kind() == std::io::ErrorKind::InvalidData));
    }
}
