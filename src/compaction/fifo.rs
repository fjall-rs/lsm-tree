// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{config::Config, level_manifest::LevelManifest, HashSet};

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest segment(s)
/// when the threshold is reached.
///
/// Will also merge segments if the number of segments in level 0 grows too much, which
/// could cause write stalls.
///
/// Additionally, a (lazy) TTL can be configured to drop old segments.
///
/// ###### Caution
///
/// Only use it for specific workloads where:
///
/// 1) You only want to store recent data (unimportant logs, ...)
/// 2) Your keyspace grows monotonically (e.g. time series)
/// 3) You only insert new data (no updates)
#[derive(Clone)]
pub struct Strategy {
    /// Data set size limit in bytes
    pub limit: u64,

    /// TTL in seconds, will be disabled if 0 or None
    pub ttl_seconds: Option<u64>,
}

impl Strategy {
    /// Configures a new `Fifo` compaction strategy
    #[must_use]
    pub fn new(limit: u64, ttl_seconds: Option<u64>) -> Self {
        Self { limit, ttl_seconds }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "FifoCompaction"
    }

    // TODO: TTL
    fn choose(&self, levels: &LevelManifest, _config: &Config) -> Choice {
        // NOTE: We always have at least one level
        #[allow(clippy::expect_used)]
        let first_level = levels.as_slice().first().expect("should have first level");

        assert!(first_level.is_disjoint(), "L0 needs to be disjoint");

        let l0_size = first_level.size();

        if l0_size > self.limit {
            let overshoot = l0_size - self.limit;

            let mut oldest_segments = HashSet::default();
            let mut collected_bytes = 0;

            for segment in first_level.iter().flat_map(|run| run.iter().rev()) {
                if collected_bytes >= overshoot {
                    break;
                }

                oldest_segments.insert(segment.id());
                collected_bytes += segment.file_size();
            }

            Choice::Drop(oldest_segments)
        } else {
            Choice::DoNothing
        }
    }
}

// TODO: restore tests
/*
#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        cache::Cache,
        compaction::{Choice, CompactionStrategy},
        config::Config,
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        level_manifest::LevelManifest,
        segment::{
            block::offset::BlockOffset,
            block_index::{two_level_index::TwoLevelBlockIndex, BlockIndexImpl},
            file_offsets::FileOffsets,
            meta::{Metadata, SegmentId},
            SegmentInner,
        },
        super_segment::Segment,
        time::unix_timestamp,
        HashSet, KeyRange,
    };
    use std::sync::{atomic::AtomicBool, Arc};
    use test_log::test;

    #[allow(clippy::expect_used)]
    #[allow(clippy::cast_possible_truncation)]
    fn fixture_segment(id: SegmentId, created_at: u128) -> Segment {
        todo!()

        /* let cache = Arc::new(Cache::with_capacity_bytes(10 * 1_024 * 1_024));

        let block_index = TwoLevelBlockIndex::new((0, id).into(), cache.clone());
        let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

        SegmentInner {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index,

            offsets: FileOffsets {
                bloom_ptr: BlockOffset(0),
                range_filter_ptr: BlockOffset(0),
                index_block_ptr: BlockOffset(0),
                metadata_ptr: BlockOffset(0),
                range_tombstones_ptr: BlockOffset(0),
                tli_ptr: BlockOffset(0),
                pfx_ptr: BlockOffset(0),
            },

            metadata: Metadata {
                data_block_count: 0,
                index_block_count: 0,
                data_block_size: 4_096,
                index_block_size: 4_096,
                created_at,
                id,
                file_size: 1,
                compression: crate::segment::meta::CompressionType::None,
                table_type: crate::segment::meta::TableType::Block,
                item_count: 0,
                key_count: 0,
                key_range: KeyRange::new((vec![].into(), vec![].into())),
                tombstone_count: 0,
                range_tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, created_at as u64),
            },
            cache,

            bloom_filter: Some(crate::bloom::BloomFilter::with_fp_rate(1, 0.1)),

            path: "a".into(),
            is_deleted: AtomicBool::default(),
        }
        .into() */
    }

    #[test]
    fn fifo_ttl() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(u64::MAX, Some(5_000));

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment(1, 1));
        levels.add(fixture_segment(2, unix_timestamp().as_micros()));

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Drop(set![1])
        );

        Ok(())
    }

    #[test]
    fn fifo_empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(1, None);

        let levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn fifo_below_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(4, None);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        levels.add(fixture_segment(1, 1));
        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment(2, 2));
        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment(3, 3));
        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        levels.add(fixture_segment(4, 4));
        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn fifo_more_than_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::new(2, None);

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        levels.add(fixture_segment(1, 1));
        levels.add(fixture_segment(2, 2));
        levels.add(fixture_segment(3, 3));
        levels.add(fixture_segment(4, 4));

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Drop([1, 2].into_iter().collect::<HashSet<_>>())
        );

        Ok(())
    }
}
 */
