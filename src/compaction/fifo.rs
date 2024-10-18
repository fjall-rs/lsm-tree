// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{config::Config, level_manifest::LevelManifest, time::unix_timestamp, HashSet};

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest segment(s)
/// when the threshold is reached.
///
/// Will also merge segments if the amount of segments in level 0 grows too much, which
/// could cause write stalls.
///
/// Additionally, a TTL can be configured to drop old segments.
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
    fn choose(&self, levels: &LevelManifest, config: &Config) -> Choice {
        let resolved_view = levels.resolved_view();

        // NOTE: First level always exists, trivial
        #[allow(clippy::expect_used)]
        let first_level = resolved_view.first().expect("L0 should always exist");

        let mut segment_ids_to_delete = HashSet::with_hasher(xxhash_rust::xxh3::Xxh3Builder::new());

        if let Some(ttl_seconds) = self.ttl_seconds {
            if ttl_seconds > 0 {
                let now = unix_timestamp().as_micros();

                for segment in first_level.iter() {
                    let lifetime_us = now - segment.metadata.created_at;
                    let lifetime_sec = lifetime_us / 1000 / 1000;

                    if lifetime_sec > ttl_seconds.into() {
                        log::trace!(
                            "segment is older than configured TTL: {:?}",
                            segment.metadata
                        );
                        segment_ids_to_delete.insert(segment.metadata.id);
                    }
                }
            }
        }

        let db_size = levels.size();

        if db_size > self.limit {
            let mut bytes_to_delete = db_size - self.limit;

            // NOTE: Sort the level by oldest to newest
            // levels are sorted from newest to oldest, so we can just reverse
            let mut first_level = first_level.clone();
            first_level.sort_by_seqno();
            first_level.segments.reverse();

            for segment in first_level.iter() {
                if bytes_to_delete == 0 {
                    break;
                }

                bytes_to_delete = bytes_to_delete.saturating_sub(segment.metadata.file_size);

                segment_ids_to_delete.insert(segment.metadata.id);

                log::trace!(
                    "dropping segment to reach configured size limit: {:?}",
                    segment.metadata
                );
            }
        }

        if segment_ids_to_delete.is_empty() {
            // NOTE: Only try to merge segments if they are not disjoint
            // to improve read performance
            // But ideally FIFO is only used for monotonic workloads
            // so there's nothing we need to do
            if first_level.is_disjoint {
                Choice::DoNothing
            } else {
                super::maintenance::Strategy.choose(levels, config)
            }
        } else {
            let mut ids = segment_ids_to_delete.into_iter().collect::<Vec<_>>();
            ids.sort_unstable();

            Choice::Drop(ids)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Strategy;
    use crate::{
        block_cache::BlockCache,
        compaction::{Choice, CompactionStrategy},
        config::Config,
        descriptor_table::FileDescriptorTable,
        file::LEVELS_MANIFEST_FILE,
        key_range::KeyRange,
        level_manifest::LevelManifest,
        segment::{
            block_index::two_level_index::TwoLevelBlockIndex,
            file_offsets::FileOffsets,
            meta::{Metadata, SegmentId},
            value_block::BlockOffset,
            Segment,
        },
        time::unix_timestamp,
    };
    use std::sync::Arc;
    use test_log::test;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    #[allow(clippy::expect_used)]
    #[allow(clippy::cast_possible_truncation)]
    fn fixture_segment(id: SegmentId, created_at: u128) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        Arc::new(Segment {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(TwoLevelBlockIndex::new((0, id).into(), block_cache.clone())),

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
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
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
            Choice::Drop(vec![1])
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
            Choice::Drop(vec![1, 2])
        );

        Ok(())
    }
}
