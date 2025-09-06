// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{config::Config, level_manifest::LevelManifest, segment::Segment, HashSet, SegmentId};

const L0_SEGMENT_CAP: usize = 20;

/// Maintenance compactor
///
/// This is a hidden compaction strategy that may be called by other strategies.
///
/// It cleans up L0 if it grows too large.
#[derive(Default)]
pub struct Strategy;

/// Choose a run of segments that has the least file size sum.
///
/// This minimizes the compaction time (+ write amp) for a set of segments we
/// want to partially compact.
pub fn choose_least_effort_compaction(segments: &[Segment], n: usize) -> HashSet<SegmentId> {
    let num_segments = segments.len();

    // Ensure that n is not greater than the number of segments
    assert!(
        n <= num_segments,
        "N must be less than or equal to the number of segments"
    );

    let windows = segments.windows(n);

    let window = windows
        .min_by_key(|window| window.iter().map(|s| s.file_size()).sum::<u64>())
        .expect("should have at least one window");

    window.iter().map(Segment::id).collect()
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "MaintenanceStrategy"
    }

    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        todo!()
    }
}
/*
#[cfg(test)]
mod tests {
    use super::*;
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
            meta::Metadata,
            SegmentInner,
        },
        super_segment::Segment,
        KeyRange,
    };
    use std::sync::{atomic::AtomicBool, Arc};
    use test_log::test;

    #[allow(clippy::expect_used)]
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
    fn maintenance_empty_level() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;

        let levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn maintenance_below_limit() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        for id in 0..5 {
            levels.add(fixture_segment(id, u128::from(id)));
        }

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn maintenance_l0_too_large() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy;

        let mut levels = LevelManifest::create_new(4, tempdir.path().join(LEVELS_MANIFEST_FILE))?;
        for id in 0..(L0_SEGMENT_CAP + 2) {
            levels.add(fixture_segment(id as u64, id as u128));
        }

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(crate::compaction::Input {
                dest_level: 0,
                segment_ids: [0, 1, 2].into_iter().collect::<HashSet<_>>(),
                target_size: u64::MAX
            })
        );

        Ok(())
    }
}
 */
