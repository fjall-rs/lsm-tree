// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    config::Config, key_range::KeyRange, level_manifest::LevelManifest, segment::Segment, HashSet,
};
use std::sync::Arc;

/// Levelled compaction strategy (LCS)
///
/// If a level reaches some threshold size, parts of it are merged into overlapping segments in the next level.
///
/// Each level Ln for n >= 1 can have up to ratio^n segments.
///
/// LCS suffers from comparatively high write amplification, but has decent read & space amplification.
///
/// LCS is the recommended compaction strategy to use.
///
/// More info here: <https://fjall-rs.github.io/post/lsm-leveling/>
#[derive(Clone)]
pub struct Strategy {
    /// When the number of segments in L0 reaches this threshold,
    /// they are merged into L1
    ///
    /// Default = 4
    ///
    /// Same as `level0_file_num_compaction_trigger` in `RocksDB`
    pub l0_threshold: u8,

    /// Target segment size (compressed)
    ///
    /// Default = 64 MiB
    ///
    /// Same as `target_file_size_base` in `RocksDB`
    pub target_size: u32,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate)
    ///
    /// This is the exponential growth of the from one.
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1)
    #[allow(clippy::doc_markdown)]
    pub level_ratio: u8,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 8,
        }
    }
}

fn aggregate_key_range(segments: &[Arc<Segment>]) -> KeyRange {
    KeyRange::aggregate(segments.iter().map(|x| &x.metadata.key_range))
}

fn desired_level_size_in_bytes(level_idx: u8, ratio: u8, target_size: u32) -> usize {
    (ratio as usize).pow(u32::from(level_idx)) * (target_size as usize)
}

impl CompactionStrategy for Strategy {
    #[allow(clippy::too_many_lines)]
    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        let resolved_view = levels.resolved_view();

        // If there are any levels that already have a compactor working on it
        // we can't touch those, because that could cause a race condition
        // violating the leveled compaction invariance of having a single sorted
        // run per level
        //
        // TODO: However, this can probably improved by checking two compaction
        // workers just don't cross key ranges
        let busy_levels = levels.busy_levels();

        for (curr_level_index, level) in resolved_view
            .iter()
            .enumerate()
            .skip(1)
            .take(resolved_view.len() - 2)
            .rev()
        {
            // NOTE: Level count is 255 max
            #[allow(clippy::cast_possible_truncation)]
            let curr_level_index = curr_level_index as u8;

            let next_level_index = curr_level_index + 1;

            if level.is_empty() {
                continue;
            }

            if busy_levels.contains(&curr_level_index) || busy_levels.contains(&next_level_index) {
                continue;
            }

            let curr_level_bytes = level.size();

            let desired_bytes =
                desired_level_size_in_bytes(curr_level_index, self.level_ratio, self.target_size);

            let mut overshoot = curr_level_bytes.saturating_sub(desired_bytes as u64);

            if overshoot > 0 {
                let mut segments_to_compact = vec![];

                let mut level = level.clone();
                level.sort_by_key_range(); // TODO: disjoint levels shouldn't need sort

                for segment in level.iter().take(self.level_ratio.into()).cloned() {
                    if overshoot == 0 {
                        break;
                    }

                    overshoot = overshoot.saturating_sub(segment.metadata.file_size);
                    segments_to_compact.push(segment);
                }

                let Some(next_level) = &resolved_view.get(next_level_index as usize) else {
                    break;
                };

                let mut segment_ids: HashSet<u64> =
                    segments_to_compact.iter().map(|x| x.metadata.id).collect();

                // Get overlapping segments in same level
                let key_range = aggregate_key_range(&segments_to_compact);

                let curr_level_overlapping_segment_ids: Vec<_> = level
                    .overlapping_segments(&key_range)
                    .map(|x| x.metadata.id)
                    .collect();

                segment_ids.extend(&curr_level_overlapping_segment_ids);

                // Get overlapping segments in next level
                let key_range = aggregate_key_range(&segments_to_compact);

                let next_level_overlapping_segment_ids: Vec<_> = next_level
                    .overlapping_segments(&key_range)
                    .map(|x| x.metadata.id)
                    .collect();

                segment_ids.extend(&next_level_overlapping_segment_ids);

                let choice = CompactionInput {
                    segment_ids: {
                        let mut v = segment_ids.into_iter().collect::<Vec<_>>();
                        v.sort_unstable();
                        v
                    },
                    dest_level: next_level_index,
                    target_size: u64::from(self.target_size),
                };

                if next_level_overlapping_segment_ids.is_empty() && level.is_disjoint {
                    return Choice::Move(choice);
                }
                return Choice::Merge(choice);
            }
        }

        {
            let Some(first_level) = resolved_view.first() else {
                return Choice::DoNothing;
            };

            if first_level.len() >= self.l0_threshold.into()
                && !busy_levels.contains(&0)
                && !busy_levels.contains(&1)
            {
                let mut level = first_level.clone();
                level.sort_by_key_range(); // TODO: disjoint levels shouldn't need sort

                let Some(next_level) = &resolved_view.get(1) else {
                    return Choice::DoNothing;
                };

                let mut segment_ids: Vec<u64> =
                    level.iter().map(|x| x.metadata.id).collect::<Vec<_>>();

                // Get overlapping segments in next level
                let key_range = aggregate_key_range(&level);

                let next_level_overlapping_segment_ids: Vec<_> = next_level
                    .overlapping_segments(&key_range)
                    .map(|x| x.metadata.id)
                    .collect();

                segment_ids.extend(&next_level_overlapping_segment_ids);

                let choice = CompactionInput {
                    segment_ids,
                    dest_level: 1,
                    target_size: u64::from(self.target_size),
                };

                if next_level_overlapping_segment_ids.is_empty() && level.is_disjoint {
                    return Choice::Move(choice);
                }
                return Choice::Merge(choice);
            }
        }

        Choice::DoNothing
    }
}

#[cfg(test)]
mod tests {
    use super::{Choice, Strategy};
    use crate::{
        block_cache::BlockCache,
        compaction::{CompactionStrategy, Input as CompactionInput},
        descriptor_table::FileDescriptorTable,
        key_range::KeyRange,
        level_manifest::LevelManifest,
        segment::{
            block_index::two_level_index::TwoLevelBlockIndex,
            file_offsets::FileOffsets,
            meta::{Metadata, SegmentId},
            Segment,
        },
        time::unix_timestamp,
        Config,
    };
    use std::{path::Path, sync::Arc};
    use test_log::test;

    #[cfg(feature = "bloom")]
    use crate::bloom::BloomFilter;

    fn string_key_range(a: &str, b: &str) -> KeyRange {
        KeyRange::new((a.as_bytes().into(), b.as_bytes().into()))
    }

    #[allow(
        clippy::expect_used,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn fixture_segment(
        id: SegmentId,
        key_range: KeyRange,
        size: u64,
        tombstone_ratio: f32,
    ) -> Arc<Segment> {
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));

        Arc::new(Segment {
            tree_id: 0,
            descriptor_table: Arc::new(FileDescriptorTable::new(512, 1)),
            block_index: Arc::new(TwoLevelBlockIndex::new((0, id).into(), block_cache.clone())),

            offsets: FileOffsets {
                bloom_ptr: 0,
                range_filter_ptr: 0,
                index_block_ptr: 0,
                metadata_ptr: 0,
                range_tombstones_ptr: 0,
                tli_ptr: 0,
                pfx_ptr: 0,
            },

            metadata: Metadata {
                data_block_count: 0,
                index_block_count: 0,
                data_block_size: 4_096,
                index_block_size: 4_096,
                created_at: unix_timestamp().as_nanos(),
                id,
                file_size: size,
                compression: crate::segment::meta::CompressionType::None,
                table_type: crate::segment::meta::TableType::Block,
                item_count: 1_000_000,
                key_count: 0,
                key_range,
                tombstone_count: (1_000_000.0 * tombstone_ratio) as u64,
                range_tombstone_count: 0,
                uncompressed_size: 0,
                seqnos: (0, 0),
            },
            block_cache,

            #[cfg(feature = "bloom")]
            bloom_filter: BloomFilter::with_fp_rate(1, 0.1),
        })
    }

    #[allow(clippy::expect_used)]
    fn build_levels(
        path: &Path,
        recipe: Vec<Vec<(SegmentId, &str, &str)>>,
    ) -> crate::Result<LevelManifest> {
        let mut levels = LevelManifest::create_new(
            recipe.len().try_into().expect("oopsie"),
            path.join("levels"),
        )?;

        for (idx, level) in recipe.into_iter().enumerate() {
            for (id, min, max) in level {
                levels.insert_into_level(
                    idx.try_into().expect("oopsie"),
                    fixture_segment(id, string_key_range(min, max), 64 * 1_024 * 1_024, 0.0),
                );
            }
        }

        Ok(levels)
    }

    #[test]
    fn leveled_empty_levels() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn leveled_default_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let mut levels = build_levels(tempdir.path(), vec![
            vec![(1, "a", "z"), (2, "a", "z"), (3, "a", "z"), (4, "a", "z")],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: vec![1, 2, 3, 4],
                target_size: 64 * 1_024 * 1_024
            })
        );

        levels.hide_segments(&[4]);

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn leveled_more_than_min_no_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![(1, "h", "t"), (2, "h", "t"), (3, "h", "t"), (4, "h", "t")],
            vec![(5, "a", "g"), (6, "a", "g"), (7, "a", "g"), (8, "a", "g")],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: vec![1, 2, 3, 4],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn leveled_more_than_min_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let mut levels = build_levels(tempdir.path(), vec![
            vec![(1, "a", "g"), (2, "h", "t"), (3, "i", "t"), (4, "j", "t")],
            vec![(5, "a", "g"), (6, "a", "g"), (7, "y", "z"), (8, "y", "z")],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: vec![1, 2, 3, 4, 5, 6],
                target_size: 64 * 1_024 * 1_024
            })
        );

        levels.hide_segments(&[5]);
        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::DoNothing
        );

        Ok(())
    }

    #[test]
    fn leveled_deeper_level_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 2,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![(1, "a", "g"), (2, "h", "t"), (3, "x", "z")],
            vec![(4, "f", "l")],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: vec![1, 4],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn leveled_deeper_level_no_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 2,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![(1, "a", "g"), (2, "h", "j"), (3, "k", "t")],
            vec![(4, "k", "l")],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Move(CompactionInput {
                dest_level: 2,
                segment_ids: vec![1],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn leveled_last_level_with_overlap() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 2,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![],
            vec![(1, "a", "g"), (2, "a", "g"), (3, "a", "g"), (4, "a", "g"), (5, "y", "z")],
            vec![(6, "f", "l")],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 3,
                segment_ids: vec![1, 2, 3, 4, 6],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn levelled_last_level_with_overlap_invariant() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 2,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![],
            vec![(1, "a", "g"), (2, "h", "j"), (3, "k", "l"), (4, "m", "n"), (5, "y", "z")],
            vec![(6, "f", "l")],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 3,
                segment_ids: vec![1, 6],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn levelled_last_level_without_overlap_invariant() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 2,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![],
            vec![(1, "a", "g"), (2, "h", "j"), (3, "k", "l"), (4, "m", "n"), (5, "y", "z")],
            vec![(6, "w", "x")],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Move(CompactionInput {
                dest_level: 3,
                segment_ids: vec![1],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }

    #[test]
    fn levelled_from_tiered() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            level_ratio: 2,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![(1, "a", "z"), (2, "a", "z"), (3, "g", "z")],
            vec![(4, "a", "g")],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: vec![1, 2, 3, 4],
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }
}
