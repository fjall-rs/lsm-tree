// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    config::Config,
    key_range::KeyRange,
    level_manifest::{level::Level, LevelManifest},
    segment::Segment,
    HashSet, SegmentId,
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
            level_ratio: 8, // TODO: benchmark vs 10
        }
    }
}

fn aggregate_key_range(segments: &[Arc<Segment>]) -> KeyRange {
    KeyRange::aggregate(segments.iter().map(|x| &x.metadata.key_range))
}

fn desired_level_size_in_bytes(level_idx: u8, ratio: u8, target_size: u32) -> usize {
    (ratio as usize).pow(u32::from(level_idx)) * (target_size as usize)
}

fn pick_minimal_overlap(
    curr_level: &Level,
    next_level: &Level,
    overshoot: u64,
) -> (HashSet<SegmentId>, bool) {
    let mut choices = vec![];

    for size in 1..=curr_level.len() {
        let windows = curr_level.windows(size);

        for window in windows {
            let size_sum = window.iter().map(|x| x.metadata.file_size).sum::<u64>();

            if size_sum >= overshoot {
                // NOTE: Consider this window

                let mut segment_ids: HashSet<SegmentId> =
                    window.iter().map(|x| x.metadata.id).collect();

                // Get overlapping segments in next level
                let key_range = aggregate_key_range(window);

                let next_level_overlapping_segments: Vec<_> = next_level
                    .overlapping_segments(&key_range)
                    .cloned()
                    .collect();

                // Get overlapping segments in same level
                let key_range = aggregate_key_range(&next_level_overlapping_segments);

                let curr_level_overlapping_segment_ids: Vec<_> = curr_level
                    .overlapping_segments(&key_range)
                    .filter(|x| !segment_ids.contains(&x.metadata.id))
                    .collect();

                // Calculate effort
                let size_next_level = next_level_overlapping_segments
                    .iter()
                    .map(|x| x.metadata.file_size)
                    .sum::<u64>();

                let size_curr_level = curr_level_overlapping_segment_ids
                    .iter()
                    .map(|x| x.metadata.file_size)
                    .sum::<u64>();

                let effort = size_sum + size_next_level + size_curr_level;

                segment_ids.extend(
                    next_level_overlapping_segments
                        .iter()
                        .map(|x| x.metadata.id),
                );

                segment_ids.extend(
                    curr_level_overlapping_segment_ids
                        .iter()
                        .map(|x| x.metadata.id),
                );

                // TODO: need to calculate write_amp and choose minimum write_amp instead
                //
                // consider the segments in La = A to be the ones in the window
                // and the segments in La+1 B to be the ones that overlap
                // and r = A / B
                // we want to avoid compactions that have a low ratio r
                // because that means we don't clear out a lot of segments in La
                // but have to rewrite a lot of segments in La+1
                //
                // ultimately, we want the highest ratio
                // to maximize the amount of segments we are getting rid of in La
                // for the least amount of effort
                choices.push((
                    effort,
                    segment_ids,
                    next_level_overlapping_segments.is_empty(),
                ));
            }
        }
    }

    let minimum_effort_choice = choices.into_iter().min_by(|a, b| a.0.cmp(&b.0));
    let (_, set, can_trivial_move) = minimum_effort_choice.expect("should exist");

    (set, can_trivial_move)
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

            let desired_bytes =
                desired_level_size_in_bytes(curr_level_index, self.level_ratio, self.target_size);

            let overshoot = level.size().saturating_sub(desired_bytes as u64);

            if overshoot > 0 {
                let Some(next_level) = &resolved_view.get(next_level_index as usize) else {
                    break;
                };

                let (segment_ids, can_trivial_move) =
                    pick_minimal_overlap(level, next_level, overshoot);

                let choice = CompactionInput {
                    segment_ids,
                    dest_level: next_level_index,
                    target_size: u64::from(self.target_size),
                };

                // TODO: eventually, this should happen lazily
                // if a segment file lives for very long, it should get rewritten
                // Rocks, by default, rewrites files that are 1 month or older
                //
                // TODO: 3.0.0 configuration?
                // NOTE: We purposefully not trivially move segments
                // if we go from L1 to L2
                // https://github.com/fjall-rs/lsm-tree/issues/63
                let goes_into_cold_storage = next_level_index == 2;

                if goes_into_cold_storage {
                    return Choice::Merge(choice);
                }

                if can_trivial_move && level.is_disjoint {
                    return Choice::Move(choice);
                }
                return Choice::Merge(choice);
            }
        }

        {
            let Some(first_level) = resolved_view.first() else {
                return Choice::DoNothing;
            };

            if first_level.len() >= self.l0_threshold.into() && !busy_levels.contains(&0) {
                let first_level_size = first_level.size();

                // NOTE: Special handling for disjoint workloads
                if levels.is_disjoint() {
                    if first_level_size < self.target_size.into() {
                        // TODO: also do this in non-disjoint workloads
                        // -> intra-L0 compaction

                        // NOTE: Force a merge into L0 itself
                        // ...we seem to have *very* small flushes
                        return if first_level.len() >= 32 {
                            Choice::Merge(CompactionInput {
                                dest_level: 0,
                                segment_ids: first_level.list_ids(),
                                // NOTE: Allow a bit of overshooting
                                target_size: ((self.target_size as f32) * 1.1) as u64,
                            })
                        } else {
                            Choice::DoNothing
                        };
                    }

                    return Choice::Merge(CompactionInput {
                        dest_level: 1,
                        segment_ids: first_level.list_ids(),
                        target_size: ((self.target_size as f32) * 1.1) as u64,
                    });
                }

                if first_level_size < self.target_size.into() {
                    // NOTE: We reached the threshold, but L0 is still very small
                    // meaning we have very small segments, so do intra-L0 compaction
                    return Choice::Merge(CompactionInput {
                        dest_level: 0,
                        segment_ids: first_level.list_ids(),
                        target_size: self.target_size.into(),
                    });
                }

                if !busy_levels.contains(&1) {
                    let mut level = first_level.clone();
                    level.sort_by_key_range();

                    let Some(next_level) = &resolved_view.get(1) else {
                        return Choice::DoNothing;
                    };

                    let mut segment_ids: HashSet<u64> =
                        level.iter().map(|x| x.metadata.id).collect();

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
            value_block::BlockOffset,
            Segment,
        },
        time::unix_timestamp,
        Config, HashSet,
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
        recipe: Vec<Vec<(SegmentId, &str, &str, u64)>>,
    ) -> crate::Result<LevelManifest> {
        let mut levels = LevelManifest::create_new(
            recipe.len().try_into().expect("oopsie"),
            path.join("levels"),
        )?;

        for (idx, level) in recipe.into_iter().enumerate() {
            for (id, min, max, size_mib) in level {
                levels.insert_into_level(
                    idx.try_into().expect("oopsie"),
                    fixture_segment(
                        id,
                        string_key_range(min, max),
                        size_mib * 1_024 * 1_024,
                        0.0,
                    ),
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
            vec![(1, "a", "z", 64), (2, "a", "z", 64), (3, "a", "z", 64), (4, "a", "z", 64)],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        levels.hide_segments(std::iter::once(4));

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
            vec![(1, "h", "t", 64), (2, "h", "t", 64), (3, "h", "t", 64), (4, "h", "t", 64)],
            vec![(5, "a", "g", 64), (6, "a", "g", 64), (7, "a", "g", 64), (8, "a", "g", 64)],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
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
            vec![(1, "a", "g", 64), (2, "h", "t", 64), (3, "i", "t", 64), (4, "j", "t", 64)],
            vec![(5, "a", "g", 64), (6, "a", "g", 64), (7, "y", "z", 64), (8, "y", "z", 64)],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 1,
                segment_ids: [1, 2, 3, 4, 5, 6].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        levels.hide_segments(std::iter::once(5));
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
            vec![(1, "a", "g", 64), (2, "h", "t", 64), (3, "x", "z", 64)],
            vec![(4, "f", "l", 64)],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: set![3],
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
            vec![(1, "a", "g", 64), (2, "h", "j", 64), (3, "k", "t", 64)],
            vec![(4, "k", "l", 64)],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            // NOTE: We merge because segments are demoted into "cold" levels
            // see https://github.com/fjall-rs/lsm-tree/issues/63
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: set![1],
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
            vec![(1, "a", "g", 64), (2, "a", "g", 64), (3, "a", "g", 64), (4, "a", "g", 64), (5, "y", "z", 64)],
            vec![(6, "f", "l", 64)],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 3,
                // NOTE: 5 is the only segment that has no overlap with #3
                segment_ids: set![5],
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
            vec![(1, "a", "g", 64), (2, "h", "j", 64), (3, "k", "l", 64), (4, "m", "n", 64), (5, "y", "z", 64)],
            vec![(6, "f", "l", 64)],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Move(CompactionInput {
                dest_level: 3,
                // NOTE: segment #4 is the left-most segment that has no overlap with L3
                segment_ids: set![4],
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
            vec![(1, "a", "g", 64), (2, "h", "j", 64), (3, "k", "l", 64), (4, "m", "n", 64), (5, "y", "z", 64)],
            vec![(6, "w", "x", 64)],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Move(CompactionInput {
                dest_level: 3,
                segment_ids: set![1],
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
            vec![(1, "a", "z", 64), (2, "a", "z", 64), (3, "g", "z", 64)],
            vec![(4, "a", "g", 64)],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }
}
