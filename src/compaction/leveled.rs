// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    config::Config,
    level_manifest::{hidden_set::HiddenSet, LevelManifest},
    segment::Segment,
    version::{run::Ranged, Run},
    windows::{GrowingWindowsExt, ShrinkingWindowsExt},
    HashSet, KeyRange, SegmentId,
};

pub fn aggregate_run_key_range(segments: &[Segment]) -> KeyRange {
    let lo = segments.first().expect("run should never be empty");
    let hi = segments.last().expect("run should never be empty");
    KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
}

/// Tries to find the most optimal compaction set from
/// one level into the other.
fn pick_minimal_compaction(
    curr_run: &Run<Segment>,
    next_run: Option<&Run<Segment>>,
    hidden_set: &HiddenSet,
    overshoot: u64,
) -> Option<(HashSet<SegmentId>, bool)> {
    struct Choice {
        write_amp: f32,
        segment_ids: HashSet<SegmentId>,
        can_trivial_move: bool,
    }

    let mut choices = vec![];

    let mut add_choice = |choice: Choice| {
        let valid_choice = if hidden_set.is_blocked(choice.segment_ids.iter().copied()) {
            // IMPORTANT: Compaction is blocked because of other
            // on-going compaction
            false
        } else if choice.can_trivial_move {
            true
        } else {
            // TODO: this should not consider the number of segments, but the amount of rewritten data
            // which corresponds to the amount of temporary space amp

            // NOTE: Keep compactions with N or less segments
            // to make compactions not too large
            //
            // This value is currently manually fine-tuned based on benchmarks
            // with 50%/50% read-write workload
            //
            // Making compactions too granular heavily increases read tail latencies
            choice.segment_ids.len() < 100
            // true
        };

        if valid_choice {
            choices.push(choice);
        }
    };

    if let Some(next_run) = &next_run {
        for window in next_run.growing_windows() {
            if hidden_set.is_blocked(window.iter().map(Segment::id)) {
                // IMPORTANT: Compaction is blocked because of other
                // on-going compaction
                continue;
            }

            let key_range = aggregate_run_key_range(window);

            // Pull in all segments in current level into compaction
            let curr_level_pull_in: Vec<_> = curr_run.get_contained(&key_range).collect();

            if hidden_set.is_blocked(curr_level_pull_in.iter().map(|x| x.id())) {
                // IMPORTANT: Compaction is blocked because of other
                // on-going compaction
                continue;
            }

            let curr_level_size = curr_level_pull_in
                .iter()
                .map(|x| x.metadata.file_size)
                .sum::<u64>();

            // NOTE: Only consider compactions where we actually reach the amount
            // of bytes we need to merge?
            if curr_level_size >= overshoot {
                let next_level_size = window.iter().map(|x| x.metadata.file_size).sum::<u64>();

                let mut segment_ids: HashSet<_> = window.iter().map(Segment::id).collect();
                segment_ids.extend(curr_level_pull_in.iter().map(|x| x.id()));

                let write_amp = (next_level_size as f32) / (curr_level_size as f32);

                add_choice(Choice {
                    write_amp,
                    segment_ids,
                    can_trivial_move: false,
                });
            }
        }
    }

    // NOTE: Find largest trivial move (if it exists)
    for window in curr_run.shrinking_windows() {
        let key_range = aggregate_run_key_range(window);

        if let Some(next_run) = &next_run {
            if next_run.get_overlapping(&key_range).next().is_none() {
                add_choice(Choice {
                    write_amp: 0.0,
                    segment_ids: window.iter().map(Segment::id).collect(),
                    can_trivial_move: true,
                });
                break;
            }
        } else {
            add_choice(Choice {
                write_amp: 0.0,
                segment_ids: window.iter().map(Segment::id).collect(),
                can_trivial_move: true,
            });
            break;
        }
    }

    let minimum_effort_choice = choices.into_iter().min_by(|a, b| {
        a.write_amp
            .partial_cmp(&b.write_amp)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    minimum_effort_choice.map(|c| (c.segment_ids, c.can_trivial_move))
}

/// Levelled compaction strategy (LCS)
///
/// When a level reaches some threshold size, parts of it are merged into overlapping segments in the next level.
///
/// Each level Ln for n >= 2 can have up to `level_base_size * ratio^n` segments.
///
/// LCS suffers from comparatively high write amplification, but has decent read amplification and great space amplification (~1.1x).
///
/// LCS is the recommended compaction strategy to use.
///
/// More info here: <https://fjall-rs.github.io/post/lsm-leveling/>
#[derive(Clone)]
pub struct Strategy {
    /// When the number of segments in L0 reaches this threshold,
    /// they are merged into L1.
    ///
    /// Default = 4
    ///
    /// Same as `level0_file_num_compaction_trigger` in `RocksDB`.
    pub l0_threshold: u8,

    /// The target segment size as disk (possibly compressed).
    ///
    /// Default = 64 MiB
    ///
    /// Same as `target_file_size_base` in `RocksDB`.
    pub target_size: u32,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate)
    ///
    /// This is the exponential growth of the from one.
    /// level to the next
    ///
    /// A level target size is: max_memtable_size * level_ratio.pow(#level + 1).
    #[allow(clippy::doc_markdown)]
    pub level_ratio: u8,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            target_size:/* 64 Mib */ 64 * 1_024 * 1_024,
            level_ratio: 10,
        }
    }
}

impl Strategy {
    /// Calculates the level target size.
    ///
    /// L1 = `level_base_size`
    ///
    /// L2 = `level_base_size * ratio`
    ///
    /// L3 = `level_base_size * ratio * ratio`
    /// ...
    fn level_target_size(&self, level_idx: u8) -> u64 {
        assert!(level_idx >= 1, "level_target_size does not apply to L0");

        let power = (self.level_ratio as usize).pow(u32::from(level_idx) - 1);

        (power * (self.level_base_size() as usize)) as u64
    }

    fn level_base_size(&self) -> u64 {
        u64::from(self.target_size) * u64::from(self.l0_threshold)
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "LeveledStrategy"
    }

    #[allow(clippy::too_many_lines)]
    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        // L1+ compactions
        for (curr_level_index, level) in levels
            .as_slice()
            .iter()
            .enumerate()
            .skip(1)
            .take(usize::from(levels.level_count() - 2))
            .rev()
        {
            // NOTE: Level count is 255 max
            #[allow(clippy::cast_possible_truncation)]
            let curr_level_index = curr_level_index as u8;

            let next_level_index = curr_level_index + 1;

            if level.is_empty() {
                continue;
            }

            let level_size: u64 = level
                .iter()
                .flat_map(|x| x.iter())
                // NOTE: Take bytes that are already being compacted into account,
                // otherwise we may be overcompensating
                .filter(|x| !levels.hidden_set().is_hidden(x.id()))
                .map(|x| x.metadata.file_size)
                .sum();

            let desired_bytes = self.level_target_size(curr_level_index);

            let overshoot = level_size.saturating_sub(desired_bytes);

            if overshoot > 0 {
                let Some(next_level) = levels.current_version().level(next_level_index as usize)
                else {
                    break;
                };

                let Some((segment_ids, can_trivial_move)) = pick_minimal_compaction(
                    level.first_run().expect("should have exactly one run"),
                    next_level.first_run().map(std::ops::Deref::deref),
                    levels.hidden_set(),
                    overshoot,
                ) else {
                    break;
                };

                let choice = CompactionInput {
                    segment_ids,
                    dest_level: next_level_index,
                    target_size: u64::from(self.target_size),
                };

                eprintln!(
                    "{} {} segments, L{}->L{next_level_index}: {:?}",
                    if can_trivial_move { "move" } else { "merge" },
                    choice.segment_ids.len(),
                    next_level_index - 1,
                    choice.segment_ids,
                );

                if can_trivial_move && level.is_disjoint() {
                    return Choice::Move(choice);
                }
                return Choice::Merge(choice);
            }
        }

        // L0->L1 compactions
        {
            let busy_levels = levels.busy_levels();

            let Some(first_level) = levels.current_version().level(0) else {
                return Choice::DoNothing;
            };

            if busy_levels.contains(&0) {
                return Choice::DoNothing;
            }

            if first_level.len() >= self.l0_threshold.into() {
                // let first_level_size = first_level.size();

                /* // NOTE: Special handling for disjoint workloads
                if levels.is_disjoint() && first_level_size < self.target_size.into() {
                    // TODO: also do this in non-disjoint workloads
                    // -> intra-L0 compaction

                    // NOTE: Force a merge into L0 itself
                    // ...we seem to have *very* small flushes
                    return if first_level.len() >= 30 {
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

                if first_level_size < self.target_size.into() {
                    // NOTE: We reached the threshold, but L0 is still very small
                    // meaning we have very small segments, so do intra-L0 compaction
                    return Choice::Merge(CompactionInput {
                        dest_level: 0,
                        segment_ids: first_level.list_ids(),
                        target_size: self.target_size.into(),
                    });
                } */

                if !busy_levels.contains(&1) {
                    let Some(next_level) = &levels.current_version().level(1) else {
                        return Choice::DoNothing;
                    };

                    let mut segment_ids: HashSet<u64> = first_level.list_ids();

                    let key_range = first_level.aggregate_key_range();

                    // Get overlapping segments in next level
                    let next_level_overlapping_segment_ids: Vec<_> = next_level
                        .iter()
                        .flat_map(|run| run.get_overlapping(&key_range))
                        .map(Segment::id)
                        .collect();

                    segment_ids.extend(&next_level_overlapping_segment_ids);

                    let choice = CompactionInput {
                        segment_ids,
                        dest_level: 1,
                        target_size: u64::from(self.target_size),
                    };

                    eprintln!(
                        "merge {} segments, L0->L1: {:?}",
                        choice.segment_ids.len(),
                        choice.segment_ids,
                    );

                    if next_level_overlapping_segment_ids.is_empty() && first_level.is_disjoint() {
                        return Choice::Move(choice);
                    }
                    return Choice::Merge(choice);
                }
            }
        }

        Choice::DoNothing
    }
}
/*
#[cfg(test)]
mod tests {
    use super::{Choice, Strategy};
    use crate::{
        cache::Cache,
        compaction::{CompactionStrategy, Input as CompactionInput},
        descriptor_table::FileDescriptorTable,
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
        Config, HashSet, KeyRange,
    };
    use std::{
        path::Path,
        sync::{atomic::AtomicBool, Arc},
    };
    use test_log::test;

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
    ) -> Segment {
        todo!()

        /*   let cache = Arc::new(Cache::with_capacity_bytes(10 * 1_024 * 1_024));

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
            cache,

            bloom_filter: Some(crate::bloom::BloomFilter::with_fp_rate(1, 0.1)),

            path: "a".into(),
            is_deleted: AtomicBool::default(),
        }
        .into() */
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
    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation
    )]
    fn leveled_intra_l0() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };

        #[rustfmt::skip]
        let mut levels = build_levels(tempdir.path(), vec![
            vec![(1, "a", "z", 1), (2, "a", "z", 1), (3, "a", "z", 1), (4, "a", "z", 1)],
            vec![],
            vec![],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &Config::default()),
            Choice::Merge(CompactionInput {
                dest_level: 0,
                segment_ids: [1, 2, 3, 4].into_iter().collect::<HashSet<_>>(),
                target_size: u64::from(compactor.target_size),
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
    fn levelled_from_tiered() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;
        let compactor = Strategy {
            target_size: 64 * 1_024 * 1_024,
            ..Default::default()
        };
        let config = Config::default();

        #[rustfmt::skip]
        let levels = build_levels(tempdir.path(), vec![
            vec![],
            vec![(1, "a", "z", 64), (2, "a", "z", 64), (3, "g", "z", 64), (5, "g", "z", 64), (6, "g", "z", 64)],
            vec![(4, "a", "g", 64)],
            vec![],
        ])?;

        assert_eq!(
            compactor.choose(&levels, &config),
            Choice::Merge(CompactionInput {
                dest_level: 2,
                segment_ids: [1, 2, 3, 4, 5, 6].into_iter().collect::<HashSet<_>>(),
                target_size: 64 * 1_024 * 1_024
            })
        );

        Ok(())
    }
}
 */
