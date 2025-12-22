// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(test)]
mod test;

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    compaction::state::{hidden_set::HiddenSet, CompactionState},
    config::Config,
    slice_windows::{GrowingWindowsExt, ShrinkingWindowsExt},
    table::{util::aggregate_run_key_range, Table},
    version::{Run, Version},
    HashSet, TableId,
};

/// Tries to find the most optimal compaction set from one level into the other.
fn pick_minimal_compaction(
    curr_run: &Run<Table>,
    next_run: Option<&Run<Table>>,
    hidden_set: &HiddenSet,
    overshoot: u64,
    table_base_size: u64,
) -> Option<(HashSet<TableId>, bool)> {
    // NOTE: Find largest trivial move (if it exists)
    if let Some(window) = curr_run.shrinking_windows().find(|window| {
        if hidden_set.is_blocked(window.iter().map(Table::id)) {
            // IMPORTANT: Compaction is blocked because of other
            // on-going compaction
            return false;
        }

        let Some(next_run) = &next_run else {
            // No run in next level, so we can trivially move
            return true;
        };

        let key_range = aggregate_run_key_range(window);

        next_run.get_overlapping(&key_range).is_empty()
    }) {
        let ids = window.iter().map(Table::id).collect();
        return Some((ids, true));
    }

    // NOTE: Look for merges
    if let Some(next_run) = &next_run {
        next_run
            .growing_windows()
            .take_while(|window| {
                // Cap at 50x tables per compaction for now
                //
                // At this point, all compactions are too large anyway
                // so we can escape early
                let next_level_size = window.iter().map(Table::file_size).sum::<u64>();
                next_level_size <= (50 * table_base_size)
            })
            .filter_map(|window| {
                if hidden_set.is_blocked(window.iter().map(Table::id)) {
                    // IMPORTANT: Compaction is blocked because of other
                    // on-going compaction
                    return None;
                }

                let key_range = aggregate_run_key_range(window);

                // Pull in all contained tables in current level into compaction
                let curr_level_pull_in = curr_run.get_contained(&key_range);

                let curr_level_size = curr_level_pull_in.iter().map(Table::file_size).sum::<u64>();

                if curr_level_size == 0 {
                    return None;
                }

                // TODO: toggling this statement can deadlock compactions because if there are only larger-than-overshoot
                //  compactions, they would not be chosen
                // if curr_level_size < overshoot {
                //     return None;
                // }

                if hidden_set.is_blocked(curr_level_pull_in.iter().map(Table::id)) {
                    // IMPORTANT: Compaction is blocked because of other
                    // on-going compaction
                    return None;
                }

                let next_level_size = window.iter().map(Table::file_size).sum::<u64>();

                let compaction_bytes = curr_level_size + next_level_size;

                #[expect(clippy::cast_precision_loss)]
                let write_amp = (next_level_size as f32) / (curr_level_size as f32);

                Some((window, curr_level_pull_in, write_amp, compaction_bytes))
            })
            // Find the compaction with the smallest write set
            .min_by_key(|(_, _, _waf, bytes)| *bytes)
            .map(|(window, curr_level_pull_in, _, _)| {
                let mut ids: HashSet<_> = window.iter().map(Table::id).collect();
                ids.extend(curr_level_pull_in.iter().map(Table::id));
                (ids, false)
            })
    } else {
        None
    }
}

#[doc(hidden)]
pub const NAME: &str = "LeveledCompaction";

/// Leveled compaction strategy (LCS)
///
/// When a level reaches some threshold size, parts of it are merged into overlapping tables in the next level.
///
/// Each level Ln for n >= 2 can have up to `level_base_size * ratio^(n - 1)` tables.
///
/// LCS suffers from comparatively high write amplification, but has decent read amplification and great space amplification (~1.1x).
///
/// LCS is the recommended compaction strategy to use.
///
/// More info here: <https://fjall-rs.github.io/post/lsm-leveling/>
#[derive(Clone)]
pub struct Strategy {
    l0_threshold: u8,

    /// The target table size as disk (possibly compressed).
    target_size: u64,

    /// Size ratio between levels of the LSM tree (a.k.a fanout, growth rate)
    level_ratio_policy: Vec<f32>,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            target_size:/* 64 MiB */ 64 * 1_024 * 1_024,
            level_ratio_policy: vec![10.0],
        }
    }
}

impl Strategy {
    /// Sets the growth ratio between levels.
    ///
    /// Same as `set_max_bytes_for_level_multiplier` in `RocksDB`.
    ///
    /// Default = [10.0]
    #[must_use]
    pub fn with_level_ratio_policy(mut self, policy: Vec<f32>) -> Self {
        self.level_ratio_policy = policy;
        self
    }

    /// Sets the L0 threshold.
    ///
    /// When the number of tables in L0 reaches this threshold,
    /// they are merged into L1.
    ///
    /// Same as `level0_file_num_compaction_trigger` in `RocksDB`.
    ///
    /// Default = 4
    #[must_use]
    pub fn with_l0_threshold(mut self, threshold: u8) -> Self {
        self.l0_threshold = threshold;
        self
    }

    /// Sets the table target size on disk (possibly compressed).
    ///
    /// Same as `target_file_size_base` in `RocksDB`.
    ///
    /// Default = 64 MiB
    #[must_use]
    pub fn with_table_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }

    /// Calculates the size of L1.
    fn level_base_size(&self) -> u64 {
        self.target_size * u64::from(self.l0_threshold)
    }

    /// Calculates the level target size.
    ///
    /// L1 = `level_base_size`
    ///
    /// L2 = `level_base_size * ratio`
    ///
    /// L3 = `level_base_size * ratio * ratio`
    ///
    /// ...
    fn level_target_size(&self, canonical_level_idx: u8) -> u64 {
        assert!(
            canonical_level_idx >= 1,
            "level_target_size does not apply to L0",
        );

        if canonical_level_idx == 1 {
            // u64::from(self.target_size)
            self.level_base_size()
        } else {
            #[expect(
                clippy::cast_precision_loss,
                reason = "precision loss is acceptable for level size calculations"
            )]
            let mut size = self.level_base_size() as f32;

            // NOTE: Minus 2 because |{L0, L1}|
            for idx in 0..=(canonical_level_idx - 2) {
                let ratio = self
                    .level_ratio_policy
                    .get(usize::from(idx))
                    .copied()
                    .unwrap_or_else(|| self.level_ratio_policy.last().copied().unwrap_or(10.0));

                size *= ratio;
            }

            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "size is always positive and will never even come close to u64::MAX"
            )]
            {
                size as u64
            }
        }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn get_config(&self) -> Vec<crate::KvPair> {
        vec![
            (
                crate::UserKey::from("leveled_l0_threshold"),
                crate::UserValue::from(self.l0_threshold.to_le_bytes()),
            ),
            (
                crate::UserKey::from("leveled_target_size"),
                crate::UserValue::from(self.target_size.to_le_bytes()),
            ),
            (
                crate::UserKey::from("leveled_level_ratio_policy"),
                crate::UserValue::from({
                    use byteorder::{LittleEndian, WriteBytesExt};

                    let mut v = vec![];

                    #[expect(
                        clippy::expect_used,
                        clippy::cast_possible_truncation,
                        reason = "writing into Vec should not fail; policies have length of 255 max"
                    )]
                    v.write_u8(self.level_ratio_policy.len() as u8)
                        .expect("cannot fail");

                    for &f in &self.level_ratio_policy {
                        #[expect(clippy::expect_used, reason = "writing into Vec should not fail")]
                        v.write_f32::<LittleEndian>(f).expect("cannot fail");
                    }

                    v
                }),
            ),
        ]
    }

    #[expect(clippy::too_many_lines)]
    fn choose(&self, version: &Version, _: &Config, state: &CompactionState) -> Choice {
        assert!(version.level_count() == 7, "should have exactly 7 levels");

        // Find the level that corresponds to L1
        #[expect(clippy::map_unwrap_or)]
        let first_non_empty_level = version
            .iter_levels()
            .enumerate()
            .skip(1)
            .find(|(_, lvl)| !lvl.is_empty())
            .map(|(idx, _)| idx)
            .unwrap_or_else(|| version.level_count() - 1);

        let mut canonical_l1_idx = first_non_empty_level;

        // Number of levels we have to shift to get from the actual level idx to the canonical
        let mut level_shift = canonical_l1_idx - 1;

        if canonical_l1_idx > 1 && version.iter_levels().skip(1).any(|lvl| !lvl.is_empty()) {
            let need_new_l1 = version
                .iter_levels()
                .enumerate()
                .skip(1)
                .filter(|(_, lvl)| !lvl.is_empty())
                .all(|(idx, level)| {
                    let level_size = level
                        .iter()
                        .flat_map(|x| x.iter())
                        // NOTE: Take bytes that are already being compacted into account,
                        // otherwise we may be overcompensating
                        .filter(|x| !state.hidden_set().is_hidden(x.id()))
                        .map(Table::file_size)
                        .sum::<u64>();

                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "level index is bounded by level count (7, technically 255)"
                    )]
                    let target_size = self.level_target_size((idx - level_shift) as u8);

                    level_size > target_size
                });

            // Move up L1 one level if all current levels are at capacity
            if need_new_l1 {
                canonical_l1_idx -= 1;
                level_shift -= 1;
            }
        }

        // Trivial move into L1
        'trivial: {
            let first_level = version.l0();
            let target_level_idx = first_non_empty_level.min(canonical_l1_idx);

            if first_level.run_count() == 1 {
                if version.level_is_busy(0, state.hidden_set())
                    || version.level_is_busy(target_level_idx, state.hidden_set())
                {
                    break 'trivial;
                }

                let Some(target_level) = &version.level(target_level_idx) else {
                    break 'trivial;
                };

                if target_level.run_count() != 1 {
                    break 'trivial;
                }

                let key_range = first_level.aggregate_key_range();

                // Get overlapping tables in next level
                let get_overlapping = target_level
                    .iter()
                    .flat_map(|run| run.get_overlapping(&key_range))
                    .map(Table::id)
                    .next();

                if get_overlapping.is_none() && first_level.is_disjoint() {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "level index is bounded by level count (7)"
                    )]
                    return Choice::Move(CompactionInput {
                        table_ids: first_level.list_ids(),
                        dest_level: target_level_idx as u8,
                        canonical_level: 1,
                        target_size: self.target_size,
                    });
                }
            }
        }

        // Scoring
        let mut scores = [(/* score */ 0.0, /* overshoot */ 0u64); 7];

        {
            // TODO(weak-tombstone-rewrite): incorporate `Table::weak_tombstone_count` and
            // `Table::weak_tombstone_reclaimable` when computing level scores so rewrite
            // decisions can prioritize tables that would free the most reclaimable values.

            // Score first level
            let first_level = version.l0();

            if first_level.table_count() >= usize::from(self.l0_threshold) {
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "precision loss is acceptable for scoring calculations"
                )]
                let ratio = (first_level.table_count() as f64) / f64::from(self.l0_threshold);
                scores[0] = (ratio, 0);
            }

            // Score L1+
            for (idx, level) in version.iter_levels().enumerate().skip(1) {
                if level.is_empty() {
                    continue;
                }

                let level_size = level
                    .iter()
                    .flat_map(|x| x.iter())
                    // NOTE: Take bytes that are already being compacted into account,
                    // otherwise we may be overcompensating
                    .filter(|x| !state.hidden_set().is_hidden(x.id()))
                    .map(Table::file_size)
                    .sum::<u64>();

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "level index is bounded by level count (7, technically 255)"
                )]
                let target_size = self.level_target_size((idx - level_shift) as u8);

                // NOTE: We check for level length above
                #[expect(clippy::indexing_slicing)]
                if level_size > target_size {
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "precision loss is acceptable for scoring calculations"
                    )]
                    let score = level_size as f64 / target_size as f64;
                    scores[idx] = (score, level_size - target_size);

                    // NOTE: Force a trivial move
                    if version
                        .level(idx + 1)
                        .is_some_and(|next_level| next_level.is_empty())
                    {
                        scores[idx] = (99.99, 999);
                    }
                }
            }

            // NOTE: Never score Lmax
            {
                scores[6] = (0.0, 0);
            }
        }

        // Choose compaction
        #[expect(clippy::expect_used, reason = "highest score is expected to exist")]
        let (level_idx_with_highest_score, (score, overshoot_bytes)) = scores
            .into_iter()
            .enumerate()
            .max_by(|(_, (score_a, _)), (_, (score_b, _))| {
                score_a
                    .partial_cmp(score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .expect("should have highest score somewhere");

        if score < 1.0 {
            return Choice::DoNothing;
        }

        // We choose L0->L1 compaction
        if level_idx_with_highest_score == 0 {
            let Some(first_level) = version.level(0) else {
                return Choice::DoNothing;
            };

            if version.level_is_busy(0, state.hidden_set())
                || version.level_is_busy(canonical_l1_idx, state.hidden_set())
            {
                return Choice::DoNothing;
            }

            let Some(target_level) = &version.level(canonical_l1_idx) else {
                return Choice::DoNothing;
            };

            let mut table_ids = first_level.list_ids();

            let key_range = first_level.aggregate_key_range();

            // Get overlapping tables in next level
            let target_level_overlapping_table_ids: Vec<_> = target_level
                .iter()
                .flat_map(|run| run.get_overlapping(&key_range))
                .map(Table::id)
                .collect();

            table_ids.extend(&target_level_overlapping_table_ids);

            #[expect(
                clippy::cast_possible_truncation,
                reason = "level index is bounded by level count (7, technically 255)"
            )]
            let choice = CompactionInput {
                table_ids,
                dest_level: canonical_l1_idx as u8,
                canonical_level: 1,
                target_size: self.target_size,
            };

            if target_level_overlapping_table_ids.is_empty() && first_level.is_disjoint() {
                return Choice::Move(choice);
            }
            return Choice::Merge(choice);
        }

        // We choose L1+ compaction instead

        // NOTE: Level count is 255 max
        #[expect(clippy::cast_possible_truncation)]
        let curr_level_index = level_idx_with_highest_score as u8;

        let next_level_index = curr_level_index + 1;

        let Some(level) = version.level(level_idx_with_highest_score) else {
            return Choice::DoNothing;
        };

        let Some(next_level) = version.level(next_level_index as usize) else {
            return Choice::DoNothing;
        };

        debug_assert!(level.is_disjoint(), "level should be disjoint");
        debug_assert!(next_level.is_disjoint(), "next level should be disjoint");

        #[expect(
            clippy::expect_used,
            reason = "first run should exist because score is >0.0"
        )]
        let Some((table_ids, can_trivial_move)) = pick_minimal_compaction(
            level.first_run().expect("should have exactly one run"),
            next_level.first_run().map(std::ops::Deref::deref),
            state.hidden_set(),
            overshoot_bytes,
            self.target_size,
        ) else {
            return Choice::DoNothing;
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "level shift is bounded by level count (7, technically 255)"
        )]
        let choice = CompactionInput {
            table_ids,
            dest_level: next_level_index,
            canonical_level: next_level_index - (level_shift as u8),
            target_size: self.target_size,
        };

        if can_trivial_move && level.is_disjoint() {
            return Choice::Move(choice);
        }
        Choice::Merge(choice)
    }
}
