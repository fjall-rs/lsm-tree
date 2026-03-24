// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::unnecessary_map_or,
    reason = "test code"
)]
mod test;

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    compaction::state::{hidden_set::HiddenSet, CompactionState},
    config::Config,
    slice_windows::{GrowingWindowsExt, ShrinkingWindowsExt},
    table::{util::aggregate_run_key_range, Table},
    version::{run::Ranged, Level, Version},
    HashSet, TableId,
};

/// Tries to find the most optimal compaction set from one level into the other.
///
/// Scans all runs in both levels to handle transient multi-run states from
/// multi-level compaction (#108). See #122 Part 3.
fn pick_minimal_compaction(
    curr_level: &Level,
    next_level: &Level,
    hidden_set: &HiddenSet,
    _overshoot: u64,
    table_base_size: u64,
    cmp: &dyn crate::comparator::UserComparator,
) -> Option<(HashSet<TableId>, bool)> {
    // NOTE: Find largest trivial move (if it exists)
    // Check all runs in curr_level for a window that doesn't overlap ANY run
    // in next_level.
    for curr_run in curr_level.iter() {
        if let Some(window) = curr_run.shrinking_windows().find(|window| {
            if hidden_set.is_blocked(window.iter().map(Table::id)) {
                return false;
            }

            if next_level.is_empty() {
                return true;
            }

            let key_range = aggregate_run_key_range(window);

            // Must not overlap ANY run in the next level
            next_level
                .iter()
                .all(|run| run.get_overlapping_cmp(&key_range, cmp).is_empty())
        }) {
            let ids = window.iter().map(Table::id).collect();
            return Some((ids, true));
        }
    }

    // NOTE: Look for merges
    // Iterate windows across all runs in next_level, pull in from all runs
    // in curr_level.
    if next_level.is_empty() {
        return None;
    }

    next_level
        .iter()
        .flat_map(|run| {
            // Cap per-run windows at 50x table_base_size. take_while is safe
            // here because growing_windows within a single run are monotonically
            // increasing in size — once one exceeds the cap, all subsequent will too.
            run.growing_windows().take_while(|window| {
                let size = window.iter().map(Table::file_size).sum::<u64>();
                size <= (50 * table_base_size)
            })
        })
        .filter_map(|window| {
            if hidden_set.is_blocked(window.iter().map(Table::id)) {
                return None;
            }

            let key_range = aggregate_run_key_range(window);

            // Pull in contained tables from ALL runs in curr_level
            let curr_level_pull_in: Vec<&Table> = curr_level
                .iter()
                .flat_map(|run| run.get_contained_cmp(&key_range, cmp))
                .collect();

            let curr_level_size = curr_level_pull_in
                .iter()
                .map(|t| Table::file_size(t))
                .sum::<u64>();

            if curr_level_size == 0 {
                return None;
            }

            if hidden_set.is_blocked(curr_level_pull_in.iter().map(|t| Table::id(t))) {
                return None;
            }

            let next_level_size = window.iter().map(Table::file_size).sum::<u64>();
            let compaction_bytes = curr_level_size + next_level_size;

            Some((window, curr_level_pull_in, compaction_bytes))
        })
        .min_by_key(|(_, _, bytes)| *bytes)
        .map(|(window, curr_level_pull_in, _)| {
            let mut ids: HashSet<_> = window.iter().map(Table::id).collect();
            ids.extend(curr_level_pull_in.iter().map(|t| Table::id(t)));
            (ids, false)
        })
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

    /// When true, dynamically sizes levels based on the actual data in
    /// the last non-empty level, reducing space amplification to ~1.1x.
    ///
    /// Same as `level_compaction_dynamic_level_bytes` in `RocksDB`.
    ///
    /// Default = false (static leveling).
    dynamic: bool,

    /// When true, enables multi-level compaction: if L0→L1 is chosen but
    /// L1 is already oversized, compacts L0+L1→L2 directly in one pass
    /// to avoid a write-then-rewrite cycle.
    ///
    /// Default = false.
    multi_level: bool,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            l0_threshold: 4,
            target_size:/* 64 MiB */ 64 * 1_024 * 1_024,
            level_ratio_policy: vec![10.0],
            dynamic: false,
            multi_level: false,
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

    /// Enables dynamic level sizing based on actual data in the last level.
    ///
    /// When enabled, level target sizes are computed top-down from the actual
    /// size of the last non-empty level, divided by the ratio at each step.
    /// This reduces space amplification to ~1.1x while keeping write
    /// amplification comparable to static leveling.
    ///
    /// Same as `level_compaction_dynamic_level_bytes` in `RocksDB`.
    ///
    /// Default = false
    #[must_use]
    pub fn with_dynamic_level_bytes(mut self, enabled: bool) -> Self {
        self.dynamic = enabled;
        self
    }

    /// Enables multi-level compaction optimization.
    ///
    /// When L0→L1 compaction is selected but L1 already exceeds its target
    /// size, this option allows compacting L0+L1 directly into L2 in one
    /// pass, avoiding the write-then-rewrite cycle that would otherwise
    /// occur.
    ///
    /// Default = false
    #[must_use]
    pub fn with_multi_level(mut self, enabled: bool) -> Self {
        self.multi_level = enabled;
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

    /// Computes level target sizes for all 7 levels.
    ///
    /// In static mode, uses the standard exponential formula.
    /// In dynamic mode, derives targets from the actual size of the last
    /// non-empty level, dividing backwards by the ratio at each step.
    /// Falls back to static mode when the tree is small (dynamic L1 target
    /// would be less than `level_base_size`).
    fn compute_level_targets(
        &self,
        version: &Version,
        level_shift: usize,
        state: &CompactionState,
    ) -> [u64; 7] {
        let mut targets = [u64::MAX; 7];

        // L0 target is not size-based (it's count-based), so leave at MAX
        targets[0] = u64::MAX;

        if self.dynamic {
            // Find the last non-empty level (Lmax) and its actual size.
            // Iterate forward and keep track of the last non-empty level
            // since iter_levels() does not support DoubleEndedIterator.
            let mut lmax_idx = None;

            for (idx, lvl) in version.iter_levels().enumerate().skip(1) {
                if !lvl.is_empty() {
                    lmax_idx = Some(idx);
                }
            }

            if let Some(lmax_idx) = lmax_idx {
                #[expect(
                    clippy::expect_used,
                    reason = "lmax_idx was found by iterating levels, so it must exist"
                )]
                let lmax_level = version.level(lmax_idx).expect("level should exist");

                let lmax_size: u64 = lmax_level
                    .iter()
                    .flat_map(|run| run.iter())
                    .filter(|table| !state.hidden_set().is_hidden(table.id()))
                    .map(Table::file_size)
                    .sum();

                // Work backwards from Lmax
                if let Some(slot) = targets.get_mut(lmax_idx) {
                    *slot = lmax_size;
                }

                #[expect(
                    clippy::cast_precision_loss,
                    reason = "precision loss is acceptable for level size calculations"
                )]
                let mut current_target = lmax_size as f64;

                // Only backfill down to the effective L1 (accounting for
                // level_shift), not to physical level 1, so we don't
                // overwrite slots below the shifted canonical L1.
                let dynamic_l1_idx = level_shift + 1;

                for idx in (dynamic_l1_idx..lmax_idx).rev() {
                    let canonical = idx - level_shift;
                    // In the forward formula, target(k+1)/target(k) = ratio[k-1],
                    // so backwards: target(k) = target(k+1) / ratio[k-1]
                    let ratio_idx = canonical.saturating_sub(1);
                    let ratio = f64::from(
                        self.level_ratio_policy
                            .get(ratio_idx)
                            .copied()
                            .unwrap_or_else(|| {
                                self.level_ratio_policy.last().copied().unwrap_or(10.0)
                            }),
                    );

                    // Guard against invalid ratios (zero, negative, NaN, infinite).
                    // Fall back to static targets instead of leaving partial
                    // dynamic targets with u64::MAX in lower-level slots.
                    if !ratio.is_finite() || ratio <= 0.0 {
                        return self.compute_static_targets(level_shift);
                    }

                    current_target /= ratio;

                    #[expect(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "target is always positive"
                    )]
                    if let Some(slot) = targets.get_mut(idx) {
                        *slot = current_target as u64;
                    }
                }

                // Fallback: if dynamic L1 target is too small, use static.
                // Compare the shifted L1 slot, not physical slot 1.
                let static_l1 = self.level_base_size();
                if targets.get(dynamic_l1_idx).copied().unwrap_or(0) < static_l1 {
                    return self.compute_static_targets(level_shift);
                }

                return targets;
            }
        }

        self.compute_static_targets(level_shift)
    }

    /// Computes static (exponential) level targets.
    fn compute_static_targets(&self, level_shift: usize) -> [u64; 7] {
        let mut targets = [u64::MAX; 7];

        for (idx, slot) in targets.iter_mut().enumerate().skip(1) {
            if idx <= level_shift {
                continue; // stays at u64::MAX
            }
            #[expect(
                clippy::cast_possible_truncation,
                reason = "level index is bounded by level count (7)"
            )]
            {
                *slot = self.level_target_size((idx - level_shift) as u8);
            }
        }

        targets
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
            (
                crate::UserKey::from("leveled_dynamic"),
                crate::UserValue::from([u8::from(self.dynamic)]),
            ),
            (
                crate::UserKey::from("leveled_multi_level"),
                crate::UserValue::from([u8::from(self.multi_level)]),
            ),
        ]
    }

    #[expect(clippy::too_many_lines)]
    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        assert!(version.level_count() == 7, "should have exactly 7 levels");
        let cmp = config.comparator.as_ref();

        // Trivial move into Lmax
        'trivial_lmax: {
            #[expect(
                clippy::expect_used,
                reason = "level 0 is guaranteed to exist in a valid version"
            )]
            let l0 = version.level(0).expect("first level should exist");

            if !l0.is_empty() && l0.is_disjoint() {
                let lmax_index = version.level_count() - 1;

                if (1..lmax_index).any(|idx| {
                    #[expect(
                        clippy::expect_used,
                        reason = "levels within level_count are guaranteed to exist"
                    )]
                    let level = version.level(idx).expect("level should exist");
                    !level.is_empty()
                }) {
                    // There are intermediary levels with data, cannot trivially move to Lmax
                    break 'trivial_lmax;
                }

                #[expect(
                    clippy::expect_used,
                    reason = "lmax_index is derived from level_count so level is guaranteed to exist"
                )]
                let lmax = version.level(lmax_index).expect("last level should exist");

                if !lmax
                    .aggregate_key_range_cmp(cmp)
                    .overlaps_with_key_range_cmp(&l0.aggregate_key_range_cmp(cmp), cmp)
                {
                    return Choice::Move(CompactionInput {
                        table_ids: l0.list_ids(),
                        #[expect(
                            clippy::cast_possible_truncation,
                            reason = "level count is at most 7, fits in u8"
                        )]
                        dest_level: lmax_index as u8,
                        canonical_level: 1,
                        target_size: self.target_size,
                    });
                }
            }
        }

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

                let key_range = first_level.aggregate_key_range_cmp(cmp);

                // Get overlapping tables in next level
                let get_overlapping = target_level
                    .iter()
                    .flat_map(|run| run.get_overlapping_cmp(&key_range, cmp))
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

        // Intra-L0 compaction: merge multiple L0 runs into a single run within L0
        // when table count is below the L0→L1 threshold
        {
            let first_level = version.l0();

            if first_level.run_count() > 1
                && first_level.table_count() < usize::from(self.l0_threshold)
                && !version.level_is_busy(0, state.hidden_set())
            {
                return Choice::Merge(CompactionInput {
                    table_ids: first_level.list_ids(),
                    dest_level: 0,
                    canonical_level: 0,
                    target_size: self.target_size,
                });
            }
        }

        // Compute level targets (supports both static and dynamic modes)
        let level_targets = self.compute_level_targets(version, level_shift, state);

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

                // NOTE: We check for level length above
                #[expect(clippy::indexing_slicing)]
                let target_size = level_targets[idx];

                #[expect(
                    clippy::indexing_slicing,
                    reason = "idx is from iter_levels().enumerate() so always < 7 = scores.len()"
                )]
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

            let key_range = first_level.aggregate_key_range_cmp(cmp);

            // Get overlapping tables in next level
            let target_level_overlapping_table_ids: Vec<_> = target_level
                .iter()
                .flat_map(|run| run.get_overlapping_cmp(&key_range, cmp))
                .map(Table::id)
                .collect();

            table_ids.extend(&target_level_overlapping_table_ids);

            // Multi-level compaction: if L1 is already oversized, skip it
            // and compact L0+L1 directly into L2 in one pass.
            // NOTE: Currently triggers on pre-compaction L1 score. A future
            // improvement could use projected post-compaction bytes to also
            // catch cases where L1 is close to its target and this batch
            // would push it over.
            if self.multi_level {
                let l1_score = scores.get(canonical_l1_idx).map_or(0.0, |(s, _)| *s);
                let l2_idx = canonical_l1_idx + 1;

                if l1_score > 1.0
                    && l2_idx < version.level_count()
                    && !version.level_is_busy(l2_idx, state.hidden_set())
                {
                    if let Some(l2) = version.level(l2_idx) {
                        // Include ALL L1 tables (we're emptying L1 into L2)
                        table_ids.extend(target_level.list_ids());

                        // Include overlapping L2 tables — query per merged
                        // interval instead of one coarse aggregate (#72).
                        // An aggregate across disjoint tables (e.g. [a,d] and
                        // [x,z] → [a,z]) covers gaps and pulls in L2 tables
                        // that don't actually overlap any input table.
                        //
                        // Merge input key ranges into disjoint intervals first
                        // to reduce redundant queries when L0 tables overlap
                        // (#122 Part 2). Sort by comparator-min, then coalesce.
                        {
                            let mut input_ranges: Vec<_> = target_level
                                .iter()
                                .chain(first_level.iter())
                                .flat_map(|run| run.iter())
                                .map(|t| t.key_range().clone())
                                .collect();
                            input_ranges.sort_by(|a, b| cmp.compare(a.min(), b.min()));

                            let merged = crate::KeyRange::merge_sorted_cmp(input_ranges, cmp);

                            for run in l2.iter() {
                                for interval in &merged {
                                    for l2t in run.get_overlapping_cmp(interval, cmp) {
                                        table_ids.insert(Table::id(l2t));
                                    }
                                }
                            }
                        }

                        #[expect(
                            clippy::cast_possible_truncation,
                            reason = "level index is bounded by level count (7)"
                        )]
                        return Choice::Merge(CompactionInput {
                            table_ids,
                            dest_level: l2_idx as u8,
                            canonical_level: 2,
                            target_size: self.target_size,
                        });
                    }
                }
            }

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

        // pick_minimal_compaction scans all runs in both levels, handling
        // transient multi-run states from multi-level compaction (#108, #122).
        let Some((table_ids, can_trivial_move)) = pick_minimal_compaction(
            level,
            next_level,
            state.hidden_set(),
            overshoot_bytes,
            self.target_size,
            cmp,
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
