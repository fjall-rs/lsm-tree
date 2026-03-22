// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    compaction::state::CompactionState, config::Config, table::Table, version::Version, HashSet,
    KvPair, TableId,
};

#[cfg(test)]
mod tests;

#[doc(hidden)]
pub const NAME: &str = "SizeTieredCompaction";

/// Size-tiered compaction strategy (STCS), also known as Universal compaction.
///
/// All sorted runs live in L0. When enough similarly-sized runs accumulate,
/// they are merged into a single larger run (still in L0). This minimizes
/// write amplification at the cost of higher read and space amplification.
///
/// Best for write-heavy workloads: posting list merges, counters, time-series
/// append-only data.
///
/// # Algorithm
///
/// 1. **Space amplification check:** if `total_size / largest_run_size - 1`
///    exceeds [`max_space_amplification_percent`](Strategy::with_max_space_amplification_percent),
///    all runs are merged (full compaction).
/// 2. **Size-ratio merge:** runs are sorted by size (smallest first). The
///    longest prefix where each consecutive pair satisfies
///    `next.size / prev.size <= 1.0 + size_ratio` is selected. If the prefix
///    length ≥ [`min_merge_width`](Strategy::with_min_merge_width), those runs
///    are merged.
///
/// # Trade-offs vs Leveled
///
/// | Metric | STCS | Leveled |
/// |--------|------|---------|
/// | Write amplification | ~O(N/T) | ~O(T×L) |
/// | Read amplification | Higher (more runs) | Lower (1 run per level) |
/// | Space amplification | Up to 2× temporary | ~1.1× |
#[derive(Clone)]
pub struct Strategy {
    /// Maximum allowed size ratio between adjacent sorted runs (by size) for
    /// them to be considered "similar" and eligible for merging together.
    ///
    /// For two adjacent runs sorted by size, if `larger / smaller <= 1.0 + size_ratio`,
    /// they are considered similar.
    ///
    /// Default = 1.0 (adjacent run can be up to 2× the previous).
    size_ratio: f64,

    /// Minimum number of similarly-sized sorted runs required before
    /// triggering a merge.
    ///
    /// Default = 4.
    min_merge_width: usize,

    /// Maximum number of sorted runs to merge at once.
    ///
    /// Default = `usize::MAX` (unlimited).
    max_merge_width: usize,

    /// When space amplification exceeds this percentage, a full compaction
    /// of all runs is triggered.
    ///
    /// Space amplification is computed as `(total_size / largest_run_size - 1) × 100`.
    ///
    /// Default = 200 (i.e. 200%, meaning total data can be up to 3× the largest run).
    max_space_amplification_percent: u64,

    /// Target table size on disk (possibly compressed) for output tables.
    ///
    /// Default = 64 MiB.
    target_size: u64,
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            size_ratio: 1.0,
            min_merge_width: 4,
            max_merge_width: usize::MAX,
            max_space_amplification_percent: 200,
            target_size: 64 * 1_024 * 1_024,
        }
    }
}

impl Strategy {
    /// Sets the size ratio threshold for considering runs "similar".
    ///
    /// Two adjacent runs (sorted by size) are similar if
    /// `larger / smaller <= 1.0 + size_ratio`.
    ///
    /// Same as `compaction_options_universal.size_ratio` in `RocksDB`.
    ///
    /// Default = 1.0
    #[must_use]
    pub fn with_size_ratio(mut self, ratio: f64) -> Self {
        // Clamp invalid values: NaN, negative, and infinite are replaced
        // with the default (1.0). Zero is allowed (exact-size-match only).
        self.size_ratio = if ratio.is_finite() && ratio >= 0.0 {
            ratio
        } else {
            1.0
        };
        self
    }

    /// Sets the minimum number of runs to merge at once.
    ///
    /// Same as `compaction_options_universal.min_merge_width` in `RocksDB`.
    ///
    /// Default = 4
    #[must_use]
    pub fn with_min_merge_width(mut self, width: usize) -> Self {
        self.min_merge_width = width.max(2);
        self
    }

    /// Sets the maximum number of runs to merge at once.
    ///
    /// Same as `compaction_options_universal.max_merge_width` in `RocksDB`.
    ///
    /// Default = `usize::MAX`
    #[must_use]
    pub fn with_max_merge_width(mut self, width: usize) -> Self {
        self.max_merge_width = width.max(2);
        self
    }

    /// Sets the space amplification threshold (in percent) that triggers
    /// a full compaction of all runs.
    ///
    /// Same as `compaction_options_universal.max_size_amplification_percent` in `RocksDB`.
    ///
    /// Default = 200
    #[must_use]
    pub fn with_max_space_amplification_percent(mut self, percent: u64) -> Self {
        self.max_space_amplification_percent = percent;
        self
    }

    /// Sets the target table size on disk (possibly compressed).
    ///
    /// Default = 64 MiB
    #[must_use]
    pub fn with_table_target_size(mut self, bytes: u64) -> Self {
        self.target_size = bytes;
        self
    }
}

/// Per-run metadata for compaction decisions.
struct RunInfo {
    /// Total on-disk size of all tables in this run.
    size: u64,

    /// Table IDs belonging to this run.
    table_ids: Vec<TableId>,
}

/// Collects run information from L0, filtering out runs with hidden tables.
fn collect_available_runs(version: &Version, state: &CompactionState) -> Vec<RunInfo> {
    let l0 = version.l0();

    l0.iter()
        .filter_map(|run| {
            // Skip runs that have any table in the hidden set (being compacted)
            if run
                .iter()
                .any(|table| state.hidden_set().is_hidden(table.id()))
            {
                return None;
            }

            let size = run.iter().map(Table::file_size).sum::<u64>();
            let table_ids = run.iter().map(Table::id).collect();

            Some(RunInfo { size, table_ids })
        })
        .collect()
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn get_config(&self) -> Vec<KvPair> {
        use byteorder::{LittleEndian, WriteBytesExt};

        let mut size_ratio_bytes = vec![];
        #[expect(clippy::expect_used, reason = "writing into Vec should not fail")]
        size_ratio_bytes
            .write_f64::<LittleEndian>(self.size_ratio)
            .expect("cannot fail");

        vec![
            (
                crate::UserKey::from("tiered_size_ratio"),
                crate::UserValue::from(size_ratio_bytes),
            ),
            (
                crate::UserKey::from("tiered_min_merge_width"),
                crate::UserValue::from(
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "min_merge_width fits in u32 for persistence; usize::MAX maps to u32::MAX"
                    )]
                    (self.min_merge_width.min(u32::MAX as usize) as u32).to_le_bytes(),
                ),
            ),
            (
                crate::UserKey::from("tiered_max_merge_width"),
                crate::UserValue::from(
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "max_merge_width fits in u32 for persistence; usize::MAX maps to u32::MAX"
                    )]
                    (self.max_merge_width.min(u32::MAX as usize) as u32).to_le_bytes(),
                ),
            ),
            (
                crate::UserKey::from("tiered_max_space_amp_pct"),
                crate::UserValue::from(self.max_space_amplification_percent.to_le_bytes()),
            ),
            (
                crate::UserKey::from("tiered_target_size"),
                crate::UserValue::from(self.target_size.to_le_bytes()),
            ),
        ]
    }

    fn choose(&self, version: &Version, _: &Config, state: &CompactionState) -> Choice {
        let runs = collect_available_runs(version, state);

        if runs.len() < 2 {
            return Choice::DoNothing;
        }

        // --- Space amplification check ---
        //
        // The largest run is treated as the "base" data set. Everything else
        // is overhead. If overhead exceeds the threshold, compact everything.
        let total_size: u64 = runs.iter().map(|r| r.size).sum();
        let largest_run_size = runs.iter().map(|r| r.size).max().unwrap_or(0);

        if largest_run_size > 0 {
            // Integer arithmetic to avoid f64 precision loss on large sizes.
            //   (total / largest - 1) * 100 >= threshold
            // is equivalent to:
            //   total * 100 >= largest * (100 + threshold)
            let lhs = u128::from(total_size).saturating_mul(100);
            let rhs = u128::from(largest_run_size)
                .saturating_mul(100 + u128::from(self.max_space_amplification_percent));

            if lhs >= rhs {
                let table_ids: HashSet<TableId> = runs
                    .iter()
                    .flat_map(|r| r.table_ids.iter().copied())
                    .collect();

                return Choice::Merge(CompactionInput {
                    table_ids,
                    dest_level: 0,
                    canonical_level: 0,
                    target_size: self.target_size,
                });
            }
        }

        // --- Size-ratio triggered merge ---
        //
        // Sort runs by size (smallest first), then find the longest prefix
        // where adjacent runs have similar sizes.
        let mut sorted_runs = runs;
        sorted_runs.sort_by(|a, b| a.size.cmp(&b.size));

        let mut prefix_len = 1;

        for window in sorted_runs.windows(2) {
            // NOTE: windows(2) guarantees exactly 2 elements
            let (Some(smaller), Some(larger)) = (window.first(), window.get(1)) else {
                unreachable!("windows(2) always yields slices of length 2");
            };

            if smaller.size == 0 {
                // Zero-size run: always "similar" to the next
                prefix_len += 1;
                continue;
            }

            #[expect(
                clippy::cast_precision_loss,
                reason = "precision loss is acceptable for ratio comparison"
            )]
            let ratio = larger.size as f64 / smaller.size as f64;

            if ratio <= 1.0 + self.size_ratio {
                prefix_len += 1;
            } else {
                break;
            }
        }

        if prefix_len >= self.min_merge_width {
            // Cap at max_merge_width, but ensure we still meet min_merge_width
            // (guards against misconfigured max < min)
            let merge_count = prefix_len.min(self.max_merge_width);

            if merge_count >= self.min_merge_width {
                let table_ids: HashSet<TableId> = sorted_runs
                    .iter()
                    .take(merge_count)
                    .flat_map(|r| r.table_ids.iter().copied())
                    .collect();

                return Choice::Merge(CompactionInput {
                    table_ids,
                    dest_level: 0,
                    canonical_level: 0,
                    target_size: self.target_size,
                });
            }
        }

        Choice::DoNothing
    }
}
