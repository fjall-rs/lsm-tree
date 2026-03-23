// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::compaction::state::CompactionState;
use crate::version::Version;
use crate::{config::Config, slice::Slice, version::run::Ranged, KeyRange};
use crate::{HashSet, Table};
use std::ops::{Bound, RangeBounds};

#[derive(Clone, Debug)]
pub struct OwnedBounds {
    pub start: Bound<Slice>,
    pub end: Bound<Slice>,
}

impl RangeBounds<Slice> for OwnedBounds {
    fn start_bound(&self) -> Bound<&Slice> {
        match &self.start {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(key),
            Bound::Excluded(key) => Bound::Excluded(key),
        }
    }

    fn end_bound(&self) -> Bound<&Slice> {
        match &self.end {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) => Bound::Included(key),
            Bound::Excluded(key) => Bound::Excluded(key),
        }
    }
}

impl OwnedBounds {
    /// Returns `true` if the key range is fully contained in these bounds,
    /// using the given comparator for key ordering.
    #[must_use]
    pub fn contains(&self, range: &KeyRange, cmp: &dyn crate::comparator::UserComparator) -> bool {
        use std::cmp::Ordering;

        let lower_ok = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(key) => cmp.compare(key.as_ref(), range.min()) != Ordering::Greater,
            Bound::Excluded(key) => cmp.compare(key.as_ref(), range.min()) == Ordering::Less,
        };

        if !lower_ok {
            return false;
        }

        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(key) => cmp.compare(key.as_ref(), range.max()) != Ordering::Less,
            Bound::Excluded(key) => cmp.compare(key.as_ref(), range.max()) == Ordering::Greater,
        }
    }
}

/// Drops all tables that are **contained** in a key range
pub struct Strategy {
    bounds: OwnedBounds,
}

impl Strategy {
    /// Configures a new `DropRange` compaction strategy.
    #[must_use]
    pub fn new(bounds: OwnedBounds) -> Self {
        Self { bounds }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "DropRangeCompaction"
    }

    fn choose(&self, version: &Version, config: &Config, state: &CompactionState) -> Choice {
        let cmp = config.comparator.as_ref();

        let table_ids: HashSet<_> = version
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .flat_map(|run| {
                run.range_overlap_indexes_cmp(&self.bounds, cmp)
                    .and_then(|(lo, hi)| run.get(lo..=hi))
                    .unwrap_or_default()
                    .iter()
                    .filter(|x| self.bounds.contains(x.key_range(), cmp))
            })
            .map(Table::id)
            .collect();

        // NOTE: This should generally not occur because of the
        // tree-level major compaction lock
        // But just as a fail-safe...
        let some_hidden = table_ids.iter().any(|&id| state.hidden_set().is_hidden(id));

        if some_hidden {
            Choice::DoNothing
        } else {
            Choice::Drop(table_ids)
        }
    }
}
