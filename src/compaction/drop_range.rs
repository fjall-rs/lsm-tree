// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{
    config::Config, level_manifest::LevelManifest, slice::Slice, version::run::Ranged, KeyRange,
};
use crate::{HashSet, Segment};
use std::ops::Bound;

#[derive(Clone, Debug)]
pub struct OwnedBounds {
    pub start: Bound<Slice>,
    pub end: Bound<Slice>,
}

impl OwnedBounds {
    #[must_use]
    pub fn contains(&self, range: &KeyRange) -> bool {
        let lower_ok = match &self.start {
            Bound::Unbounded => true,
            Bound::Included(key) => key.as_ref() <= range.min().as_ref(),
            Bound::Excluded(key) => key.as_ref() < range.min().as_ref(),
        };

        if !lower_ok {
            return false;
        }

        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(key) => key.as_ref() >= range.max().as_ref(),
            Bound::Excluded(key) => key.as_ref() > range.max().as_ref(),
        }
    }
}

/// Drops all segments that are **contained** in a key range
pub struct Strategy {
    bounds: OwnedBounds,
}

impl Strategy {
    /// Configures a new `DropRange` compaction strategy.
    ///
    /// # Panics
    ///
    /// Panics, if `target_size` is below 1024 bytes.
    #[must_use]
    #[allow(dead_code)]
    pub fn new(bounds: OwnedBounds) -> Self {
        Self { bounds }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "DropRangeCompaction"
    }

    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        let segment_ids: HashSet<_> = levels
            .current_version()
            .iter_levels()
            .flat_map(|lvl| lvl.iter())
            .flat_map(|run| {
                run.iter()
                    .filter(|segment| self.bounds.contains(segment.key_range()))
            })
            .map(Segment::id)
            .collect();

        // NOTE: This should generally not occur because of the
        // tree-level major compaction lock
        // But just as a fail-safe...
        let some_hidden = segment_ids
            .iter()
            .any(|&id| levels.hidden_set().is_hidden(id));

        if some_hidden {
            Choice::DoNothing
        } else {
            Choice::Drop(segment_ids)
        }
    }
}
