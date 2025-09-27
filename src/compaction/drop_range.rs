// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{config::Config, level_manifest::LevelManifest, KeyRange};
use crate::{HashSet, Segment};

/// Drops all segments that are **contained** in a key range
pub struct Strategy {
    key_range: KeyRange,
}

impl Strategy {
    /// Configures a new `DropRange` compaction strategy.
    ///
    /// # Panics
    ///
    /// Panics, if `target_size` is below 1024 bytes.
    #[must_use]
    #[allow(dead_code)]
    pub fn new(key_range: KeyRange) -> Self {
        Self { key_range }
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
            .flat_map(|run| run.get_contained(&self.key_range))
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
