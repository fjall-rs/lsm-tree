// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{
    compaction::state::CompactionState, config::Config, segment::Segment, version::Version, HashSet,
};

/// Compacts all tables into the last level
pub struct Strategy {
    target_size: u64,
}

impl Strategy {
    /// Configures a new `Major` compaction strategy.
    #[must_use]
    #[allow(dead_code)]
    pub fn new(target_size: u64) -> Self {
        Self { target_size }
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Self {
            target_size: u64::MAX,
        }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "MajorCompaction"
    }

    fn choose(&self, version: &Version, cfg: &Config, state: &CompactionState) -> Choice {
        let table_ids: HashSet<_> = version.iter_tables().map(Segment::id).collect();

        // NOTE: This should generally not occur because of the
        // tree-level major compaction lock
        // But just as a fail-safe...
        let some_hidden = table_ids.iter().any(|&id| state.hidden_set().is_hidden(id));

        if some_hidden {
            Choice::DoNothing
        } else {
            let last_level_idx = cfg.level_count - 1;

            Choice::Merge(CompactionInput {
                segment_ids: table_ids,
                dest_level: last_level_idx,
                canonical_level: last_level_idx,
                target_size: self.target_size,
            })
        }
    }
}
