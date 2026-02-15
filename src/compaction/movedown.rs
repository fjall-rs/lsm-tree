// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input};
use crate::{
    compaction::state::CompactionState, fs::FileSystem, table::Table, version::Version, Config,
};

/// Moves down a level into the destination level.
pub struct Strategy(pub u8, pub u8);

impl<F: FileSystem> CompactionStrategy<F> for Strategy {
    fn get_name(&self) -> &'static str {
        "MoveDownCompaction"
    }

    fn choose(&self, version: &Version<F>, _: &Config<F>, state: &CompactionState) -> Choice {
        if version.level_is_busy(usize::from(self.0), state.hidden_set()) {
            return Choice::DoNothing;
        }

        let Some(level) = version.level(self.0.into()) else {
            return Choice::DoNothing;
        };

        let table_ids = level
            .iter()
            .flat_map(|run| run.iter())
            .map(Table::id)
            .collect();

        Choice::Move(Input {
            table_ids,
            dest_level: self.1,
            canonical_level: self.1,
            target_size: u64::MAX,
        })
    }
}
