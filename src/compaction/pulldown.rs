// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{
    compaction::{state::CompactionState, Input},
    version::Version,
    Config,
};

/// Pulls down and merges a level into the destination level.
///
/// Used for unit tests.
pub struct Strategy(pub u8, pub u8);

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "PullDownCompaction"
    }

    #[expect(clippy::expect_used)]
    fn choose(&self, version: &Version, _: &Config, _: &CompactionState) -> Choice {
        let level = version
            .level(usize::from(self.0))
            .expect("source level should exist");

        let next_level = version
            .level(usize::from(self.1))
            .expect("destination level should exist");

        let mut table_ids = level.list_ids();
        table_ids.extend(next_level.list_ids());

        Choice::Merge(Input {
            table_ids,
            dest_level: self.1,
            target_size: 64_000_000,
            canonical_level: 6, // We don't really care - this compaction is only used for very specific unit tests
        })
    }
}
