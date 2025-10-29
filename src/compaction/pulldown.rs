// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy};
use crate::{compaction::state::CompactionState, version::Version, Config};

/// Pulls down and merges a level into the destination level.
///
/// Used for unit tests.
pub struct Strategy(pub u8, pub u8);

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "PullDownCompaction"
    }

    fn choose(&self, version: &Version, _: &Config, state: &CompactionState) -> Choice {
        todo!()
    }
}
