// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input};
use crate::{level_manifest::LevelManifest, segment::Segment, Config, HashSet};

/// Moves down a level into the destination level.
pub struct Strategy(pub u8, pub u8);

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "MoveDownCompaction"
    }

    #[allow(clippy::expect_used)]
    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        todo!()
    }
}
