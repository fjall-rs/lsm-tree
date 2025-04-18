// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input};
use crate::{level_manifest::LevelManifest, super_segment::Segment, Config, HashSet};

/// Moves down a level into the destination level.
pub struct Strategy(pub u8, pub u8);

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        "MoveDownCompaction"
    }

    #[allow(clippy::expect_used)]
    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        let resolved_view = levels.resolved_view();

        let level = resolved_view
            .get(usize::from(self.0))
            .expect("level should exist");

        let next_level = resolved_view
            .get(usize::from(self.1))
            .expect("next level should exist");

        if next_level.is_empty() {
            // TODO: list_ids()
            let segment_ids: HashSet<_> = level.segments.iter().map(Segment::id).collect();

            Choice::Move(Input {
                segment_ids,
                dest_level: self.1,
                target_size: 64_000_000,
            })
        } else {
            Choice::DoNothing
        }
    }
}
