// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{config::Config, level_manifest::LevelManifest, segment::Segment, HashSet};

/// Major compaction
///
/// Compacts all segments into the last level.
pub struct Strategy {
    target_size: u64,
}

impl Strategy {
    /// Configures a new `SizeTiered` compaction strategy.
    ///
    /// # Panics
    ///
    /// Panics, if `target_size` is below 1024 bytes.
    #[must_use]
    #[allow(dead_code)]
    pub fn new(target_size: u64) -> Self {
        assert!(target_size >= 1_024);
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

    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        let segment_ids: HashSet<_> = levels.iter().map(Segment::id).collect();

        // NOTE: This should generally not occur because of the
        // tree-level major compaction lock
        // But just as a fail-safe...
        let some_hidden = segment_ids
            .iter()
            .any(|&id| levels.hidden_set().is_hidden(id));

        if some_hidden {
            Choice::DoNothing
        } else {
            Choice::Merge(CompactionInput {
                segment_ids,
                dest_level: levels.last_level_index(),
                target_size: self.target_size,
            })
        }
    }
}
