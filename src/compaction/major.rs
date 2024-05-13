use super::{Choice, CompactionStrategy, Input as CompactionInput};
use crate::{config::Config, levels::LevelManifest};

/// Major compaction
///
/// Compacts all segments into the last level
pub struct Strategy {
    target_size: u64,
}

impl Strategy {
    /// Configures a new `SizeTiered` compaction strategy
    ///
    /// # Panics
    ///
    /// Panics, if `target_size` is below 1024 bytes
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
    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        let segment_ids = levels.iter().map(|x| x.metadata.id).collect();

        Choice::DoCompact(CompactionInput {
            segment_ids,
            dest_level: levels.last_level_index(),
            target_size: self.target_size,
        })
    }
}
