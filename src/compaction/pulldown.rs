use super::{Choice, CompactionStrategy, Input};
use crate::{levels::LevelManifest, Config};

/// Pulls down and merges a level into the destination level.
///
/// Used for unit tests.
pub struct Strategy(pub u8, pub u8);

impl CompactionStrategy for Strategy {
    #[allow(clippy::expect_used)]
    fn choose(&self, levels: &LevelManifest, _: &Config) -> Choice {
        let resolved_view = levels.resolved_view();

        let level = resolved_view
            .get(usize::from(self.0))
            .expect("level should exist");

        let next_level = resolved_view
            .get(usize::from(self.1))
            .expect("next level should exist");

        let mut segment_ids = level
            .segments
            .iter()
            .map(|x| x.metadata.id)
            .collect::<Vec<_>>();

        segment_ids.extend(next_level.segments.iter().map(|x| x.metadata.id));

        Choice::Merge(Input {
            segment_ids,
            dest_level: self.1,
            target_size: 64_000_000,
        })
    }
}
