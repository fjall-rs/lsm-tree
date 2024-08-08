use super::LevelManifest;
use crate::Segment;
use std::sync::Arc;

/// Iterates through all levels
pub struct LevelManifestIterator<'a> {
    level_manifest: &'a LevelManifest,
    current_level: usize,
    current_idx: usize,
}

impl<'a> LevelManifestIterator<'a> {
    #[must_use]
    pub fn new(level_manifest: &'a LevelManifest) -> Self {
        Self {
            level_manifest,
            current_idx: 0,
            current_level: 0,
        }
    }
}

impl<'a> Iterator for LevelManifestIterator<'a> {
    type Item = &'a Arc<Segment>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let segment = self
                .level_manifest
                .levels
                .get(self.current_level)?
                .segments
                .get(self.current_idx);

            if let Some(segment) = segment {
                self.current_idx += 1;
                return Some(segment);
            }

            self.current_level += 1;
            self.current_idx = 0;
        }
    }
}
