use super::HiddenSet;
use crate::{key_range::KeyRange, segment::Segment};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::DerefMut, sync::Arc};

#[derive(Serialize, Deserialize)]
pub struct Level(Vec<Arc<str>>);

impl std::ops::Deref for Level {
    type Target = Vec<Arc<str>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Level {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Default for Level {
    fn default() -> Self {
        Self(Vec::with_capacity(10))
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct ResolvedLevel(pub(crate) Vec<Arc<Segment>>);

impl std::ops::Deref for ResolvedLevel {
    type Target = Vec<Arc<Segment>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResolvedLevel {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl ResolvedLevel {
    pub fn new(
        level: &Level,
        hidden_set: &HiddenSet,
        segments: &HashMap<Arc<str>, Arc<Segment>>,
    ) -> Self {
        let mut new_level = Vec::new();

        for segment_id in level.iter() {
            if !hidden_set.contains(segment_id) {
                new_level.push(
                    segments
                        .get(segment_id)
                        .cloned()
                        .expect("where's the segment at?"),
                );
            }
        }

        Self(new_level)
    }

    /// Gets the level (compressed) size in bytes
    pub fn size(&self) -> u64 {
        self.iter().map(|x| x.metadata.file_size).sum()
    }

    // TODO: unit tests
    pub fn is_disjunct(&self) -> bool {
        for i in 0..self.0.len() {
            for j in i + 1..self.0.len() {
                let segment1 = &self.0.get(i).expect("should exist");
                let segment2 = &self.0.get(j).expect("should exist");

                if segment1
                    .metadata
                    .key_range
                    .overlaps_with_key_range(&segment2.metadata.key_range)
                {
                    return false;
                }
            }
        }

        true
    }

    pub fn get_overlapping_segments(&self, key_range: &KeyRange) -> Vec<Arc<str>> {
        self.0
            .iter()
            .filter(|x| x.metadata.key_range.overlaps_with_key_range(key_range))
            .map(|x| &x.metadata.id)
            .cloned()
            .collect()
    }
}

/*
// Helper function to check if two segments are disjoint
fn is_disjoint(a: (UserKey, UserKey), b: (UserKey, UserKey)) -> bool {
    use std::ops::Bound::Included;

    let (start, end) = b.clone();
    let bounds = (Included(start), Included(end));

    segment1.check_key_range_overlap(&bounds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_disjoint_segments_disjoint() {
        // Create two disjoint segments
        let segment1 = Arc::new(Segment { start: 0, end: 5 });
        let segment2 = Arc::new(Segment { start: 6, end: 10 });

        // Check if they are disjoint
        assert!(is_disjoint(&segment1, &segment2));
    }

    #[test]
    fn test_is_disjoint_segments_overlap() {
        // Create two overlapping segments
        let segment1 = Arc::new(Segment { start: 0, end: 5 });
        let segment2 = Arc::new(Segment { start: 3, end: 7 });

        // Check if they are disjoint
        assert!(!is_disjoint(&segment1, &segment2));
    }
}
 */
