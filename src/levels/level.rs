use crate::{key_range::KeyRange, segment::meta::SegmentId, Segment};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Level {
    #[doc(hidden)]
    pub segments: Vec<Arc<Segment>>,

    pub is_disjoint: bool,
}

impl std::ops::Deref for Level {
    type Target = Vec<Arc<Segment>>;

    fn deref(&self) -> &Self::Target {
        &self.segments
    }
}

impl Default for Level {
    fn default() -> Self {
        Self {
            is_disjoint: true,
            segments: Vec::with_capacity(10),
        }
    }
}

impl Level {
    pub fn insert(&mut self, segment: Arc<Segment>) {
        self.segments.push(segment);
        self.sort_by_seqno();
        self.set_disjoint_flag();
    }

    pub fn remove(&mut self, segment_id: SegmentId) {
        self.segments.retain(|x| segment_id != x.metadata.id);
        self.sort_by_seqno();
        self.set_disjoint_flag();
    }

    pub fn sort_by_key_range(&mut self) {
        self.segments
            .sort_by(|a, b| a.metadata.key_range.0.cmp(&b.metadata.key_range.0));
    }

    /// Sorts the level from newest to oldest
    ///
    /// This will make segments with highest seqno get checked first,
    /// so if there are two versions of an item, the fresher one is seen first:
    ///
    /// segment a   segment b
    /// [key:asd:2] [key:asd:1]
    ///
    /// point read ----------->
    pub fn sort_by_seqno(&mut self) {
        self.segments
            .sort_by(|a, b| b.metadata.seqnos.1.cmp(&a.metadata.seqnos.1));
    }

    pub fn ids(&self) -> Vec<SegmentId> {
        self.segments.iter().map(|x| x.metadata.id).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Gets the number of segments
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Gets the level (compressed) size in bytes
    pub fn size(&self) -> u64 {
        self.segments.iter().map(|x| x.metadata.file_size).sum()
    }

    /// Checks if the level is disjoint and caches the result in `is_disjoint`
    fn set_disjoint_flag(&mut self) {
        let ranges = self
            .segments
            .iter()
            .map(|x| &x.metadata.key_range)
            .cloned()
            .collect::<Vec<_>>();

        self.is_disjoint = KeyRange::is_disjoint(&ranges);
    }

    pub fn get_overlapping_segments(&self, key_range: &KeyRange) -> Vec<SegmentId> {
        self.segments
            .iter()
            .filter(|x| x.metadata.key_range.overlaps_with_key_range(key_range))
            .map(|x| x.metadata.id)
            .collect()
    }
}
