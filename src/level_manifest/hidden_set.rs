use crate::segment::meta::SegmentId;
use crate::HashSet;

/// The hidden set keeps track of which segments are currently being compacted
///
/// When a segment is hidden (being compacted), no other compaction task can include that
/// segment, or it will be declined to be run.
///
/// If a compaction task fails, the segments are shown again (removed from the hidden set).
#[derive(Clone)]
pub struct HiddenSet {
    pub(crate) set: HashSet<SegmentId>,
}

impl Default for HiddenSet {
    fn default() -> Self {
        Self {
            set: HashSet::with_capacity_and_hasher(10, Default::default()),
        }
    }
}

impl HiddenSet {
    pub(crate) fn hide<T: IntoIterator<Item = SegmentId>>(&mut self, keys: T) {
        self.set.extend(keys);
    }

    pub(crate) fn show<T: IntoIterator<Item = SegmentId>>(&mut self, keys: T) {
        for key in keys {
            self.set.remove(&key);
        }
    }

    pub(crate) fn is_blocked<T: IntoIterator<Item = SegmentId>>(&self, ids: T) -> bool {
        ids.into_iter().any(|id| self.is_hidden(id))
    }

    pub(crate) fn is_hidden(&self, key: SegmentId) -> bool {
        self.set.contains(&key)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}
