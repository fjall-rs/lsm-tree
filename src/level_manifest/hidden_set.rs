use crate::segment::meta::SegmentId;

use crate::HashSet;

#[derive(Clone)]
pub(super) struct HiddenSet {
    pub(crate) set: HashSet<SegmentId>,
}

impl Default for HiddenSet {
    fn default() -> Self {
        Self {
            set: HashSet::with_capacity_and_hasher(10, xxhash_rust::xxh3::Xxh3Builder::new()),
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

    pub(crate) fn contains(&self, key: SegmentId) -> bool {
        self.set.contains(&key)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}
