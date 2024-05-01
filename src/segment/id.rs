use super::meta::SegmentId;
use crate::tree_inner::TreeId;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GlobalSegmentId((TreeId, SegmentId));

impl GlobalSegmentId {
    pub fn tree_id(&self) -> TreeId {
        self.0 .0
    }

    pub fn segment_id(&self) -> SegmentId {
        self.0 .1
    }
}

impl From<(TreeId, SegmentId)> for GlobalSegmentId {
    fn from(value: (TreeId, SegmentId)) -> Self {
        Self(value)
    }
}
