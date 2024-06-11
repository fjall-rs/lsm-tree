use super::meta::SegmentId;
use crate::tree::inner::TreeId;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(clippy::module_name_repetitions)]
pub struct GlobalSegmentId(TreeId, SegmentId);

impl GlobalSegmentId {
    #[must_use]
    pub fn tree_id(&self) -> TreeId {
        self.0
    }

    #[must_use]
    pub fn segment_id(&self) -> SegmentId {
        self.1
    }
}

impl From<(TreeId, SegmentId)> for GlobalSegmentId {
    fn from((tid, sid): (TreeId, SegmentId)) -> Self {
        Self(tid, sid)
    }
}
