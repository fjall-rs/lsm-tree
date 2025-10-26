// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::tree::inner::TreeId;

pub type TableId = u64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(clippy::module_name_repetitions)]
pub struct GlobalTableId(TreeId, TableId);

impl GlobalTableId {
    #[must_use]
    pub fn tree_id(&self) -> TreeId {
        self.0
    }

    #[must_use]
    pub fn segment_id(&self) -> TableId {
        self.1
    }
}

impl From<(TreeId, TableId)> for GlobalTableId {
    fn from((tid, sid): (TreeId, TableId)) -> Self {
        Self(tid, sid)
    }
}
