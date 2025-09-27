// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::value::MaybeInlineValue;
use crate::{AbstractTree, SeqNo, Tree as LsmTree};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct IndexTree(#[doc(hidden)] pub LsmTree);

impl std::ops::Deref for IndexTree {
    type Target = LsmTree;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IndexTree {
    pub(crate) fn get_vhandle(
        &self,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<MaybeInlineValue>> {
        let Some(item) = self.get(key, seqno)? else {
            return Ok(None);
        };

        let item = MaybeInlineValue::from_slice(&item)?;

        Ok(Some(item))
    }
}

impl From<LsmTree> for IndexTree {
    fn from(value: LsmTree) -> Self {
        Self(value)
    }
}
