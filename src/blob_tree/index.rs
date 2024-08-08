use super::value::MaybeInlineValue;
use crate::{serde::Deserializable, AbstractTree, SeqNo, Tree as LsmTree};
use std::io::Cursor;

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
    pub(crate) fn get_internal_with_seqno(
        &self,
        key: &[u8],
        seqno: SeqNo,
    ) -> crate::Result<Option<MaybeInlineValue>> {
        let Some(item) = self.get_with_seqno(key, seqno)? else {
            return Ok(None);
        };

        let mut cursor = Cursor::new(item);
        let item = MaybeInlineValue::deserialize(&mut cursor)?;

        Ok(Some(item))
    }

    pub(crate) fn get_internal(&self, key: &[u8]) -> crate::Result<Option<MaybeInlineValue>> {
        let Some(item) = self.get(key)? else {
            return Ok(None);
        };

        let mut cursor = Cursor::new(item);
        let item = MaybeInlineValue::deserialize(&mut cursor)?;

        Ok(Some(item))
    }
}

impl From<LsmTree> for IndexTree {
    fn from(value: LsmTree) -> Self {
        Self(value)
    }
}
