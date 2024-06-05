use crate::{blob_tree::value::MaybeInlineValue, serde::Deserializable, MemTable};
use std::{io::Cursor, sync::RwLockWriteGuard};
use value_log::ValueHandle;

#[allow(clippy::module_name_repetitions)]
pub struct GcReader<'a> {
    tree: &'a crate::Tree,
    memtable: &'a RwLockWriteGuard<'a, MemTable>,
}

impl<'a> GcReader<'a> {
    pub fn new(tree: &'a crate::Tree, memtable: &'a RwLockWriteGuard<'a, MemTable>) -> Self {
        Self { tree, memtable }
    }

    fn get_internal(&self, key: &[u8]) -> crate::Result<Option<MaybeInlineValue>> {
        let Some(item) = self
            .tree
            .get_internal_entry_with_lock(self.memtable, key, true, None)?
            .map(|x| x.value)
        else {
            return Ok(None);
        };

        let mut cursor = Cursor::new(item);
        let item = MaybeInlineValue::deserialize(&mut cursor)?;

        Ok(Some(item))
    }
}

impl<'a> value_log::ExternalIndex for GcReader<'a> {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        use std::io::{Error as IoError, ErrorKind as IoErrorKind};

        let Some(item) = self
            .get_internal(key)
            .map_err(|e| IoError::new(IoErrorKind::Other, e.to_string()))?
        else {
            return Ok(None);
        };

        match item {
            MaybeInlineValue::Inline(_) => Ok(None),
            MaybeInlineValue::Indirect { handle, .. } => Ok(Some(handle)),
        }
    }
}
