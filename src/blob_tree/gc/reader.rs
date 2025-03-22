// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{blob_tree::value::MaybeInlineValue, coding::Decode, Memtable};
use std::{io::Cursor, sync::RwLockWriteGuard};
use value_log::ValueHandle;

#[allow(clippy::module_name_repetitions)]
pub struct GcReader<'a> {
    tree: &'a crate::Tree,
    memtable: &'a RwLockWriteGuard<'a, Memtable>,
}

impl<'a> GcReader<'a> {
    pub fn new(tree: &'a crate::Tree, memtable: &'a RwLockWriteGuard<'a, Memtable>) -> Self {
        Self { tree, memtable }
    }

    fn get_internal(&self, key: &[u8]) -> crate::Result<Option<MaybeInlineValue>> {
        let Some(item) = self
            .tree
            .get_internal_entry_with_lock(self.memtable, key, None)?
            .map(|x| x.value)
        else {
            return Ok(None);
        };

        let mut cursor = Cursor::new(item);
        let item = MaybeInlineValue::decode_from(&mut cursor)?;

        Ok(Some(item))
    }
}

impl<'a> value_log::IndexReader for GcReader<'a> {
    fn get(&self, key: &[u8]) -> std::io::Result<Option<ValueHandle>> {
        use std::io::{Error as IoError, ErrorKind as IoErrorKind};
        use MaybeInlineValue::{Indirect, Inline};

        let Some(item) = self
            .get_internal(key)
            .map_err(|e| IoError::new(IoErrorKind::Other, e.to_string()))?
        else {
            return Ok(None);
        };

        match item {
            Inline(_) => Ok(None),
            Indirect { vhandle, .. } => Ok(Some(vhandle)),
        }
    }
}
