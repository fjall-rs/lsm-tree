// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    blob_tree::value::MaybeInlineValue, coding::Encode, value::InternalValue, Memtable, SeqNo,
    UserKey,
};
use value_log::ValueHandle;

#[allow(clippy::module_name_repetitions)]
pub struct GcWriter<'a> {
    seqno: SeqNo,
    buffer: Vec<(UserKey, ValueHandle, u32)>,
    memtable: &'a Memtable,
}

impl<'a> GcWriter<'a> {
    pub fn new(seqno: SeqNo, memtable: &'a Memtable) -> Self {
        Self {
            seqno,
            memtable,
            buffer: Vec::with_capacity(100),
        }
    }
}

impl<'a> value_log::IndexWriter for GcWriter<'a> {
    fn insert_indirect(
        &mut self,
        key: &[u8],
        vhandle: ValueHandle,
        size: u32,
    ) -> std::io::Result<()> {
        self.buffer.push((key.into(), vhandle, size));
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        log::trace!("Finish blob GC index writer");

        #[allow(clippy::significant_drop_in_scrutinee)]
        for (key, vhandle, size) in self.buffer.drain(..) {
            let buf = MaybeInlineValue::Indirect { vhandle, size }.encode_into_vec();

            self.memtable.insert(InternalValue::from_components(
                key,
                buf,
                self.seqno,
                crate::ValueType::Value,
            ));
        }

        Ok(())
    }
}
