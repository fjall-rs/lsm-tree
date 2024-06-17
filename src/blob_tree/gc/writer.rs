use crate::{blob_tree::value::MaybeInlineValue, serde::Serializable, MemTable, SeqNo, UserKey};
use std::sync::{Arc, RwLockWriteGuard};
use value_log::ValueHandle;

#[allow(clippy::module_name_repetitions)]
pub struct GcWriter<'a> {
    seqno: SeqNo,
    buffer: Vec<(UserKey, ValueHandle, u32)>,
    memtable: &'a RwLockWriteGuard<'a, MemTable>,
}

impl<'a> GcWriter<'a> {
    pub fn new(seqno: SeqNo, memtable: &'a RwLockWriteGuard<'a, MemTable>) -> Self {
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
        handle: ValueHandle,
        size: u32,
    ) -> std::io::Result<()> {
        self.buffer.push((key.into(), handle, size));
        Ok(())
    }

    fn finish(&mut self) -> std::io::Result<()> {
        use std::io::{Error as IoError, ErrorKind as IoErrorKind};

        log::trace!("Finish blob GC index writer");

        #[allow(clippy::significant_drop_in_scrutinee)]
        for (key, handle, size) in self.buffer.drain(..) {
            let mut buf = vec![];
            MaybeInlineValue::Indirect { handle, size }
                .serialize(&mut buf)
                .map_err(|e| IoError::new(IoErrorKind::Other, e.to_string()))?;

            self.memtable.insert(crate::Value {
                key,
                value: Arc::from(buf),
                seqno: self.seqno,
                value_type: crate::ValueType::Value,
            });
        }

        Ok(())
    }
}
