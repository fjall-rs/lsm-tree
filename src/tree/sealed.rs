use crate::{tree::inner::MemtableId, Memtable};
use std::sync::Arc;

/// Stores references to all sealed memtables
///
/// Memtable IDs are monotonically increasing, so we don't really
/// need a search tree; also there are only a handful of them at most.
#[derive(Clone, Default)]
pub struct SealedMemtables(Vec<(MemtableId, Arc<Memtable>)>);

impl SealedMemtables {
    /// Copy-and-writes a new list with additional Memtable.
    pub fn add(&self, id: MemtableId, memtable: Arc<Memtable>) -> Self {
        let mut copy = self.clone();
        copy.0.push((id, memtable));
        copy
    }

    /// Copy-and-writes a new list with the specified Memtable removed.
    pub fn remove(&self, id_to_remove: MemtableId) -> Self {
        let mut copy = self.clone();
        copy.0.retain(|(id, _)| *id != id_to_remove);
        copy
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &(MemtableId, Arc<Memtable>)> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
