// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{key::InternalKey, tree::inner::MemtableId, InternalValue, Memtable, SeqNo};
use std::{ops::RangeBounds, sync::Arc};

#[derive(Clone)]
pub struct SealedMemtable(Arc<Memtable>);

impl SealedMemtable {
    pub fn new(mt: Arc<Memtable>) -> Self {
        Self(mt)
    }

    /// Returns the memtable ID.
    pub fn id(&self) -> MemtableId {
        self.0.id()
    }

    /// Gets approximate size of memtable in bytes.
    pub fn size(&self) -> u64 {
        self.0.size()
    }

    /// Creates an iterator over all items.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        self.0.items.iter().map(|entry| InternalValue {
            key: entry.key().clone(),
            value: entry.value().clone(),
        })
    }

    /// Counts the number of items in the memtable.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_highest_seqno(&self) -> Option<SeqNo> {
        self.0.get_highest_seqno()
    }

    /// Returns the item by key if it exists.
    ///
    /// The item with the highest seqno will be returned, if `seqno` is None.
    #[doc(hidden)]
    pub fn get(&self, key: &[u8], seqno: SeqNo) -> Option<InternalValue> {
        self.0.get(key, seqno)
    }

    /// Creates an iterator over a range of items.
    pub(crate) fn range<'a, R: RangeBounds<InternalKey> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + 'a {
        self.0.range(range)
    }
}

/// Stores references to all sealed memtables
///
/// Memtable IDs are monotonically increasing, so we don't really
/// need a search tree; also there are only a handful of them at most.
#[derive(Clone, Default)]
pub struct SealedMemtables(Vec<SealedMemtable>);

impl SealedMemtables {
    /// Copy-and-writes a new list with additional Memtable.
    pub fn add(&self, memtable: Arc<Memtable>) -> Self {
        let mut copy = self.clone();
        copy.0.push(SealedMemtable::new(memtable));
        copy
    }

    /// Copy-and-writes a new list with the specified Memtable removed.
    pub fn remove(&self, id_to_remove: MemtableId) -> Self {
        let mut copy = self.clone();
        copy.0.retain(|mt| mt.id() != id_to_remove);
        copy
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &SealedMemtable> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}
