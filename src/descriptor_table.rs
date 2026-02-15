// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{fs::FileSystem, GlobalTableId};
use quick_cache::{sync::Cache as QuickCache, UnitWeighter};
use std::sync::Arc;

const TAG_BLOCK: u8 = 0;
const TAG_BLOB: u8 = 1;

type Item<F> = Arc<<F as FileSystem>::File>;

#[derive(Eq, std::hash::Hash, PartialEq)]
struct CacheKey(u8, u64, u64);

/// Caches file descriptors to tables and blob files
pub struct DescriptorTable<F: FileSystem> {
    inner: QuickCache<CacheKey, Item<F>, UnitWeighter, rustc_hash::FxBuildHasher>,
}

impl<F: FileSystem> DescriptorTable<F> {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        use quick_cache::sync::DefaultLifecycle;

        let quick_cache = QuickCache::with(
            1_000,
            capacity as u64,
            UnitWeighter,
            rustc_hash::FxBuildHasher,
            DefaultLifecycle::default(),
        );

        Self { inner: quick_cache }
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn access_for_table(&self, id: &GlobalTableId) -> Option<Arc<F::File>> {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.get(&key)
    }

    pub fn insert_for_table(&self, id: GlobalTableId, item: Item<F>) {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.insert(key, item);
    }

    #[must_use]
    pub fn access_for_blob_file(&self, id: &GlobalTableId) -> Option<Arc<F::File>> {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.get(&key)
    }

    pub fn insert_for_blob_file(&self, id: GlobalTableId, item: Item<F>) {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.insert(key, item);
    }

    pub fn remove_for_table(&self, id: &GlobalTableId) {
        let key = CacheKey(TAG_BLOCK, id.tree_id(), id.table_id());
        self.inner.remove(&key);
    }

    pub fn remove_for_blob_file(&self, id: &GlobalTableId) {
        let key = CacheKey(TAG_BLOB, id.tree_id(), id.table_id());
        self.inner.remove(&key);
    }
}
