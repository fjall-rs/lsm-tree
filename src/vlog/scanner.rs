// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::vlog::{BlobFileId, ValueHandle};
use std::{collections::BTreeMap, sync::MutexGuard};

#[derive(Debug, Default)]
pub struct BlobFileCounter {
    pub size: u64,
    pub item_count: u64,
}

pub type SizeMap = BTreeMap<BlobFileId, BlobFileCounter>;

/// Scans a value log, building a size map for the GC report
pub struct Scanner<'a, I: Iterator<Item = std::io::Result<(ValueHandle, u32)>>> {
    iter: I,

    #[allow(unused)]
    lock_guard: MutexGuard<'a, ()>,

    size_map: SizeMap,
}

impl<'a, I: Iterator<Item = std::io::Result<(ValueHandle, u32)>>> Scanner<'a, I> {
    pub fn new(iter: I, lock_guard: MutexGuard<'a, ()>, ids: &[BlobFileId]) -> Self {
        let mut size_map = BTreeMap::default();

        for &id in ids {
            size_map.insert(id, BlobFileCounter::default());
        }

        Self {
            iter,
            lock_guard,
            size_map,
        }
    }

    pub fn finish(self) -> SizeMap {
        self.size_map
    }

    pub fn scan(&mut self) -> crate::Result<()> {
        for vhandle in self.iter.by_ref() {
            let (vhandle, size) = vhandle
                .map_err(|_| crate::Error::Io(std::io::Error::other("Index returned error")))?;

            let size = u64::from(size);

            self.size_map
                .entry(vhandle.blob_file_id)
                .and_modify(|x| {
                    x.item_count += 1;
                    x.size += size;
                })
                .or_insert_with(|| BlobFileCounter {
                    size,
                    item_count: 1,
                });
        }

        Ok(())
    }
}
