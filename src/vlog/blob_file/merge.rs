// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    vlog::{BlobFileId, BlobFileReader},
    UserKey, UserValue,
};
use interval_heap::IntervalHeap;
use std::cmp::Reverse;

macro_rules! fail_iter {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return Some(Err(e.into())),
        }
    };
}

type IteratorIndex = usize;

#[derive(Debug)]
struct IteratorValue {
    index: IteratorIndex,
    key: UserKey,
    value: UserValue,
    blob_file_id: BlobFileId,
    checksum: u64,
}

impl PartialEq for IteratorValue {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for IteratorValue {}

impl PartialOrd for IteratorValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IteratorValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.key, Reverse(&self.blob_file_id)).cmp(&(&other.key, Reverse(&other.blob_file_id)))
    }
}

/// Interleaves multiple blob file readers into a single, sorted stream
#[allow(clippy::module_name_repetitions)]
pub struct MergeReader {
    readers: Vec<BlobFileReader>,
    heap: IntervalHeap<IteratorValue>,
}

impl MergeReader {
    /// Initializes a new merging reader
    pub fn new(readers: Vec<BlobFileReader>) -> Self {
        let heap = IntervalHeap::with_capacity(readers.len());
        Self { readers, heap }
    }

    fn advance_reader(&mut self, idx: usize) -> crate::Result<()> {
        let reader = self.readers.get_mut(idx).expect("iter should exist");

        if let Some(value) = reader.next() {
            let (k, v, checksum) = value?;
            let blob_file_id = reader.blob_file_id;

            self.heap.push(IteratorValue {
                index: idx,
                key: k,
                value: v,
                blob_file_id,
                checksum,
            });
        }

        Ok(())
    }

    fn push_next(&mut self) -> crate::Result<()> {
        for idx in 0..self.readers.len() {
            self.advance_reader(idx)?;
        }

        Ok(())
    }
}

impl Iterator for MergeReader {
    type Item = crate::Result<(UserKey, UserValue, BlobFileId, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            fail_iter!(self.push_next());
        }

        if let Some(head) = self.heap.pop_min() {
            fail_iter!(self.advance_reader(head.index));

            // Discard old items
            while let Some(next) = self.heap.pop_min() {
                if next.key == head.key {
                    fail_iter!(self.advance_reader(next.index));
                } else {
                    // Reached next user key now
                    // Push back non-conflicting item and exit
                    self.heap.push(next);
                    break;
                }
            }

            return Some(Ok((head.key, head.value, head.blob_file_id, head.checksum)));
        }

        None
    }
}
