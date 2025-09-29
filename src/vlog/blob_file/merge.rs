// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::scanner::Scanner as BlobFileScanner;
use crate::{vlog::BlobFileId, Checksum, UserKey, UserValue};
use interval_heap::IntervalHeap;
use std::cmp::Reverse;

type IteratorIndex = usize;

#[derive(Debug)]
struct IteratorValue {
    index: IteratorIndex,
    key: UserKey,
    value: UserValue,
    blob_file_id: BlobFileId,
    checksum: Checksum,
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
pub struct MergeScanner {
    readers: Vec<BlobFileScanner>,
    heap: IntervalHeap<IteratorValue>,
}

impl MergeScanner {
    /// Initializes a new merging reader
    pub fn new(readers: Vec<BlobFileScanner>) -> Self {
        let heap = IntervalHeap::with_capacity(readers.len());
        Self { readers, heap }
    }

    fn advance_reader(&mut self, idx: usize) -> crate::Result<()> {
        // NOTE: We trust the caller
        #[allow(clippy::indexing_slicing)]
        let reader = &mut self.readers[idx];

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

impl Iterator for MergeScanner {
    type Item = crate::Result<(UserKey, UserValue, BlobFileId, Checksum)>;

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::scanner::Scanner;
    use super::*;
    use crate::{vlog::blob_file::writer::Writer as BlobFileWriter, Slice};
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn blob_file_merger() -> crate::Result<()> {
        let dir = tempdir()?;

        let blob_file_0_path = dir.path().join("0");

        let blob_file_1_path = dir.path().join("1");

        {
            let keys = [b"a", b"c", b"e"];

            {
                let mut writer = BlobFileWriter::new(&blob_file_0_path, 0)?;

                for key in keys {
                    writer.write(key, &key.repeat(100))?;
                }

                writer.finish()?;
            }
        }

        {
            let keys = [b"b", b"d"];

            {
                let mut writer = BlobFileWriter::new(&blob_file_1_path, 1)?;

                for key in keys {
                    writer.write(key, &key.repeat(100))?;
                }

                writer.finish()?;
            }
        }

        {
            let mut merger = MergeScanner::new(vec![
                Scanner::new(&blob_file_0_path, 0)?,
                Scanner::new(&blob_file_1_path, 1)?,
            ]);

            let merged_keys = [b"a", b"b", b"c", b"d", b"e"];

            for key in merged_keys {
                assert_eq!(
                    (Slice::from(key), Slice::from(key.repeat(100))),
                    merger
                        .next()
                        .map(|result| result.map(|(k, v, _, _)| { (k, v) }))
                        .unwrap()?,
                );
            }

            assert!(merger.next().is_none());
        }

        Ok(())
    }
}
