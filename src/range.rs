// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    key::InternalKey,
    memtable::Memtable,
    merge::Merger,
    mvcc_stream::MvccStream,
    range_tombstone::RangeTombstone,
    range_tombstone_filter::RangeTombstoneFilter,
    run_reader::RunReader,
    table::Table,
    value::{SeqNo, UserKey},
    version::SuperVersion,
    BoxedIterator, InternalValue,
};
use self_cell::self_cell;
use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

#[must_use]
pub fn seqno_filter(item_seqno: SeqNo, seqno: SeqNo) -> bool {
    item_seqno < seqno
}

/// Returns `true` if a table is fully covered by a range tombstone visible at `read_seqno`.
///
/// A table can be skipped when a tombstone fully covers its key range `[min, max]`
/// and has a seqno greater than the table's highest seqno, meaning every entry
/// in the table is suppressed.
#[must_use]
fn is_table_fully_covered(table: &Table, tombstones: &[RangeTombstone], read_seqno: SeqNo) -> bool {
    let key_range = &table.metadata.key_range;
    let table_min = key_range.min().as_ref();
    let table_max = key_range.max().as_ref();
    let table_max_seqno = table.get_highest_seqno();

    tombstones.iter().any(|rt| {
        rt.visible_at(read_seqno)
            && rt.fully_covers(table_min, table_max)
            && rt.seqno > table_max_seqno
    })
}

/// Calculates the prefix's upper range.
///
/// # Panics
///
/// Panics if the prefix is empty.
pub(crate) fn prefix_upper_range(prefix: &[u8]) -> Bound<UserKey> {
    use std::ops::Bound::{Excluded, Unbounded};

    assert!(!prefix.is_empty(), "prefix may not be empty");

    let mut end = prefix.to_vec();
    let len = end.len();

    for (idx, byte) in end.iter_mut().rev().enumerate() {
        let idx = len - 1 - idx;

        if *byte < 255 {
            *byte += 1;
            end.truncate(idx + 1);
            return Excluded(end.into());
        }
    }

    Unbounded
}

/// Converts a prefix to range bounds.
#[must_use]
#[expect(clippy::module_name_repetitions)]
pub fn prefix_to_range(prefix: &[u8]) -> (Bound<UserKey>, Bound<UserKey>) {
    use std::ops::Bound::{Included, Unbounded};

    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    (Included(prefix.into()), prefix_upper_range(prefix))
}

/// The iter state references the memtables used while the range is open
///
/// Because of Rust rules, the state is referenced using `self_cell`, see below.
pub struct IterState {
    pub(crate) version: SuperVersion,
    pub(crate) ephemeral: Option<(Arc<Memtable>, SeqNo)>,
}

type BoxedMerge<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + Send + 'a>;

self_cell!(
    pub struct TreeIter {
        owner: IterState,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl Iterator for TreeIter {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for TreeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

impl TreeIter {
    pub fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        guard: IterState,
        range: R,
        seqno: SeqNo,
    ) -> Self {
        Self::new(guard, |lock| {
            let lo = match range.start_bound() {
                // NOTE: See memtable.rs for range explanation
                Bound::Included(key) => Bound::Included(InternalKey::new(
                    key.as_ref(),
                    SeqNo::MAX,
                    crate::ValueType::Tombstone,
                )),
                Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                    key.as_ref(),
                    0,
                    crate::ValueType::Tombstone,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let hi = match range.end_bound() {
                // NOTE: See memtable.rs for range explanation, this is the reverse case
                // where we need to go all the way to the last seqno of an item
                //
                // Example: We search for (Unbounded..Excluded(abdef))
                //
                // key -> seqno
                //
                // a   -> 7 <<< This is the lowest key that matches the range
                // abc -> 5
                // abc -> 4
                // abc -> 3 <<< This is the highest key that matches the range
                // abcdef -> 6
                // abcdef -> 5
                //
                Bound::Included(key) => {
                    Bound::Included(InternalKey::new(key.as_ref(), 0, crate::ValueType::Value))
                }
                Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                    key.as_ref(),
                    SeqNo::MAX,
                    crate::ValueType::Value,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let range = (lo, hi);

            // Collect range tombstones from all sources for forward suppression
            let mut all_tombstones: Vec<RangeTombstone> = Vec::new();

            // From active memtable
            all_tombstones.extend(lock.version.active_memtable.range_tombstones_by_start());

            // From sealed memtables
            for memtable in lock.version.sealed_memtables.iter() {
                all_tombstones.extend(memtable.range_tombstones_by_start());
            }

            // From SST tables
            for run in lock
                .version
                .version
                .iter_levels()
                .flat_map(|lvl| lvl.iter())
            {
                for table in run.iter() {
                    if let Ok(tombstones) = table.range_tombstones_by_start_iter() {
                        all_tombstones.extend(tombstones);
                    }
                }
            }

            // Sort by (start asc, seqno desc, end asc) — the Ord impl on RangeTombstone
            all_tombstones.sort();

            let mut iters: Vec<BoxedIterator<'_>> = Vec::with_capacity(5);

            for run in lock
                .version
                .version
                .iter_levels()
                .flat_map(|lvl| lvl.iter())
            {
                match run.len() {
                    0 => {
                        // Do nothing
                    }
                    1 => {
                        #[expect(clippy::expect_used, reason = "we checked for length")]
                        let table = run.first().expect("should exist");

                        if table.check_key_range_overlap(&(
                            range.start_bound().map(|x| &*x.user_key),
                            range.end_bound().map(|x| &*x.user_key),
                        )) {
                            // Skip table if fully covered by a range tombstone
                            if is_table_fully_covered(table, &all_tombstones, seqno) {
                                continue;
                            }

                            let reader = table
                                .range((
                                    range.start_bound().map(|x| &x.user_key).cloned(),
                                    range.end_bound().map(|x| &x.user_key).cloned(),
                                ))
                                .filter(move |item| match item {
                                    Ok(item) => seqno_filter(item.key.seqno, seqno),
                                    Err(_) => true,
                                });

                            iters.push(Box::new(reader));
                        }
                    }
                    _ => {
                        if let Some(reader) = RunReader::new(
                            run.clone(),
                            (
                                range.start_bound().map(|x| &x.user_key).cloned(),
                                range.end_bound().map(|x| &x.user_key).cloned(),
                            ),
                        ) {
                            iters.push(Box::new(reader.filter(move |item| match item {
                                Ok(item) => seqno_filter(item.key.seqno, seqno),
                                Err(_) => true,
                            })));
                        }
                    }
                }
            }

            // Sealed memtables
            for memtable in lock.version.sealed_memtables.iter() {
                let iter = memtable.range(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            // Active memtable
            {
                let iter = lock.version.active_memtable.range(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            if let Some((mt, seqno)) = &lock.ephemeral {
                let iter = Box::new(
                    mt.range(range)
                        .filter(move |item| seqno_filter(item.key.seqno, *seqno))
                        .map(Ok),
                );
                iters.push(iter);
            }

            let merged = Merger::new(iters);
            let iter = MvccStream::new(merged);
            let iter = RangeTombstoneFilter::new(iter, all_tombstones, seqno);

            Box::new(iter.filter(|x| match x {
                Ok(value) => !value.key.is_tombstone(),
                Err(_) => true,
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Slice;
    use std::ops::Bound::{Excluded, Included, Unbounded};
    use test_log::test;

    fn test_prefix(prefix: &[u8], upper_bound: Bound<&[u8]>) {
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (
                match prefix {
                    _ if prefix.is_empty() => Unbounded,
                    _ => Included(Slice::from(prefix)),
                },
                upper_bound.map(Slice::from),
            ),
        );
    }

    #[test]
    fn prefix_to_range_basic() {
        test_prefix(b"abc", Excluded(b"abd"));
    }

    #[test]
    fn prefix_to_range_empty() {
        test_prefix(b"", Unbounded);
    }

    #[test]
    fn prefix_to_range_single_char() {
        test_prefix(b"a", Excluded(b"b"));
    }

    #[test]
    fn prefix_to_range_1() {
        test_prefix(&[0, 250], Excluded(&[0, 251]));
    }

    #[test]
    fn prefix_to_range_2() {
        test_prefix(&[0, 250, 50], Excluded(&[0, 250, 51]));
    }

    #[test]
    fn prefix_to_range_3() {
        test_prefix(&[255, 255, 255], Unbounded);
    }

    #[test]
    fn prefix_to_range_char_max() {
        test_prefix(&[0, 255], Excluded(&[1]));
    }

    #[test]
    fn prefix_to_range_char_max_2() {
        test_prefix(&[0, 2, 255], Excluded(&[0, 3]));
    }
}
