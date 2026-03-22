// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    key::InternalKey,
    memtable::Memtable,
    merge::Merger,
    merge_operator::MergeOperator,
    mvcc_stream::MvccStream,
    range_tombstone::RangeTombstone,
    range_tombstone_filter::RangeTombstoneFilter,
    run_reader::RunReader,
    value::{SeqNo, UserKey},
    version::{Run, SuperVersion},
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
    pub(crate) merge_operator: Option<Arc<dyn MergeOperator>>,

    /// Optional prefix hash for prefix bloom filter skipping.
    ///
    /// When set, segments whose bloom filter reports no match for this
    /// hash will be skipped entirely during the scan.
    pub(crate) prefix_hash: Option<u64>,

    /// Optional key hash for standard bloom filter pre-filtering.
    ///
    /// When set (typically for single-key point-read pipelines), segments
    /// whose bloom filter reports no match for this hash will be skipped.
    pub(crate) key_hash: Option<u64>,

    /// Optional metrics handle for recording prefix-related statistics (e.g. bloom skips).
    ///
    /// `None` when the caller does not wish to record metrics; this is
    /// independent of whether the iterator uses a prefix.
    #[cfg(feature = "metrics")]
    pub(crate) metrics: Option<Arc<crate::Metrics>>,
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

fn range_tombstone_overlaps_bounds(
    rt: &RangeTombstone,
    bounds: &(Bound<UserKey>, Bound<UserKey>),
) -> bool {
    let overlaps_lo = match &bounds.0 {
        Bound::Included(key) | Bound::Excluded(key) => rt.end.as_ref() > key.as_ref(),
        Bound::Unbounded => true,
    };

    let overlaps_hi = match &bounds.1 {
        Bound::Included(key) => rt.start.as_ref() <= key.as_ref(),
        Bound::Excluded(key) => rt.start.as_ref() < key.as_ref(),
        Bound::Unbounded => true,
    };

    overlaps_lo && overlaps_hi
}

/// Checks prefix and key bloom filters for a table.
///
/// Returns `true` if the table should be included (bloom says "maybe" or no
/// filter available), `false` if it can be safely skipped.
fn bloom_passes(state: &IterState, table: &crate::table::Table) -> bool {
    if let Some(prefix_hash) = state.prefix_hash {
        match table.maybe_contains_prefix(prefix_hash) {
            Ok(false) => {
                #[cfg(feature = "metrics")]
                if let Some(m) = &state.metrics {
                    m.prefix_bloom_skips
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                return false;
            }
            Err(e) => {
                log::debug!("prefix bloom check failed for table {:?}: {e}", table.id(),);
            }
            _ => {}
        }
    }

    if let Some(key_hash) = state.key_hash {
        match table.bloom_may_contain_key_hash(key_hash) {
            Ok(false) => return false,
            Err(e) => {
                log::debug!("key bloom check failed for table {:?}: {e}", table.id(),);
            }
            _ => {}
        }
    }

    true
}

impl TreeIter {
    #[expect(
        clippy::too_many_lines,
        reason = "create_range wires up multiple iterator sources, filters, and tombstone handling; splitting further would reduce clarity"
    )]
    pub fn create_range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        guard: IterState,
        range: R,
        seqno: SeqNo,
    ) -> Self {
        Self::new(guard, |lock| {
            let user_range = (
                match range.start_bound() {
                    Bound::Included(key) => Bound::Included(UserKey::from(key.as_ref())),
                    Bound::Excluded(key) => Bound::Excluded(UserKey::from(key.as_ref())),
                    Bound::Unbounded => Bound::Unbounded,
                },
                match range.end_bound() {
                    Bound::Included(key) => Bound::Included(UserKey::from(key.as_ref())),
                    Bound::Excluded(key) => Bound::Excluded(UserKey::from(key.as_ref())),
                    Bound::Unbounded => Bound::Unbounded,
                },
            );

            let range = (
                match &user_range.0 {
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
                },
                match &user_range.1 {
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
                },
            );

            let mut iters: Vec<BoxedIterator<'_>> = Vec::with_capacity(5);
            // Each RT is paired with the per-source visibility cutoff so that
            // ephemeral memtable RTs use their own index_seqno instead of the
            // outer scan seqno (see issue #33).
            let mut all_range_tombstones: Vec<(RangeTombstone, SeqNo)> = Vec::new();
            let mut single_tables = Vec::new();
            let mut multi_runs = Vec::new();

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

                        all_range_tombstones.extend(
                            table
                                .range_tombstones()
                                .iter()
                                .filter(|rt| range_tombstone_overlaps_bounds(rt, &user_range))
                                .map(|rt| (rt.clone(), seqno)),
                        );

                        // Check key range overlap first (cheap metadata check) before
                        // running the O(rt_count) table-skip scan.
                        if table.check_key_range_overlap(&(
                            user_range.0.as_ref().map(std::convert::AsRef::as_ref),
                            user_range.1.as_ref().map(std::convert::AsRef::as_ref),
                        )) && bloom_passes(lock, table)
                        {
                            single_tables.push(table.clone());
                        }
                    }
                    _ => {
                        // Collect range tombstones from ALL tables in the run
                        // regardless of bloom filtering — they may affect keys
                        // in other tables/levels.
                        for table in run.iter() {
                            all_range_tombstones.extend(
                                table
                                    .range_tombstones()
                                    .iter()
                                    .filter(|rt| range_tombstone_overlaps_bounds(rt, &user_range))
                                    .map(|rt| (rt.clone(), seqno)),
                            );
                        }

                        // If a prefix or key hash is available, filter individual
                        // tables within the multi-table run using their bloom
                        // filters. This covers both prefix scans (prefix_hash)
                        // and point-read merge pipelines (key_hash).
                        if lock.prefix_hash.is_some() || lock.key_hash.is_some() {
                            let bounds = (
                                user_range.0.as_ref().map(std::convert::AsRef::as_ref),
                                user_range.1.as_ref().map(std::convert::AsRef::as_ref),
                            );

                            let surviving: Vec<_> = run
                                .iter()
                                .filter(|table| {
                                    // Cheap key-range metadata check first to avoid
                                    // bloom filter I/O for non-overlapping tables.
                                    if !table.check_key_range_overlap(&bounds) {
                                        return false;
                                    }

                                    bloom_passes(lock, table)
                                })
                                .cloned()
                                .collect();

                            match surviving.len() {
                                0 => {
                                    // All tables in this run were filtered out.
                                }
                                1 => {
                                    // Demote to single-table path so it also
                                    // benefits from the range-tombstone table-skip
                                    // optimization below.
                                    if let Some(table) = surviving.into_iter().next() {
                                        single_tables.push(table);
                                    }
                                }
                                _ => {
                                    // surviving.len() >= 2, so Run::new cannot
                                    // return None (only empty vecs yield None).
                                    #[expect(
                                        clippy::expect_used,
                                        reason = "Run::new returns None only for empty vecs"
                                    )]
                                    let new_run =
                                        Run::new(surviving).expect("non-empty surviving tables");
                                    multi_runs.push(Arc::new(new_run));
                                }
                            }
                        } else {
                            multi_runs.push(run.clone());
                        }
                    }
                }
            }

            // Sort SST-sourced RTs by start key for binary search in
            // table-skip below. This is intentionally a separate sort from
            // the full sort+dedup later: table-skip runs here (before memtable
            // RTs are collected), so only SST RTs are present. The later sort
            // covers the complete list. Both sorts are O(n log n) on their
            // respective subsets; the SST-only subset is typically small.
            all_range_tombstones.sort_unstable_by(|(a, _), (b, _)| a.start.cmp(&b.start));

            for table in single_tables {
                // Table-skip: if a range tombstone fully covers this table
                // with a higher seqno, skip it entirely (avoid I/O).
                //
                // Uses get_highest_kv_seqno() which excludes RT seqnos, so a
                // covering RT stored in the same table can now trigger skip.
                //
                // Binary search on sorted RT list: partition_point finds the
                // first RT with start > table_min; only the prefix [0..idx]
                // can have start <= table_min (required for fully_covers).
                // key_range.max() is inclusive; fully_covers checks max < rt.end
                // (half-open), so this is correct for inclusive upper bounds.
                let table_min: &[u8] = table.metadata.key_range.min().as_ref();
                let table_max: &[u8] = table.metadata.key_range.max().as_ref();
                let table_kv_seqno = table.get_highest_kv_seqno();

                let candidate_end =
                    all_range_tombstones.partition_point(|(rt, _)| rt.start.as_ref() <= table_min);

                let is_covered =
                    all_range_tombstones
                        .iter()
                        .take(candidate_end)
                        .any(|(rt, cutoff)| {
                            rt.visible_at(*cutoff)
                                && rt.fully_covers(table_min, table_max)
                                && rt.seqno > table_kv_seqno
                        });

                if !is_covered {
                    let reader = table
                        .range(user_range.clone())
                        .filter(move |item| match item {
                            Ok(item) => seqno_filter(item.key.seqno, seqno),
                            Err(_) => true,
                        });

                    iters.push(Box::new(reader));
                }
            }

            for run in multi_runs {
                if let Some(reader) = RunReader::new(run, user_range.clone()) {
                    iters.push(Box::new(reader.filter(move |item| match item {
                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                        Err(_) => true,
                    })));
                }
            }

            // Sealed memtables
            for memtable in lock.version.sealed_memtables.iter() {
                all_range_tombstones.extend(
                    memtable
                        .range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| range_tombstone_overlaps_bounds(rt, &user_range))
                        .map(|rt| (rt, seqno)),
                );

                let iter = memtable.range(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            // Active memtable
            {
                all_range_tombstones.extend(
                    lock.version
                        .active_memtable
                        .range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| range_tombstone_overlaps_bounds(rt, &user_range))
                        .map(|rt| (rt, seqno)),
                );

                let iter = lock.version.active_memtable.range(range.clone());

                iters.push(Box::new(
                    iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                        .map(Ok),
                ));
            }

            if let Some((mt, eph_seqno)) = &lock.ephemeral {
                all_range_tombstones.extend(
                    mt.range_tombstones_sorted()
                        .into_iter()
                        .filter(|rt| range_tombstone_overlaps_bounds(rt, &user_range))
                        .map(|rt| (rt, *eph_seqno)),
                );

                let iter = Box::new(
                    mt.range(range)
                        .filter(move |item| seqno_filter(item.key.seqno, *eph_seqno))
                        .map(Ok),
                );
                iters.push(iter);
            }

            let merged = Merger::new(iters);
            // Clone needed: MvccStream uses the RT set for merge suppression,
            // while RangeTombstoneFilter below consumes it for post-merge
            // filtering. An Arc<[_]> could avoid the copy if RT sets grow large.
            let iter = MvccStream::new(merged, lock.merge_operator.clone())
                .with_range_tombstones(all_range_tombstones.clone());

            let iter = iter.filter(|x| match x {
                Ok(value) => !value.key.is_tombstone(),
                Err(_) => true,
            });

            // Deduplicate: MultiWriter rotation copies the same RTs into each
            // output table, so collected tombstones can contain duplicates.
            // When the same RT appears from different sources with different
            // cutoffs (e.g., persisted SST + ephemeral), keep the max cutoff
            // so the RT stays visible if ANY source's snapshot includes it.
            all_range_tombstones.sort_by(|a, b| a.0.cmp(&b.0));
            all_range_tombstones.dedup_by(|a, b| {
                if a.0 == b.0 {
                    // dedup_by passes (a=later, b=earlier); b survives, a is
                    // removed.  Merge a's cutoff into the surviving b.
                    b.1 = b.1.max(a.1);
                    true
                } else {
                    false
                }
            });

            // Fast path: skip filter wrapping when no tombstone is visible at
            // its per-source cutoff. Each RT carries the seqno of its originating
            // source, so the check is per-RT rather than global.
            if all_range_tombstones
                .iter()
                .all(|(rt, cutoff)| !rt.visible_at(*cutoff))
            {
                Box::new(iter)
            } else {
                Box::new(RangeTombstoneFilter::new(iter, all_range_tombstones))
            }
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
