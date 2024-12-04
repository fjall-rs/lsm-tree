// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    key::InternalKey,
    level_manifest::LevelManifest,
    memtable::Memtable,
    merge::{BoxedIterator, Merger},
    multi_reader::MultiReader,
    mvcc_stream::MvccStream,
    segment::{level_reader::LevelReader, value_block::CachePolicy},
    tree::inner::SealedMemtables,
    value::{SeqNo, UserKey},
    InternalValue,
};
use guardian::ArcRwLockReadGuardian;
use self_cell::self_cell;
use std::{ops::Bound, sync::Arc};

#[must_use]
pub fn seqno_filter(item_seqno: SeqNo, seqno: SeqNo) -> bool {
    item_seqno < seqno
}

#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn prefix_to_range(prefix: &[u8]) -> (Bound<UserKey>, Bound<UserKey>) {
    use std::ops::Bound::{Excluded, Included, Unbounded};

    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    let mut end = prefix.to_vec();
    let len = end.len();

    for (idx, byte) in end.iter_mut().rev().enumerate() {
        let idx = len - 1 - idx;

        if *byte < 255 {
            *byte += 1;
            end.truncate(idx + 1);
            return (Included(prefix.into()), Excluded(end.into()));
        }
    }

    (Included(prefix.into()), Unbounded)
}

pub struct MemtableLockGuard {
    pub(crate) active: ArcRwLockReadGuardian<Memtable>,
    pub(crate) sealed: ArcRwLockReadGuardian<SealedMemtables>,
    pub(crate) ephemeral: Option<Arc<Memtable>>,
}

type BoxedMerge<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<InternalValue>> + 'a>;

self_cell!(
    pub struct TreeIter {
        owner: MemtableLockGuard,

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

fn collect_disjoint_tree_with_range(
    level_manifest: &LevelManifest,
    bounds: &(Bound<UserKey>, Bound<UserKey>),
) -> MultiReader<LevelReader> {
    debug_assert!(level_manifest.is_disjoint());

    let mut levels = level_manifest
        .levels
        .iter()
        .filter(|x| !x.is_empty())
        .cloned()
        .collect::<Vec<_>>();

    // TODO: save key range per level, makes key range sorting easier
    // and can remove levels not needed

    // NOTE: We know the levels are disjoint to each other, so we can just sort
    // them by comparing the first segment
    //
    // NOTE: Also, we filter out levels that are empty, so expect is fine
    #[allow(clippy::expect_used)]
    levels.sort_by(|a, b| {
        a.segments
            .first()
            .expect("level should not be empty")
            .metadata
            .key_range
            .0
            .cmp(
                &b.segments
                    .first()
                    .expect("level should not be empty")
                    .metadata
                    .key_range
                    .0,
            )
    });

    let readers = levels
        .into_iter()
        .map(|lvl| LevelReader::new(lvl, bounds, CachePolicy::Write))
        .collect();

    MultiReader::new(readers)
}

impl TreeIter {
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn create_range(
        guard: MemtableLockGuard,
        bounds: (Bound<UserKey>, Bound<UserKey>),
        seqno: Option<SeqNo>,
        level_manifest: ArcRwLockReadGuardian<LevelManifest>,
    ) -> Self {
        Self::new(guard, |lock| {
            let lo = match &bounds.0 {
                // NOTE: See memtable.rs for range explanation
                Bound::Included(key) => Bound::Included(InternalKey::new(
                    key.clone(),
                    SeqNo::MAX,
                    crate::value::ValueType::Tombstone,
                )),
                Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                    key.clone(),
                    0,
                    crate::value::ValueType::Tombstone,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let hi = match &bounds.1 {
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
                Bound::Included(key) => Bound::Included(InternalKey::new(
                    key.clone(),
                    0,
                    crate::value::ValueType::Value,
                )),
                Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
                    key.clone(),
                    SeqNo::MAX,
                    crate::value::ValueType::Value,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let range = (lo, hi);

            let mut iters: Vec<BoxedIterator<'_>> = Vec::with_capacity(5);

            // NOTE: Optimize disjoint trees (e.g. timeseries) to only use a single MultiReader.
            if level_manifest.is_disjoint() {
                let reader = collect_disjoint_tree_with_range(&level_manifest, &bounds);

                if let Some(seqno) = seqno {
                    iters.push(Box::new(reader.filter(move |item| match item {
                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                        Err(_) => true,
                    })));
                } else {
                    iters.push(Box::new(reader));
                }
            } else {
                for level in &level_manifest.levels {
                    if level.is_disjoint {
                        if !level.is_empty() {
                            let reader =
                                LevelReader::new(level.clone(), &bounds, CachePolicy::Write);

                            if let Some(seqno) = seqno {
                                iters.push(Box::new(reader.filter(move |item| match item {
                                    Ok(item) => seqno_filter(item.key.seqno, seqno),
                                    Err(_) => true,
                                })));
                            } else {
                                iters.push(Box::new(reader));
                            }
                        }
                    } else {
                        for segment in &level.segments {
                            if segment.check_key_range_overlap(&bounds) {
                                let reader = segment.range(bounds.clone());

                                if let Some(seqno) = seqno {
                                    iters.push(Box::new(reader.filter(move |item| match item {
                                        Ok(item) => seqno_filter(item.key.seqno, seqno),
                                        Err(_) => true,
                                    })));
                                } else {
                                    iters.push(Box::new(reader));
                                }
                            }
                        }
                    }
                }
            };

            drop(level_manifest);

            // Sealed memtables
            for (_, memtable) in lock.sealed.iter() {
                let iter = memtable.range(range.clone());

                if let Some(seqno) = seqno {
                    iters.push(Box::new(
                        iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                            .map(Ok),
                    ));
                } else {
                    iters.push(Box::new(iter.map(Ok)));
                }
            }

            // Active memtable
            {
                let iter = lock.active.range(range.clone());

                if let Some(seqno) = seqno {
                    iters.push(Box::new(
                        iter.filter(move |item| seqno_filter(item.key.seqno, seqno))
                            .map(Ok),
                    ));
                } else {
                    iters.push(Box::new(iter.map(Ok)));
                }
            }

            if let Some(index) = &lock.ephemeral {
                let iter = Box::new(index.range(range).map(Ok));
                iters.push(iter);
            }

            let merged = Merger::new(iters);
            let iter = MvccStream::new(merged);

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
                // TODO: Bound::map 1.77
                match upper_bound {
                    Unbounded => Unbounded,
                    Included(x) => Included(Slice::from(x)),
                    Excluded(x) => Excluded(Slice::from(x)),
                }
            )
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
