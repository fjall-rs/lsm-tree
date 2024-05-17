use crate::{
    levels::LevelManifest,
    memtable::MemTable,
    merge::{BoxedIterator, MergeIterator},
    segment::multi_reader::MultiReader,
    tree_inner::SealedMemtables,
    value::{ParsedInternalKey, SeqNo, UserKey, UserValue, ValueType},
    Value,
};
use self_cell::self_cell;
use std::{collections::VecDeque, ops::Bound, sync::RwLockReadGuard};

pub struct MemtableLockGuard<'a> {
    pub(crate) active: RwLockReadGuard<'a, MemTable>,
    pub(crate) sealed: RwLockReadGuard<'a, SealedMemtables>,
}

type BoxedMerge<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'a>;

self_cell!(
    pub struct TreeIter<'a> {
        owner: MemtableLockGuard<'a>,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl<'a> Iterator for TreeIter<'a> {
    type Item = crate::Result<(UserKey, UserValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl<'a> DoubleEndedIterator for TreeIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

impl<'a> TreeIter<'a> {
    #[must_use]
    pub fn create_prefix(
        guard: MemtableLockGuard<'a>,
        prefix: UserKey,
        seqno: Option<SeqNo>,
        level_manifest: RwLockReadGuard<'a, LevelManifest>,
    ) -> Self {
        TreeIter::new(guard, |lock| {
            let prefix = prefix.clone();

            let mut segment_iters: Vec<BoxedIterator<'_>> =
                Vec::with_capacity(level_manifest.len());

            for level in &level_manifest.levels {
                if level.is_disjoint {
                    let mut level = level.clone();

                    let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

                    level.sort_by_key_range();

                    for segment in &level.segments {
                        if segment.metadata.key_range.contains_prefix(&prefix) {
                            let range = segment.prefix(prefix.clone());
                            readers.push_back(Box::new(range));
                        }
                    }

                    if !readers.is_empty() {
                        segment_iters.push(Box::new(MultiReader::new(readers)));
                    }
                } else {
                    for segment in &level.segments {
                        if segment.metadata.key_range.contains_prefix(&prefix) {
                            segment_iters.push(Box::new(segment.prefix(prefix.clone())));
                        }
                    }
                }
            }

            drop(level_manifest);

            let mut iters: Vec<_> = segment_iters;

            for (_, memtable) in lock.sealed.iter() {
                let prefix = prefix.clone();

                iters.push(Box::new(
                    memtable
                        .items
                        // NOTE: See memtable.rs for range explanation
                        .range(
                            ParsedInternalKey::new(
                                prefix.clone(),
                                SeqNo::MAX,
                                ValueType::Tombstone,
                            )..,
                        )
                        .filter(move |entry| entry.key().user_key.starts_with(&prefix))
                        .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone())))),
                ));
            }

            let memtable_iter = {
                lock.active
                    .items
                    .range(
                        ParsedInternalKey::new(prefix.clone(), SeqNo::MAX, ValueType::Tombstone)..,
                    )
                    .filter(move |entry| entry.key().user_key.starts_with(&prefix))
                    .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone()))))
            };

            iters.push(Box::new(memtable_iter));

            let mut iter = MergeIterator::new(iters).evict_old_versions(true);

            if let Some(seqno) = seqno {
                iter = iter.snapshot_seqno(seqno);
            }

            Box::new(
                iter.filter(|x| match x {
                    Ok(value) => value.value_type != ValueType::Tombstone,
                    Err(_) => true,
                })
                .map(|item| match item {
                    Ok(kv) => Ok((kv.key, kv.value)),
                    Err(e) => Err(e),
                }),
            )
        })
    }

    #[must_use]
    pub fn create_range(
        guard: MemtableLockGuard<'a>,
        bounds: (Bound<UserKey>, Bound<UserKey>),
        seqno: Option<SeqNo>,
        level_manifest: RwLockReadGuard<'a, LevelManifest>,
    ) -> Self {
        TreeIter::new(guard, |lock| {
            let lo = match &bounds.0 {
                // NOTE: See memtable.rs for range explanation
                Bound::Included(key) => Bound::Included(ParsedInternalKey::new(
                    key.clone(),
                    SeqNo::MAX,
                    crate::value::ValueType::Tombstone,
                )),
                Bound::Excluded(key) => Bound::Excluded(ParsedInternalKey::new(
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
                Bound::Included(key) => Bound::Included(ParsedInternalKey::new(
                    key.clone(),
                    0,
                    crate::value::ValueType::Value,
                )),
                Bound::Excluded(key) => Bound::Excluded(ParsedInternalKey::new(
                    key.clone(),
                    0,
                    crate::value::ValueType::Value,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            eprintln!("{hi:?}");

            let range = (lo, hi);

            let mut segment_iters: Vec<BoxedIterator<'_>> =
                Vec::with_capacity(level_manifest.len());

            for level in &level_manifest.levels {
                if level.is_disjoint {
                    let mut level = level.clone();

                    let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

                    level.sort_by_key_range();

                    for segment in &level.segments {
                        if segment.check_key_range_overlap(&bounds) {
                            let range = segment.range(bounds.clone());
                            readers.push_back(Box::new(range));
                        }
                    }

                    if !readers.is_empty() {
                        segment_iters.push(Box::new(MultiReader::new(readers)));
                    }
                } else {
                    for segment in &level.segments {
                        if segment.check_key_range_overlap(&bounds) {
                            segment_iters.push(Box::new(segment.range(bounds.clone())));
                        }
                    }
                }
            }

            drop(level_manifest);

            let mut iters: Vec<_> = segment_iters;

            for (_, memtable) in lock.sealed.iter() {
                iters.push(Box::new(memtable.items.range(range.clone()).map(|entry| {
                    Ok(Value::from((entry.key().clone(), entry.value().clone())))
                })));
            }

            let memtable_iter = {
                lock.active
                    .items
                    .range(range)
                    .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone()))))
            };

            iters.push(Box::new(memtable_iter));

            let mut iter = MergeIterator::new(iters).evict_old_versions(true);

            if let Some(seqno) = seqno {
                iter = iter.snapshot_seqno(seqno);
            }

            Box::new(
                iter.filter(|x| match x {
                    Ok(value) => value.value_type != ValueType::Tombstone,
                    Err(_) => true,
                })
                .map(|item| match item {
                    Ok(kv) => Ok((kv.key, kv.value)),
                    Err(e) => Err(e),
                }),
            )
        })
    }
}
