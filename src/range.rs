use crate::{
    levels::LevelManifest,
    memtable::MemTable,
    merge::{seqno_filter, BoxedIterator, MergeIterator},
    segment::{multi_reader::MultiReader, prefix::PrefixedReader, range::Range as RangeReader},
    tree::inner::SealedMemtables,
    value::{ParsedInternalKey, SeqNo, UserKey, UserValue, ValueType},
};
use guardian::ArcRwLockReadGuardian;
use self_cell::self_cell;
use std::{collections::VecDeque, ops::Bound, sync::Arc};

pub struct MemtableLockGuard {
    pub(crate) active: ArcRwLockReadGuardian<MemTable>,
    pub(crate) sealed: ArcRwLockReadGuardian<SealedMemtables>,
    pub(crate) ephemeral: Option<Arc<MemTable>>,
}

type BoxedMerge<'a> = Box<dyn DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'a>;

self_cell!(
    pub struct TreeIter {
        owner: MemtableLockGuard,

        #[covariant]
        dependent: BoxedMerge,
    }
);

impl Iterator for TreeIter {
    type Item = crate::Result<(UserKey, UserValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next())
    }
}

impl DoubleEndedIterator for TreeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.with_dependent_mut(|_, iter| iter.next_back())
    }
}

fn collect_disjoint_tree_with_prefix(
    level_manifest: &LevelManifest,
    prefix: &[u8],
) -> MultiReader<PrefixedReader> {
    let mut readers: Vec<_> = level_manifest
        .iter()
        .filter(|x| x.metadata.key_range.contains_prefix(prefix))
        .collect();

    readers.sort_by(|a, b| a.metadata.key_range.0.cmp(&b.metadata.key_range.0));

    let readers: VecDeque<_> = readers
        .into_iter()
        .map(|x| x.prefix(prefix))
        .collect::<VecDeque<_>>();

    MultiReader::new(readers)
}

fn collect_disjoint_tree_with_range(
    level_manifest: &LevelManifest,
    bounds: &(Bound<UserKey>, Bound<UserKey>),
) -> MultiReader<RangeReader> {
    let mut readers: Vec<_> = level_manifest
        .iter()
        .filter(|x| x.check_key_range_overlap(bounds))
        .collect();

    readers.sort_by(|a, b| a.metadata.key_range.0.cmp(&b.metadata.key_range.0));

    let readers: VecDeque<_> = readers
        .into_iter()
        .map(|x| x.range(bounds.clone()))
        .collect::<VecDeque<_>>();

    MultiReader::new(readers)
}

impl TreeIter {
    #[must_use]
    pub fn create_prefix(
        guard: MemtableLockGuard,
        prefix: &UserKey,
        seqno: Option<SeqNo>,
        level_manifest: ArcRwLockReadGuardian<LevelManifest>,
    ) -> Self {
        TreeIter::new(guard, |lock| {
            let prefix = prefix.clone();

            let mut iters: Vec<BoxedIterator<'_>> = Vec::new();

            // NOTE: Optimize disjoint trees (e.g. timeseries) to only use a single MultiReader.
            if level_manifest.is_disjoint() {
                let reader = collect_disjoint_tree_with_prefix(&level_manifest, &prefix);

                if let Some(seqno) = seqno {
                    iters.push(Box::new(reader.filter(move |item| match item {
                        Ok(item) => seqno_filter(item.seqno, seqno),
                        Err(_) => true,
                    })));
                } else {
                    iters.push(Box::new(reader));
                }
            } else {
                for level in &level_manifest.levels {
                    if level.is_disjoint {
                        let mut level = level.clone();

                        let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

                        level.sort_by_key_range();

                        for segment in &level.segments {
                            if segment.metadata.key_range.contains_prefix(&prefix) {
                                let reader = segment.prefix(&prefix);
                                readers.push_back(Box::new(reader));
                            }
                        }

                        if !readers.is_empty() {
                            let multi_reader = MultiReader::new(readers);

                            if let Some(seqno) = seqno {
                                iters.push(Box::new(multi_reader.filter(move |item| match item {
                                    Ok(item) => seqno_filter(item.seqno, seqno),
                                    Err(_) => true,
                                })));
                            } else {
                                iters.push(Box::new(multi_reader));
                            }
                        }
                    } else {
                        for segment in &level.segments {
                            if segment.metadata.key_range.contains_prefix(&prefix) {
                                let reader = segment.prefix(&prefix);

                                if let Some(seqno) = seqno {
                                    #[allow(clippy::option_if_let_else)]
                                    iters.push(Box::new(reader.filter(move |item| match item {
                                        Ok(item) => seqno_filter(item.seqno, seqno),
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
                let prefix = prefix.clone();

                let iter = memtable.prefix(prefix);

                if let Some(seqno) = seqno {
                    iters.push(Box::new(
                        iter.filter(move |item| seqno_filter(item.seqno, seqno))
                            .map(Ok),
                    ));
                } else {
                    iters.push(Box::new(iter.map(Ok)));
                }
            }

            // Active memtable
            {
                let iter = lock.active.prefix(prefix.clone());

                if let Some(seqno) = seqno {
                    iters.push(Box::new(
                        iter.filter(move |item| seqno_filter(item.seqno, seqno))
                            .map(Ok),
                    ));
                } else {
                    iters.push(Box::new(iter.map(Ok)));
                }
            }

            // Add index
            if let Some(index) = &lock.ephemeral {
                iters.push(Box::new(index.prefix(prefix).map(Ok)));
            }

            let iter = MergeIterator::new(iters).evict_old_versions(true);

            Box::new(
                #[allow(clippy::option_if_let_else)]
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
    #[allow(clippy::too_many_lines)]
    pub fn create_range(
        guard: MemtableLockGuard,
        bounds: (Bound<UserKey>, Bound<UserKey>),
        seqno: Option<SeqNo>,
        level_manifest: ArcRwLockReadGuardian<LevelManifest>,
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
                    SeqNo::MAX,
                    crate::value::ValueType::Value,
                )),
                Bound::Unbounded => Bound::Unbounded,
            };

            let range = (lo, hi);

            let mut iters: Vec<BoxedIterator<'_>> = Vec::new();

            // NOTE: Optimize disjoint trees (e.g. timeseries) to only use a single MultiReader.
            if level_manifest.is_disjoint() {
                let reader = collect_disjoint_tree_with_range(&level_manifest, &bounds);

                if let Some(seqno) = seqno {
                    iters.push(Box::new(reader.filter(move |item| match item {
                        Ok(item) => seqno_filter(item.seqno, seqno),
                        Err(_) => true,
                    })));
                } else {
                    iters.push(Box::new(reader));
                }
            } else {
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
                            let multi_reader = MultiReader::new(readers);

                            if let Some(seqno) = seqno {
                                iters.push(Box::new(multi_reader.filter(move |item| match item {
                                    Ok(item) => seqno_filter(item.seqno, seqno),
                                    Err(_) => true,
                                })));
                            } else {
                                iters.push(Box::new(multi_reader));
                            }
                        }
                    } else {
                        for segment in &level.segments {
                            if segment.check_key_range_overlap(&bounds) {
                                let reader = segment.range(bounds.clone());

                                if let Some(seqno) = seqno {
                                    #[allow(clippy::option_if_let_else)]
                                    iters.push(Box::new(reader.filter(move |item| match item {
                                        Ok(item) => seqno_filter(item.seqno, seqno),
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
                        iter.filter(move |item| seqno_filter(item.seqno, seqno))
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
                        iter.filter(move |item| seqno_filter(item.seqno, seqno))
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

            let iter = MergeIterator::new(iters).evict_old_versions(true);

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
