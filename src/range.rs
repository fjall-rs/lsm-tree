use crate::{
    levels::LevelManifest,
    memtable::MemTable,
    merge::{seqno_filter, BoxedIterator, MergeIterator},
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

/* fn filter_by_seqno(item_seqno: SeqNo, seqno: Option<SeqNo>) -> bool {
    seqno.map_or(true, |seqno| item_seqno < seqno)
} */

impl<'a> TreeIter<'a> {
    #[must_use]
    pub fn create_prefix(
        guard: MemtableLockGuard<'a>,
        prefix: &UserKey,
        seqno: Option<SeqNo>,
        level_manifest: RwLockReadGuard<'a, LevelManifest>,
        add_index: Option<&'a MemTable>,
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
                            let reader = segment.prefix(prefix.clone());
                            readers.push_back(Box::new(reader));
                        }
                    }

                    if !readers.is_empty() {
                        let multi_reader = MultiReader::new(readers);

                        if let Some(seqno) = seqno {
                            segment_iters.push(Box::new(multi_reader.filter(
                                move |item| match item {
                                    Ok(item) => seqno_filter(item.seqno, seqno),
                                    Err(_) => true,
                                },
                            )));
                        } else {
                            segment_iters.push(Box::new(multi_reader));
                        }
                    }
                } else {
                    for segment in &level.segments {
                        if segment.metadata.key_range.contains_prefix(&prefix) {
                            let reader = segment.prefix(prefix.clone());

                            if let Some(seqno) = seqno {
                                #[allow(clippy::option_if_let_else)]
                                segment_iters.push(Box::new(reader.filter(
                                    move |item| match item {
                                        Ok(item) => seqno_filter(item.seqno, seqno),
                                        Err(_) => true,
                                    },
                                )));
                            } else {
                                segment_iters.push(Box::new(reader));
                            }
                        }
                    }
                }
            }

            drop(level_manifest);

            let mut iters: Vec<_> = segment_iters;

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
            if let Some(index) = add_index {
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
        guard: MemtableLockGuard<'a>,
        bounds: (Bound<UserKey>, Bound<UserKey>),
        seqno: Option<SeqNo>,
        level_manifest: RwLockReadGuard<'a, LevelManifest>,
        add_index: Option<&'a MemTable>,
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
                        let multi_reader = MultiReader::new(readers);

                        if let Some(seqno) = seqno {
                            segment_iters.push(Box::new(multi_reader.filter(
                                move |item| match item {
                                    Ok(item) => seqno_filter(item.seqno, seqno),
                                    Err(_) => true,
                                },
                            )));
                        } else {
                            segment_iters.push(Box::new(multi_reader));
                        }
                    }
                } else {
                    for segment in &level.segments {
                        if segment.check_key_range_overlap(&bounds) {
                            let reader = segment.range(bounds.clone());

                            if let Some(seqno) = seqno {
                                #[allow(clippy::option_if_let_else)]
                                segment_iters.push(Box::new(reader.filter(
                                    move |item| match item {
                                        Ok(item) => seqno_filter(item.seqno, seqno),
                                        Err(_) => true,
                                    },
                                )));
                            } else {
                                segment_iters.push(Box::new(reader));
                            }
                        }
                    }
                }
            }

            drop(level_manifest);

            let mut iters: Vec<_> = segment_iters;

            // Sealed memtables
            for (_, memtable) in lock.sealed.iter() {
                let iter = memtable
                    .items
                    .range(range.clone())
                    .map(|entry| Value::from((entry.key().clone(), entry.value().clone())));

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
                let iter = lock
                    .active
                    .items
                    .range(range.clone())
                    .map(|entry| Value::from((entry.key().clone(), entry.value().clone())));

                if let Some(seqno) = seqno {
                    iters.push(Box::new(
                        iter.filter(move |item| seqno_filter(item.seqno, seqno))
                            .map(Ok),
                    ));
                } else {
                    iters.push(Box::new(iter.map(Ok)));
                }
            }

            if let Some(index) = add_index {
                let iter =
                    Box::new(index.items.range(range).map(|entry| {
                        Ok(Value::from((entry.key().clone(), entry.value().clone())))
                    }));

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
