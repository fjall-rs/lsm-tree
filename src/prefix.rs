use crate::{
    levels::LevelManifest,
    merge::{BoxedIterator, MergeIterator},
    range::MemTableGuard,
    segment::multi_reader::MultiReader,
    value::{ParsedInternalKey, SeqNo, UserKey, UserValue, ValueType},
    Value,
};
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
};

pub struct Prefix {
    guard: MemTableGuard,
    prefix: UserKey,
    seqno: Option<SeqNo>,
    level_manifest: Arc<RwLock<LevelManifest>>,
}

impl Prefix {
    #[must_use]
    pub fn new(
        guard: MemTableGuard,
        prefix: UserKey,
        seqno: Option<SeqNo>,
        level_manifest: Arc<RwLock<LevelManifest>>,
    ) -> Self {
        Self {
            guard,
            prefix,
            seqno,
            level_manifest,
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct PrefixIterator<'a> {
    iter: BoxedIterator<'a>,
}

impl<'a> PrefixIterator<'a> {
    fn new(lock: &'a Prefix, seqno: Option<SeqNo>) -> Self {
        let level_manifest = lock.level_manifest.read().expect("lock is poisoned");
        let mut segment_iters: Vec<BoxedIterator<'_>> = Vec::with_capacity(level_manifest.len());

        for level in &level_manifest.levels {
            if level.is_disjoint {
                let mut level = level.clone();

                let mut readers: VecDeque<BoxedIterator<'_>> = VecDeque::new();

                level.sort_by_key_range();

                for segment in &level.segments {
                    if segment.metadata.key_range.contains_prefix(&lock.prefix) {
                        let range = segment.prefix(lock.prefix.clone());
                        readers.push_back(Box::new(range));
                    }
                }

                if !readers.is_empty() {
                    segment_iters.push(Box::new(MultiReader::new(readers)));
                }
            } else {
                for segment in &level.segments {
                    if segment.metadata.key_range.contains_prefix(&lock.prefix) {
                        segment_iters.push(Box::new(segment.prefix(lock.prefix.clone())));
                    }
                }
            }
        }

        drop(level_manifest);

        let mut iters: Vec<BoxedIterator<'a>> = segment_iters;

        for memtable in lock.guard.sealed.values() {
            iters.push(Box::new(
                memtable
                    .items
                    // NOTE: See memtable.rs for range explanation
                    .range(
                        ParsedInternalKey::new(
                            lock.prefix.clone(),
                            SeqNo::MAX,
                            ValueType::Tombstone,
                        )..,
                    )
                    .filter(|entry| entry.key().user_key.starts_with(&lock.prefix))
                    .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone())))),
            ));
        }

        let memtable_iter = {
            lock.guard
                .active
                .items
                .range(
                    ParsedInternalKey::new(lock.prefix.clone(), SeqNo::MAX, ValueType::Tombstone)..,
                )
                .filter(|entry| entry.key().user_key.starts_with(&lock.prefix))
                .map(|entry| Ok(Value::from((entry.key().clone(), entry.value().clone()))))
        };

        iters.push(Box::new(memtable_iter));

        let mut iter = MergeIterator::new(iters).evict_old_versions(true);

        if let Some(seqno) = seqno {
            iter = iter.snapshot_seqno(seqno);
        }

        let iter = Box::new(iter.filter(|x| match x {
            Ok(value) => value.value_type != ValueType::Tombstone,
            Err(_) => true,
        }));

        Self { iter }
    }
}

impl<'a> Iterator for PrefixIterator<'a> {
    type Item = crate::Result<(UserKey, UserValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.iter.next()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> DoubleEndedIterator for PrefixIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        Some(self.iter.next_back()?.map(|x| (x.key, x.value)))
    }
}

impl<'a> IntoIterator for &'a Prefix {
    type IntoIter = PrefixIterator<'a>;
    type Item = <Self::IntoIter as Iterator>::Item;

    fn into_iter(self) -> Self::IntoIter {
        PrefixIterator::new(self, self.seqno)
    }
}
