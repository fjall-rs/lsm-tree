// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::block_index::BlockIndex;
use super::block_index::BlockIndexImpl;
use super::id::GlobalSegmentId;
use super::reader::Reader;
use super::value_block::BlockOffset;
use super::value_block::CachePolicy;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use crate::value::InternalValue;
use crate::value::UserKey;
use crate::Slice;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

pub struct Range {
    block_index: Arc<BlockIndexImpl>,

    is_initialized: bool,

    pub(crate) range: (Bound<UserKey>, Bound<UserKey>),

    pub(crate) reader: Reader,
}

impl Range {
    pub fn new(
        data_block_boundary: BlockOffset,
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndexImpl>,
        range: (Bound<UserKey>, Bound<UserKey>),
    ) -> Self {
        let reader = Reader::new(
            data_block_boundary,
            descriptor_table,
            segment_id,
            block_cache,
            BlockOffset(0),
            None,
        );

        Self {
            is_initialized: false,

            block_index,

            reader,
            range,
        }
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.reader = self.reader.cache_policy(policy);
        self
    }

    fn initialize_lo_bound(&mut self) -> crate::Result<()> {
        let start_key = match self.range.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(start) | Bound::Excluded(start) => {
                if let Some(lower_bound) = self
                    .block_index
                    .get_lowest_block_containing_key(start, CachePolicy::Write)?
                {
                    self.reader.lo_block_offset = lower_bound;
                }

                Some(start)
            }
        };
        if let Some(key) = start_key.cloned() {
            self.reader.set_lower_bound(key);
        }
        Ok(())
    }

    fn initialize_hi_bound(&mut self) -> crate::Result<()> {
        let end_key: Option<&Slice> = match self.range.end_bound() {
            Bound::Unbounded => {
                let upper_bound = self.block_index.get_last_block_handle(CachePolicy::Write)?;

                self.reader.hi_block_offset = Some(upper_bound);

                None
            }
            Bound::Included(end) | Bound::Excluded(end) => {
                if let Some(upper_bound) = self
                    .block_index
                    .get_last_block_containing_key(end, CachePolicy::Write)?
                {
                    self.reader.hi_block_offset = Some(upper_bound);
                }

                Some(end)
            }
        };

        if let Some(key) = end_key.cloned() {
            self.reader.set_upper_bound(key);
        }
        Ok(())
    }

    fn initialize(&mut self) -> crate::Result<()> {
        // TODO: can we skip searching for lower bound until next is called at least once...?
        // would make short ranges 1.5-2x faster (if cache miss) if only one direction is used
        self.initialize_lo_bound()?;

        // TODO: can we skip searching for upper bound until next_back is called at least once...?
        // would make short ranges 1.5-2x faster (if cache miss) if only one direction is used
        self.initialize_hi_bound()?;

        self.is_initialized = true;

        Ok(())
    }
}

impl Iterator for Range {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        loop {
            let entry_result = self.reader.next()?;

            match entry_result {
                Ok(entry) => {
                    match self.range.start_bound() {
                        Bound::Included(start) => {
                            if entry.key.user_key < *start {
                                // Before min key
                                continue;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key.user_key <= *start {
                                // Before or equal min key
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    match self.range.end_bound() {
                        Bound::Included(start) => {
                            if entry.key.user_key > *start {
                                // After max key
                                return None;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key.user_key >= *start {
                                // Reached max key
                                return None;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    return Some(Ok(entry));
                }
                Err(error) => return Some(Err(error)),
            };
        }
    }
}

impl DoubleEndedIterator for Range {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        loop {
            let entry_result = self.reader.next_back()?;

            match entry_result {
                Ok(entry) => {
                    match self.range.start_bound() {
                        Bound::Included(start) => {
                            if entry.key.user_key < *start {
                                // Reached min key
                                return None;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key.user_key <= *start {
                                // Before min key
                                return None;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    match self.range.end_bound() {
                        Bound::Included(end) => {
                            if entry.key.user_key > *end {
                                // After max key
                                continue;
                            }
                        }
                        Bound::Excluded(end) => {
                            if entry.key.user_key >= *end {
                                // After or equal max key
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    return Some(Ok(entry));
                }
                Err(error) => return Some(Err(error)),
            };
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{
            block_index::{two_level_index::TwoLevelBlockIndex, BlockIndexImpl},
            range::Range,
            writer::{Options, Writer},
        },
        value::{InternalValue, UserKey, ValueType},
        Slice,
    };
    use std::ops::{
        Bound::{self, *},
        RangeBounds,
    };
    use std::sync::Arc;
    use test_log::test;

    const ITEM_COUNT: u64 = 10_000;

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_range_reader_lower_bound() -> crate::Result<()> {
        let chars = (b'a'..=b'z').collect::<Vec<_>>();

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,
            folder: folder.clone(),
            data_block_size: 1_000, // NOTE: Block size 1 to for each item to be its own block
            index_block_size: 4_096,
        })?;

        let items = chars.iter().map(|&key| {
            InternalValue::from_components(
                &[key][..],
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreezrzsernszsdaadsadsadsadsadsadsadsadsadsadsdsensnzersnzers",
                0,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = TwoLevelBlockIndex::from_file(
            segment_file_path,
            &trailer.metadata,
            &trailer.offsets,
            (0, 0).into(),
            table.clone(),
            block_cache.clone(),
        )?;
        let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

        let iter = Range::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
            (Bound::Unbounded, Bound::Unbounded),
        );
        assert_eq!(chars.len(), iter.flatten().count());

        for start_char in chars {
            let key = &[start_char][..];
            let key = Slice::from(key);

            log::debug!("{}..=z", start_char as char);

            // NOTE: Forwards
            let expected_range = (start_char..=b'z').collect::<Vec<_>>();

            let iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                (Bound::Included(key.clone()), Bound::Unbounded),
            );
            let items = iter
                .flatten()
                .map(|x| x.key.user_key.first().copied().expect("is ok"))
                .collect::<Vec<_>>();

            assert_eq!(items, expected_range);

            // NOTE: Reverse
            let expected_range = (start_char..=b'z').rev().collect::<Vec<_>>();

            let iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                (Bound::Included(key), Bound::Unbounded),
            );
            let items = iter
                .rev()
                .flatten()
                .map(|x| x.key.user_key.first().copied().expect("is ok"))
                .collect::<Vec<_>>();

            assert_eq!(items, expected_range);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_range_reader_unbounded() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,
            folder: folder.clone(),
            data_block_size: 4_096,
            index_block_size: 4_096,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            InternalValue::from_components(
                i.to_be_bytes(),
                nanoid::nanoid!().as_bytes(),
                1000 + i,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = TwoLevelBlockIndex::from_file(
            segment_file_path,
            &trailer.metadata,
            &trailer.offsets,
            (0, 0).into(),
            table.clone(),
            block_cache.clone(),
        )?;
        let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

        {
            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                range_bounds_to_tuple(&..),
            );

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key.user_key);
            }

            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                range_bounds_to_tuple(&..),
            );

            for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key.user_key);
            }
        }

        {
            log::info!("Getting every item (unbounded start)");

            let end: Slice = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                range_bounds_to_tuple::<UserKey>(&..end),
            );

            for key in (0..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key.user_key);
            }

            log::info!("Getting every item in reverse (unbounded start)");

            let end: Slice = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                range_bounds_to_tuple(&..end),
            );

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key.user_key);
            }
        }

        {
            log::info!("Getting every item (unbounded end)");

            let start: Slice = 1_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                range_bounds_to_tuple(&(start..)),
            );

            for key in (1_000..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key.user_key);
            }

            log::info!("Getting every item in reverse (unbounded end)");

            let start: Slice = 1_000_u64.to_be_bytes().into();
            let end: Slice = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
                table,
                (0, 0).into(),
                block_cache,
                block_index,
                range_bounds_to_tuple(&(start..end)),
            );

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key.user_key);
            }
        }

        Ok(())
    }

    fn range_bounds_to_tuple<T: Clone>(range: &impl RangeBounds<T>) -> (Bound<T>, Bound<T>) {
        let start_bound = match range.start_bound() {
            Included(value) => Included(value.clone()),
            Excluded(value) => Excluded(value.clone()),
            Unbounded => Unbounded,
        };

        let end_bound = match range.end_bound() {
            Included(value) => Included(value.clone()),
            Excluded(value) => Excluded(value.clone()),
            Unbounded => Unbounded,
        };

        (start_bound, end_bound)
    }

    fn bounds_u64_to_bytes(bounds: &(Bound<u64>, Bound<u64>)) -> (Bound<UserKey>, Bound<UserKey>) {
        let start_bytes = match bounds.0 {
            Included(start) => Included(start.to_be_bytes().into()),
            Excluded(start) => Excluded(start.to_be_bytes().into()),
            Unbounded => Unbounded,
        };

        let end_bytes = match bounds.1 {
            Included(end) => Included(end.to_be_bytes().into()),
            Excluded(end) => Excluded(end.to_be_bytes().into()),
            Unbounded => Unbounded,
        };

        (start_bytes, end_bytes)
    }

    fn create_range(bounds: (Bound<u64>, Bound<u64>)) -> (u64, u64) {
        let start = match bounds.0 {
            Included(value) => value,
            Excluded(value) => value + 1,
            Unbounded => 0,
        };

        let end = match bounds.1 {
            Included(value) => value + 1,
            Excluded(value) => value,
            Unbounded => u64::MAX,
        };

        (start, end)
    }

    #[test]
    fn segment_range_reader_bounded_ranges() -> crate::Result<()> {
        for data_block_size in [1, 10, 100, 200, 500, 1_000, 4_096] {
            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
                segment_id: 0,
                folder: folder.clone(),
                data_block_size,
                index_block_size: 4_096,
            })?;

            let items = (0u64..ITEM_COUNT).map(|i| {
                InternalValue::from_components(
                    i.to_be_bytes(),
                    nanoid::nanoid!().as_bytes(),
                    1000 + i,
                    ValueType::Value,
                )
            });

            for item in items {
                writer.write(item)?;
            }

            let trailer = writer.finish()?.expect("should exist");

            let segment_file_path = folder.join("0");

            let table = Arc::new(FileDescriptorTable::new(512, 1));
            table.insert(&segment_file_path, (0, 0).into());

            let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
            let block_index = TwoLevelBlockIndex::from_file(
                segment_file_path,
                &trailer.metadata,
                &trailer.offsets,
                (0, 0).into(),
                table.clone(),
                block_cache.clone(),
            )?;
            let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

            let ranges: Vec<(Bound<u64>, Bound<u64>)> = vec![
                range_bounds_to_tuple(&(0..1_000)),
                range_bounds_to_tuple(&(0..=1_000)),
                range_bounds_to_tuple(&(1_000..5_000)),
                range_bounds_to_tuple(&(1_000..=5_000)),
                range_bounds_to_tuple(&(1_000..ITEM_COUNT)),
                range_bounds_to_tuple(&..5_000),
            ];

            for bounds in ranges {
                log::info!("Bounds: {bounds:?}");

                let (start, end) = create_range(bounds);

                log::debug!("Getting every item in range");
                let range = std::ops::Range { start, end };

                let mut iter = Range::new(
                    trailer.offsets.index_block_ptr,
                    table.clone(),
                    (0, 0).into(),
                    block_cache.clone(),
                    block_index.clone(),
                    bounds_u64_to_bytes(&bounds),
                );

                for key in range.map(u64::to_be_bytes) {
                    let item = iter.next().unwrap_or_else(|| {
                        panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                    })?;

                    assert_eq!(key, &*item.key.user_key);
                }

                log::debug!("Getting every item in range in reverse");
                let range = std::ops::Range { start, end };

                let mut iter = Range::new(
                    trailer.offsets.index_block_ptr,
                    table.clone(),
                    (0, 0).into(),
                    block_cache.clone(),
                    block_index.clone(),
                    bounds_u64_to_bytes(&bounds),
                );

                for key in range.rev().map(u64::to_be_bytes) {
                    let item = iter.next_back().unwrap_or_else(|| {
                        panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                    })?;

                    assert_eq!(key, &*item.key.user_key);
                }
            }
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_range_reader_char_ranges() -> crate::Result<()> {
        let chars = (b'a'..=b'z').collect::<Vec<_>>();

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,
            folder: folder.clone(),
            data_block_size: 250,
            index_block_size: 4_096,
        })?;

        let items = chars.iter().map(|&key| {
            InternalValue::from_components(
                &[key][..],
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreezrzsernszsdaadsadsadsadsadsdsensnzersnzers",
                0,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = TwoLevelBlockIndex::from_file(
            segment_file_path,
            &trailer.metadata,
            &trailer.offsets,
            (0, 0).into(),
            table.clone(),
            block_cache.clone(),
        )?;
        let block_index = Arc::new(BlockIndexImpl::TwoLevel(block_index));

        for (i, &start_char) in chars.iter().enumerate() {
            for &end_char in chars.iter().skip(i + 1) {
                log::debug!("checking ({}, {})", start_char as char, end_char as char);

                let expected_range = (start_char..=end_char).collect::<Vec<_>>();

                let iter = Range::new(
                    trailer.offsets.index_block_ptr,
                    table.clone(),
                    (0, 0).into(),
                    block_cache.clone(),
                    block_index.clone(),
                    (
                        Included(Slice::from([start_char])),
                        Included(Slice::from([end_char])),
                    ),
                );

                let mut range = iter.flatten().map(|x| x.key.user_key);

                for &item in &expected_range {
                    assert_eq!(&*range.next().expect("should exist"), &[item]);
                }

                let iter = Range::new(
                    trailer.offsets.index_block_ptr,
                    table.clone(),
                    (0, 0).into(),
                    block_cache.clone(),
                    block_index.clone(),
                    (
                        Included(Slice::from([start_char])),
                        Included(Slice::from([end_char])),
                    ),
                );

                let mut range = iter.flatten().map(|x| x.key.user_key);

                for &item in expected_range.iter().rev() {
                    assert_eq!(&*range.next_back().expect("should exist"), &[item]);
                }
            }
        }

        Ok(())
    }
}
