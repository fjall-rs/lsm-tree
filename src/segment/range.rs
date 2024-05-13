use super::block::CachePolicy;
use super::block_index::BlockIndex;
use super::id::GlobalSegmentId;
use super::reader::Reader;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use crate::value::UserKey;
use crate::Value;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

pub struct Range {
    descriptor_table: Arc<FileDescriptorTable>,
    block_index: Arc<BlockIndex>,
    block_cache: Arc<BlockCache>,
    segment_id: GlobalSegmentId,

    range: (Bound<UserKey>, Bound<UserKey>),

    iterator: Option<Reader>,

    cache_policy: CachePolicy,
}

impl Range {
    pub fn new(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        range: (Bound<UserKey>, Bound<UserKey>),
    ) -> Self {
        Self {
            descriptor_table,
            block_cache,
            block_index,
            segment_id,

            iterator: None,
            range,

            cache_policy: CachePolicy::Write,
        }
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    // TODO: may not need initialize function anymore, just do in constructor...
    fn initialize(&mut self) -> crate::Result<()> {
        let start_key = match self.range.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(start) | Bound::Excluded(start) => Some(start),
        };

        let end_key: Option<&Arc<[u8]>> = match self.range.end_bound() {
            Bound::Unbounded => None,
            Bound::Included(end) | Bound::Excluded(end) => Some(end),
        };

        let mut reader = Reader::new(
            self.descriptor_table.clone(),
            self.segment_id,
            self.block_cache.clone(),
            self.block_index.clone(),
        )
        .cache_policy(self.cache_policy);

        if let Some(key) = start_key.cloned() {
            reader = reader.set_lower_bound(key);
        }
        if let Some(key) = end_key.cloned() {
            reader = reader.set_upper_bound(key);
        }

        self.iterator = Some(reader);

        Ok(())
    }
}

impl Iterator for Range {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterator.is_none() {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        loop {
            let entry_result = self
                .iterator
                .as_mut()
                .expect("should be initialized")
                .next()?;

            match entry_result {
                Ok(entry) => {
                    match self.range.start_bound() {
                        Bound::Included(start) => {
                            if entry.key < *start {
                                // Before min key
                                continue;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key <= *start {
                                // Before or equal min key
                                continue;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    match self.range.end_bound() {
                        Bound::Included(start) => {
                            if entry.key > *start {
                                // After max key
                                return None;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key >= *start {
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
        if self.iterator.is_none() {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        loop {
            let entry_result = self
                .iterator
                .as_mut()
                .expect("should be initialized")
                .next_back()?;

            match entry_result {
                Ok(entry) => {
                    match self.range.start_bound() {
                        Bound::Included(start) => {
                            if entry.key < *start {
                                // Reached min key
                                return None;
                            }
                        }
                        Bound::Excluded(start) => {
                            if entry.key <= *start {
                                // Before min key
                                return None;
                            }
                        }
                        Bound::Unbounded => {}
                    }

                    match self.range.end_bound() {
                        Bound::Included(end) => {
                            if entry.key > *end {
                                // After max key
                                continue;
                            }
                        }
                        Bound::Excluded(end) => {
                            if entry.key >= *end {
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
mod tests {
    use super::Reader as SegmentReader;
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        file::BLOCKS_FILE,
        segment::{
            block_index::BlockIndex,
            meta::Metadata,
            range::Range,
            writer::{Options, Writer},
        },
        value::{UserKey, ValueType},
        Value,
    };
    use std::ops::{
        Bound::{self, *},
        RangeBounds,
    };
    use std::sync::Arc;
    use test_log::test;

    const ITEM_COUNT: u64 = 50_000;

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_range_reader_lower_bound() -> crate::Result<()> {
        let chars = (b'a'..=b'z').collect::<Vec<_>>();

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 1000, // NOTE: Block size 1 to for each item to be its own block

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = chars.iter().map(|&key| {
            Value::new(
                &[key][..],
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreezrzsernszsdaadsadsadsadsadsadsadsadsadsadsdsensnzersnzers",
                0,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(0, writer)?;
        metadata.write_to_file(&folder)?;

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(folder.join(BLOCKS_FILE), (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            (0, 0).into(),
            table.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        let iter = Range::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
            (Bound::Unbounded, Bound::Unbounded),
        );
        assert_eq!(chars.len(), iter.flatten().count());

        // TODO: reverse

        for start_char in chars {
            let key = &[start_char][..];
            let key: Arc<[u8]> = Arc::from(key);

            let iter = Range::new(
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
                (Bound::Included(key), Bound::Unbounded),
            );

            let items = iter
                .flatten()
                .map(|x| x.key.first().copied().expect("is ok"))
                .collect::<Vec<_>>();

            let expected_range = (start_char..=b'z').collect::<Vec<_>>();

            assert_eq!(items, expected_range);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_range_reader_unbounded() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            Value::new(
                i.to_be_bytes(),
                nanoid::nanoid!().as_bytes(),
                1000 + i,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(0, writer)?;
        metadata.write_to_file(&folder)?;

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(folder.join(BLOCKS_FILE), (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            (0, 0).into(),
            table.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        {
            let mut iter = Range::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&..),
            );

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            let mut iter = Range::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&..),
            );

            for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        {
            log::info!("Getting every item (unbounded start)");

            let end: Arc<[u8]> = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple::<UserKey>(&..end),
            );

            for key in (0..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse (unbounded start)");

            let end: Arc<[u8]> = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&..end),
            );

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }
        }

        {
            log::info!("Getting every item (unbounded end)");

            let start: Arc<[u8]> = 1_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&(start..)),
            );

            for key in (1_000..5_000).map(u64::to_be_bytes) {
                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);
            }

            log::info!("Getting every item in reverse (unbounded end)");

            let start: Arc<[u8]> = 1_000_u64.to_be_bytes().into();
            let end: Arc<[u8]> = 5_000_u64.to_be_bytes().into();

            let mut iter = Range::new(
                table,
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                range_bounds_to_tuple(&(start..end)),
            );

            for key in (1_000..5_000).rev().map(u64::to_be_bytes) {
                let item = iter.next_back().expect("item should exist")?;
                assert_eq!(key, &*item.key);
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
        for block_size in [1, 10, 100, 200, 500, 1_000, 4_096] {
            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
                folder: folder.clone(),
                evict_tombstones: false,
                block_size,

                #[cfg(feature = "bloom")]
                bloom_fp_rate: 0.01,
            })?;

            let items = (0u64..ITEM_COUNT).map(|i| {
                Value::new(
                    i.to_be_bytes(),
                    nanoid::nanoid!().as_bytes(),
                    1000 + i,
                    ValueType::Value,
                )
            });

            for item in items {
                writer.write(item)?;
            }

            writer.finish()?;

            let metadata = Metadata::from_writer(0, writer)?;
            metadata.write_to_file(&folder)?;

            let table = Arc::new(FileDescriptorTable::new(512, 1));
            table.insert(folder.join(BLOCKS_FILE), (0, 0).into());

            let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
            let block_index = Arc::new(BlockIndex::from_file(
                (0, 0).into(),
                table.clone(),
                &folder,
                Arc::clone(&block_cache),
            )?);

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
                    table.clone(),
                    (0, 0).into(),
                    Arc::clone(&block_cache),
                    Arc::clone(&block_index),
                    bounds_u64_to_bytes(&bounds),
                );

                for key in range.map(u64::to_be_bytes) {
                    let item = iter.next().unwrap_or_else(|| {
                        panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                    })?;

                    assert_eq!(key, &*item.key);
                }

                log::debug!("Getting every item in range in reverse");
                let range = std::ops::Range { start, end };

                let mut iter = Range::new(
                    table.clone(),
                    (0, 0).into(),
                    Arc::clone(&block_cache),
                    Arc::clone(&block_index),
                    bounds_u64_to_bytes(&bounds),
                );

                for key in range.rev().map(u64::to_be_bytes) {
                    let item = iter.next_back().unwrap_or_else(|| {
                        panic!("item should exist: {:?} ({})", key, u64::from_be_bytes(key))
                    })?;

                    assert_eq!(key, &*item.key);
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
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 250,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = chars.iter().map(|&key| {
            Value::new(
                &[key][..],
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreezrzsernszsdaadsadsadsadsadsdsensnzersnzers",
                0,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        writer.finish()?;

        let metadata = Metadata::from_writer(0, writer)?;
        metadata.write_to_file(&folder)?;

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(folder.join(BLOCKS_FILE), (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            (0, 0).into(),
            table.clone(),
            &folder,
            Arc::clone(&block_cache),
        )?);

        for (i, &start_char) in chars.iter().enumerate() {
            for &end_char in chars.iter().skip(i + 1) {
                log::debug!("checking ({}, {})", start_char as char, end_char as char);

                let expected_range = (start_char..=end_char).collect::<Vec<_>>();

                let iter = SegmentReader::new(
                    table.clone(),
                    (0, 0).into(),
                    block_cache.clone(),
                    block_index.clone(),
                )
                .set_lower_bound(Arc::new([start_char]))
                .set_upper_bound(Arc::new([end_char]));
                let mut range = iter.flatten().map(|x| x.key);

                for &item in &expected_range {
                    assert_eq!(&*range.next().expect("should exist"), &[item]);
                }

                let iter = SegmentReader::new(
                    table.clone(),
                    (0, 0).into(),
                    block_cache.clone(),
                    block_index.clone(),
                )
                .set_lower_bound(Arc::new([start_char]))
                .set_upper_bound(Arc::new([end_char]));
                let mut range = iter.flatten().map(|x| x.key);

                for &item in expected_range.iter().rev() {
                    assert_eq!(&*range.next_back().expect("should exist"), &[item]);
                }
            }
        }

        Ok(())
    }
}
