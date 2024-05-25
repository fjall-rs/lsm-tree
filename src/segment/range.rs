use super::block_index::BlockIndex;
use super::id::GlobalSegmentId;
use super::reader::Reader;
use super::value_block::CachePolicy;
use crate::block_cache::BlockCache;
use crate::descriptor_table::FileDescriptorTable;
use crate::value::UserKey;
use crate::Value;
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::Arc;

pub struct Range {
    block_index: Arc<BlockIndex>,

    is_initialized: bool,

    range: (Bound<UserKey>, Bound<UserKey>),

    reader: Reader,

    cache_policy: CachePolicy,
}

impl Range {
    pub fn new(
        data_block_boundary: u64,
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        range: (Bound<UserKey>, Bound<UserKey>),
    ) -> Self {
        let reader = Reader::new(
            data_block_boundary,
            descriptor_table,
            segment_id,
            block_cache,
            0,
            None,
        );

        Self {
            is_initialized: false,

            block_index,

            reader,
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

    fn initialize(&mut self) -> crate::Result<()> {
        let start_key = match self.range.start_bound() {
            Bound::Unbounded => None,
            Bound::Included(start) | Bound::Excluded(start) => {
                if let Some(lower_bound) = self
                    .block_index
                    .get_lowest_data_block_handle_containing_item(start, CachePolicy::Write)?
                {
                    self.reader.lo_block_offset = lower_bound.offset;
                }

                Some(start)
            }
        };

        let end_key: Option<&Arc<[u8]>> = match self.range.end_bound() {
            Bound::Unbounded => {
                let upper_bound = self
                    .block_index
                    .get_last_data_block_handle(CachePolicy::Write)?;

                self.reader.hi_block_offset = Some(upper_bound.offset);

                None
            }
            Bound::Included(end) | Bound::Excluded(end) => {
                if let Some(upper_bound) = self
                    .block_index
                    .get_lowest_data_block_handle_not_containing_item(end, CachePolicy::Write)?
                {
                    self.reader.hi_block_offset = Some(upper_bound.offset);
                }

                Some(end)
            }
        };

        if let Some(key) = start_key.cloned() {
            self.reader.set_lower_bound(key);
        }
        if let Some(key) = end_key.cloned() {
            self.reader.set_upper_bound(key);
        }

        self.is_initialized = true;

        Ok(())
    }
}

impl Iterator for Range {
    type Item = crate::Result<Value>;

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
#[allow(clippy::expect_used)]
mod tests {
    // use super::Reader as SegmentReader;
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{
            block_index::BlockIndex,
            meta::CompressionType,
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
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 1000, // NOTE: Block size 1 to for each item to be its own block
            compression: CompressionType::Lz4,

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

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, 0).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

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
            let key: Arc<[u8]> = Arc::from(key);

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
                .map(|x| x.key.first().copied().expect("is ok"))
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
                .map(|x| x.key.first().copied().expect("is ok"))
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
            evict_tombstones: false,
            block_size: 4096,
            compression: CompressionType::Lz4,

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

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, 0).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

        {
            let mut iter = Range::new(
                trailer.offsets.index_block_ptr,
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
                trailer.offsets.index_block_ptr,
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
                trailer.offsets.index_block_ptr,
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
                trailer.offsets.index_block_ptr,
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
                trailer.offsets.index_block_ptr,
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
                trailer.offsets.index_block_ptr,
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
                segment_id: 0,

                folder: folder.clone(),
                evict_tombstones: false,
                block_size,
                compression: CompressionType::Lz4,

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

            let trailer = writer.finish()?.expect("should exist");

            let segment_file_path = folder.join("0");

            let table = Arc::new(FileDescriptorTable::new(512, 1));
            table.insert(&segment_file_path, (0, 0).into());

            let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
            let block_index = Arc::new(BlockIndex::from_file(
                segment_file_path,
                trailer.offsets.tli_ptr,
                (0, 0).into(),
                table.clone(),
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
                    trailer.offsets.index_block_ptr,
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
                    trailer.offsets.index_block_ptr,
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
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 250,
            compression: CompressionType::Lz4,

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

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, 0).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

        for (i, &start_char) in chars.iter().enumerate() {
            for &end_char in chars.iter().skip(i + 1) {
                log::debug!("checking ({}, {})", start_char as char, end_char as char);

                let expected_range = (start_char..=end_char).collect::<Vec<_>>();

                let iter = Range::new(
                    trailer.offsets.index_block_ptr,
                    table.clone(),
                    (0, 0).into(),
                    Arc::clone(&block_cache),
                    Arc::clone(&block_index),
                    (
                        Included(Arc::new([start_char])),
                        Included(Arc::new([end_char])),
                    ),
                );

                let mut range = iter.flatten().map(|x| x.key);

                for &item in &expected_range {
                    assert_eq!(&*range.next().expect("should exist"), &[item]);
                }

                let iter = Range::new(
                    trailer.offsets.index_block_ptr,
                    table.clone(),
                    (0, 0).into(),
                    Arc::clone(&block_cache),
                    Arc::clone(&block_index),
                    (
                        Included(Arc::new([start_char])),
                        Included(Arc::new([end_char])),
                    ),
                );

                let mut range = iter.flatten().map(|x| x.key);

                for &item in expected_range.iter().rev() {
                    assert_eq!(&*range.next_back().expect("should exist"), &[item]);
                }
            }
        }

        Ok(())
    }
}
