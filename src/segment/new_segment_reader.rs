use super::{
    new_block_consumer::NewBlockConsumer,
    value_block::{CachePolicy, ValueBlock},
};
use crate::{
    descriptor_table::FileDescriptorTable, segment::block::header::Header, BlockCache,
    GlobalSegmentId, UserKey, Value,
};
use std::sync::Arc;

pub struct NewSegmentReader {
    descriptor_table: Arc<FileDescriptorTable>,
    segment_id: GlobalSegmentId,
    block_cache: Arc<BlockCache>,

    data_block_boundary: u64,

    pub lo_block_offset: u64,
    lo_block_size: u64,
    lo_block_items: Option<NewBlockConsumer>,
    lo_initialized: bool,

    pub hi_block_offset: Option<u64>,
    pub hi_block_backlink: u64,
    pub hi_block_items: Option<NewBlockConsumer>,
    pub hi_initialized: bool,

    start_key: Option<UserKey>,
    end_key: Option<UserKey>,

    cache_policy: CachePolicy,
}

impl NewSegmentReader {
    #[must_use]
    pub fn new(
        data_block_boundary: u64,
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        lo_block_offset: u64,
        hi_block_offset: Option<u64>,
    ) -> Self {
        Self {
            data_block_boundary,

            descriptor_table,
            segment_id,
            block_cache,

            lo_block_offset,
            lo_block_size: 0,
            lo_block_items: None,
            lo_initialized: false,

            hi_block_offset,
            hi_block_backlink: 0,
            hi_block_items: None,
            hi_initialized: false,

            cache_policy: CachePolicy::Write,

            start_key: None,
            end_key: None,
        }
    }

    /// Sets the lower bound block, such that as many blocks as possible can be skipped.
    pub fn set_lower_bound(&mut self, key: UserKey) {
        self.start_key = Some(key);
    }

    /// Sets the upper bound block, such that as many blocks as possible can be skipped.
    pub fn set_upper_bound(&mut self, key: UserKey) {
        self.end_key = Some(key);
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    fn load_data_block(&self, offset: u64) -> crate::Result<Option<(u64, u64, NewBlockConsumer)>> {
        let block = ValueBlock::load_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            self.segment_id,
            offset,
            self.cache_policy,
        )?;

        // Truncate as many items as possible
        block.map_or(Ok(None), |block| {
            Ok(Some((
                block.header.data_length.into(),
                block.header.previous_block_offset,
                NewBlockConsumer::with_bounds(block, &self.start_key, &self.end_key),
            )))
        })
    }

    fn initialize_lo(&mut self) -> crate::Result<()> {
        if let Some((size, _, items)) = self.load_data_block(self.lo_block_offset)? {
            self.lo_block_items = Some(items);
            self.lo_block_size = size;
        }

        self.lo_initialized = true;

        Ok(())
    }

    fn initialize_hi(&mut self) -> crate::Result<()> {
        let offset = self
            .hi_block_offset
            .expect("no hi offset configured for segment reader");

        if let Some((_, backlink, items)) = self.load_data_block(offset)? {
            self.hi_block_items = Some(items);
            self.hi_block_backlink = backlink;
        }

        self.hi_initialized = true;

        Ok(())
    }
}

impl Iterator for NewSegmentReader {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.lo_initialized {
            if let Err(e) = self.initialize_lo() {
                return Some(Err(e));
            };
        }

        if let Some(head) = self.lo_block_items.as_mut()?.next() {
            // Just consume item
            return Some(Ok(head));
        }

        // Front buffer is empty

        // Load next block
        let next_block_offset =
            self.lo_block_offset + Header::serialized_len() as u64 + self.lo_block_size;

        assert_ne!(
            self.lo_block_offset, next_block_offset,
            "invalid next block offset"
        );

        if next_block_offset >= self.data_block_boundary {
            // We are done
            return None;
        }

        if let Some(hi_offset) = self.hi_block_offset {
            if next_block_offset == hi_offset {
                if !self.hi_initialized {
                    if let Err(e) = self.initialize_hi() {
                        return Some(Err(e));
                    };
                }

                // We reached the last block, consume from it instead
                return self.hi_block_items.as_mut()?.next().map(Ok);
            }
        }

        match self.load_data_block(next_block_offset) {
            Ok(Some((size, _, items))) => {
                self.lo_block_items = Some(items);
                self.lo_block_size = size;
                self.lo_block_offset = next_block_offset;

                // We just loaded the block
                self.lo_block_items.as_mut()?.next().map(Ok)
            }
            Ok(None) => {
                panic!("searched for invalid data block");
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl DoubleEndedIterator for NewSegmentReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.hi_initialized {
            if let Err(e) = self.initialize_hi() {
                return Some(Err(e));
            };
        }

        loop {
            // NOTE: See init function
            let hi_offset = self
                .hi_block_offset
                .expect("no hi offset configured for segment reader");

            if hi_offset == self.lo_block_offset {
                if !self.lo_initialized {
                    if let Err(e) = self.initialize_lo() {
                        return Some(Err(e));
                    };
                }

                // We reached the last block, consume from it instead
                return self.lo_block_items.as_mut()?.next_back().map(Ok);
            }

            if let Some(tail) = self.hi_block_items.as_mut()?.next_back() {
                // Just consume item
                return Some(Ok(tail));
            }

            // Back buffer is empty

            if hi_offset == 0 {
                // We are done
                return None;
            }

            // Load prev block
            let prev_block_offset = self.hi_block_backlink;

            if prev_block_offset == self.lo_block_offset {
                if !self.lo_initialized {
                    if let Err(e) = self.initialize_lo() {
                        return Some(Err(e));
                    };
                }

                // We reached the last block, consume from it instead
                return self.lo_block_items.as_mut()?.next_back().map(Ok);
            }

            match self.load_data_block(prev_block_offset) {
                Ok(Some((_, backlink, items))) => {
                    self.hi_block_items = Some(items);
                    self.hi_block_backlink = backlink;
                    self.hi_block_offset = Some(prev_block_offset);

                    // We just loaded the block
                    if let Some(item) = self.hi_block_items.as_mut()?.next_back() {
                        return Some(Ok(item));
                    }
                }
                Ok(None) => {
                    panic!("searched for invalid data block");
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{
            block_index::BlockIndex,
            reader::Reader,
            writer::{Options, Writer},
        },
        value::ValueType,
        Value,
    };
    use std::{sync::Arc, time::Instant};
    use test_log::test;

    #[test]
    #[allow(clippy::expect_used)]
    fn new_test() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 1_000_000;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4_096,

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

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(0));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, 0).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

        for _ in 0..1 {
            let start = Instant::now();

            let mut iter = Reader::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
            );

            eprintln!("setup in {}ns", start.elapsed().as_nanos());

            let start = Instant::now();

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
                /* let start = Instant::now(); */

                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);

                /* let nanos = start.elapsed().as_nanos();
                eprintln!("item read in {nanos}ns"); */
            }

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos() / u128::from(ITEM_COUNT);

            eprintln!(
                "old reader: done in {}s, {nanos} ns per item",
                elapsed.as_secs_f32()
            );
        }

        for _ in 0..1 {
            let start = Instant::now();

            /* let first_data_block_handle = block_index
            .get_first_data_block_handle(CachePolicy::Write)?; */

            let mut iter = NewSegmentReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                0, // NOTE: Can skip looking up first block, because it's always 0
                None,
            );

            eprintln!("setup in {}ns", start.elapsed().as_nanos());

            let start = Instant::now();

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
                /* let start = Instant::now(); */

                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);

                /* let nanos = start.elapsed().as_nanos();
                eprintln!("item read in {nanos}ns"); */
            }

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos() / u128::from(ITEM_COUNT);

            eprintln!(
                "new reader: done in {}s, {nanos} ns per item",
                elapsed.as_secs_f32()
            );
        }

        for _ in 0..1 {
            let start = Instant::now();

            let mut iter = Reader::new(
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
            )
            .rev();

            eprintln!("setup in {}ns", start.elapsed().as_nanos());

            let start = Instant::now();

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes).rev() {
                /* let start = Instant::now(); */

                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);

                /* let nanos = start.elapsed().as_nanos();
                eprintln!("item read in {nanos}ns"); */
            }

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos() / u128::from(ITEM_COUNT);

            eprintln!(
                "old rev reader: done in {}s, {nanos} ns per item",
                elapsed.as_secs_f32()
            );
        }

        for _ in 0..1 {
            let start = Instant::now();

            let last_data_block_handle =
                block_index.get_last_data_block_handle(CachePolicy::Write)?;

            let mut iter = NewSegmentReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                0, // NOTE: Can skip looking up first block, because it's always 0
                Some(last_data_block_handle.offset),
            )
            .rev();

            eprintln!("setup in {}ns", start.elapsed().as_nanos());

            let start = Instant::now();

            for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes).rev() {
                /* let start = Instant::now(); */

                let item = iter.next().expect("item should exist")?;
                assert_eq!(key, &*item.key);

                /* let nanos = start.elapsed().as_nanos();
                eprintln!("item read in {nanos}ns"); */
            }

            let elapsed = start.elapsed();
            let nanos = elapsed.as_nanos() / u128::from(ITEM_COUNT);

            eprintln!(
                "new rev reader: done in {}s, {nanos} ns per item",
                elapsed.as_secs_f32()
            );
        }

        Ok(())
    }
}
