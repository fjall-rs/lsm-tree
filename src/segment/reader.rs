use super::{
    block_index::{block_handle::KeyedBlockHandle, BlockIndex},
    id::GlobalSegmentId,
    index_block_consumer::IndexBlockConsumer,
    value_block::CachePolicy,
};
use crate::{block_cache::BlockCache, descriptor_table::FileDescriptorTable, UserKey, Value};
use std::{collections::HashMap, sync::Arc};

/// Stupidly iterates through the entries of a segment
/// This does not account for tombstones
#[allow(clippy::module_name_repetitions)]
pub struct Reader {
    descriptor_table: Arc<FileDescriptorTable>,
    block_index: Arc<BlockIndex>,

    segment_id: GlobalSegmentId,
    block_cache: Arc<BlockCache>,

    start_key: Option<UserKey>,
    end_key: Option<UserKey>,

    consumers: HashMap<KeyedBlockHandle, IndexBlockConsumer>,
    current_lo: Option<KeyedBlockHandle>,
    current_hi: Option<KeyedBlockHandle>,

    is_initialized: bool,

    cache_policy: CachePolicy,
}

impl Reader {
    pub fn new(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
    ) -> Self {
        Self {
            descriptor_table,

            segment_id,
            block_cache,

            block_index,

            start_key: None,
            end_key: None,

            consumers: HashMap::default(),
            current_lo: None,
            current_hi: None,

            is_initialized: false,

            cache_policy: CachePolicy::Write,
        }
    }

    /// Sets the lower bound block, such that as many blocks as possible can be skipped.
    #[must_use]
    pub fn set_lower_bound(mut self, key: UserKey) -> Self {
        self.start_key = Some(key);
        self
    }

    /// Sets the upper bound block, such that as many blocks as possible can be skipped.
    #[must_use]
    pub fn set_upper_bound(mut self, key: UserKey) -> Self {
        self.end_key = Some(key);
        self
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    fn initialize(&mut self) -> crate::Result<()> {
        if let Some(key) = &self.start_key {
            self.load_lower_bound(&key.clone())?;
        }

        if let Some(key) = &self.end_key {
            self.load_upper_bound(&key.clone())?;
        }

        self.is_initialized = true;

        Ok(())
    }

    fn load_lower_bound(&mut self, key: &[u8]) -> crate::Result<()> {
        if let Some(index_block_handle) = self
            .block_index
            .get_lowest_index_block_handle_containing_key(key)
        {
            let index_block = self
                .block_index
                .load_index_block(index_block_handle, self.cache_policy)?;

            self.current_lo = Some(index_block_handle.clone());

            let mut consumer = IndexBlockConsumer::new(
                self.descriptor_table.clone(),
                self.segment_id,
                self.block_cache.clone(),
                // self.block_index.clone(),
                index_block.items.to_vec().into(),
            )
            .cache_policy(self.cache_policy);

            if let Some(start_key) = &self.start_key {
                consumer = consumer.set_lower_bound(start_key.clone());
            }
            if let Some(end_key) = &self.end_key {
                consumer = consumer.set_upper_bound(end_key.clone());
            }

            self.consumers.insert(index_block_handle.clone(), consumer);
        }

        Ok(())
    }

    fn load_first_block(&mut self) -> crate::Result<()> {
        let block_handle = self.block_index.get_first_index_block_handle();
        let index_block = self
            .block_index
            .load_index_block(block_handle, self.cache_policy)?;

        self.current_lo = Some(block_handle.clone());

        if self.current_lo != self.current_hi {
            let mut consumer = IndexBlockConsumer::new(
                self.descriptor_table.clone(),
                self.segment_id,
                self.block_cache.clone(),
                //  self.block_index.clone(),
                index_block.items.to_vec().into(),
            )
            .cache_policy(self.cache_policy);

            if let Some(start_key) = &self.start_key {
                consumer = consumer.set_lower_bound(start_key.clone());
            }
            if let Some(end_key) = &self.end_key {
                consumer = consumer.set_upper_bound(end_key.clone());
            }

            self.consumers.insert(block_handle.clone(), consumer);
        }

        Ok(())
    }

    fn load_last_block(&mut self) -> crate::Result<()> {
        let block_handle = self.block_index.get_last_index_block_handle();

        self.current_hi = Some(block_handle.clone());

        if self.current_hi != self.current_lo {
            let index_block = self
                .block_index
                .load_index_block(block_handle, self.cache_policy)?;

            let mut consumer = IndexBlockConsumer::new(
                self.descriptor_table.clone(),
                self.segment_id,
                self.block_cache.clone(),
                //   self.block_index.clone(),
                index_block.items.to_vec().into(),
            )
            .cache_policy(self.cache_policy);

            if let Some(start_key) = &self.start_key {
                consumer = consumer.set_lower_bound(start_key.clone());
            }
            if let Some(end_key) = &self.end_key {
                consumer = consumer.set_upper_bound(end_key.clone());
            }

            self.consumers.insert(block_handle.clone(), consumer);
        }

        Ok(())
    }

    fn load_upper_bound(&mut self, key: &[u8]) -> crate::Result<()> {
        if let Some(index_block_handle) = self
            .block_index
            .get_lowest_index_block_handle_not_containing_key(key)
        {
            self.current_hi = Some(index_block_handle.clone());

            if self.current_hi != self.current_lo {
                let index_block = self
                    .block_index
                    .load_index_block(index_block_handle, self.cache_policy)?;

                let mut consumer = IndexBlockConsumer::new(
                    self.descriptor_table.clone(),
                    self.segment_id,
                    self.block_cache.clone(),
                    //   self.block_index.clone(),
                    index_block.items.to_vec().into(),
                )
                .cache_policy(self.cache_policy);

                if let Some(start_key) = &self.start_key {
                    consumer = consumer.set_lower_bound(start_key.clone());
                }
                if let Some(end_key) = &self.end_key {
                    consumer = consumer.set_upper_bound(end_key.clone());
                }

                self.consumers.insert(index_block_handle.clone(), consumer);
            }
        }

        Ok(())
    }
}

impl Iterator for Reader {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        if self.current_lo.is_none() {
            if let Err(e) = self.load_first_block() {
                return Some(Err(e));
            };
        }

        'outer: loop {
            let current_lo = self.current_lo.clone().expect("lower bound uninitialized");

            if let Some(consumer) = self.consumers.get_mut(&current_lo) {
                let next_item = consumer.next();

                if let Some(item) = next_item {
                    let item = match item {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };

                    if let Some(start_key) = &self.start_key {
                        // Continue seeking initial start key
                        if &item.key < start_key {
                            continue 'outer;
                        }
                    }

                    if let Some(end_key) = &self.end_key {
                        // Reached next key after upper bound
                        // iterator can be closed
                        if &item.key > end_key {
                            return None;
                        }
                    }

                    return Some(Ok(item));
                }

                // NOTE: Consumer is empty, load next one

                let next_index_block_handle =
                    self.block_index.get_next_index_block_handle(&current_lo)?;

                // IMPORTANT: We are going past the upper bound, we're done
                if let Some(current_hi) = &self.current_hi {
                    if next_index_block_handle > current_hi {
                        return None;
                    }
                }

                // IMPORTANT: If we already have a consumer open with that block handle
                // just use that in the next iteration
                if self.consumers.contains_key(next_index_block_handle) {
                    self.current_lo = Some(next_index_block_handle.clone());
                    continue 'outer;
                }

                let next_index_block = self
                    .block_index
                    .load_index_block(next_index_block_handle, self.cache_policy);

                let next_index_block = match next_index_block {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                // Remove old consumer
                self.consumers.remove(&current_lo);

                let mut consumer = IndexBlockConsumer::new(
                    self.descriptor_table.clone(),
                    self.segment_id,
                    self.block_cache.clone(),
                    //  self.block_index.clone(),
                    next_index_block.items.to_vec().into(),
                )
                .cache_policy(self.cache_policy);

                if let Some(start_key) = &self.start_key {
                    consumer = consumer.set_lower_bound(start_key.clone());
                }
                if let Some(end_key) = &self.end_key {
                    consumer = consumer.set_upper_bound(end_key.clone());
                }

                // Add new consumer
                self.consumers
                    .insert(next_index_block_handle.clone(), consumer);

                self.current_lo = Some(next_index_block_handle.clone());
            } else {
                panic!("no lo consumer");
            }
        }
    }
}

impl DoubleEndedIterator for Reader {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            if let Err(e) = self.initialize() {
                return Some(Err(e));
            };
        }

        if self.current_hi.is_none() {
            if let Err(e) = self.load_last_block() {
                return Some(Err(e));
            };
        }

        'outer: loop {
            let current_hi = self.current_hi.clone().expect("upper bound uninitialized");

            if let Some(consumer) = self.consumers.get_mut(&current_hi) {
                let next_item = consumer.next_back();

                if let Some(item) = next_item {
                    let item = match item {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };

                    if let Some(start_key) = &self.start_key {
                        // Reached key before lower bound
                        // iterator can be closed
                        if &item.key < start_key {
                            return None;
                        }
                    }

                    if let Some(end_key) = &self.end_key {
                        // Continue seeking to initial end key
                        if &item.key > end_key {
                            continue 'outer;
                        }
                    }

                    return Some(Ok(item));
                }

                // NOTE: Consumer is empty, load next one

                let prev_index_block_handle =
                    self.block_index.get_prev_index_block_handle(&current_hi)?;

                // IMPORTANT: We are going past the lower bound, we're done
                if let Some(current_lo) = &self.current_lo {
                    if prev_index_block_handle < current_lo {
                        return None;
                    }
                }

                // IMPORTANT: If we already have a consumer open with that block handle
                // just use that in the next iteration
                if self.consumers.contains_key(prev_index_block_handle) {
                    self.current_hi = Some(prev_index_block_handle.clone());
                    continue 'outer;
                }

                let prev_index_block = self
                    .block_index
                    .load_index_block(prev_index_block_handle, self.cache_policy);

                let prev_index_block = match prev_index_block {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                // Remove old consumer
                self.consumers.remove(&current_hi);

                let mut consumer = IndexBlockConsumer::new(
                    self.descriptor_table.clone(),
                    self.segment_id,
                    self.block_cache.clone(),
                    //  self.block_index.clone(),
                    prev_index_block.items.to_vec().into(),
                )
                .cache_policy(self.cache_policy);

                if let Some(start_key) = &self.start_key {
                    consumer = consumer.set_lower_bound(start_key.clone());
                }
                if let Some(end_key) = &self.end_key {
                    consumer = consumer.set_upper_bound(end_key.clone());
                }

                // Add new consumer
                self.consumers
                    .insert(prev_index_block_handle.clone(), consumer);

                self.current_hi = Some(prev_index_block_handle.clone());
            } else {
                panic!("no hi consumer");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{
            block_index::BlockIndex,
            new_segment_reader::NewSegmentReader,
            value_block::CachePolicy,
            writer::{Options, Writer},
        },
        value::ValueType,
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn segment_reader_ping_pong() -> crate::Result<()> {
        for block_size in [1, 10, 50, 100, 200, 500, 1_000, 2_000, 4_000] {
            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
                segment_id: 0,

                folder: folder.clone(),
                evict_tombstones: false,
                block_size,

                #[cfg(feature = "bloom")]
                bloom_fp_rate: 0.01,
            })?;

            writer.write(Value::new(*b"a", vec![], 0, ValueType::Value))?;
            writer.write(Value::new(*b"b", vec![], 0, ValueType::Value))?;
            writer.write(Value::new(*b"c", vec![], 0, ValueType::Value))?;
            writer.write(Value::new(*b"d", vec![], 0, ValueType::Value))?;
            writer.write(Value::new(*b"e", vec![], 0, ValueType::Value))?;

            let trailer = writer.finish()?.expect("should exist");

            let segment_file_path = folder.join("0");

            let table = Arc::new(FileDescriptorTable::new(512, 1));
            table.insert(&segment_file_path, (0, 0).into());

            let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
            let block_index = Arc::new(BlockIndex::from_file(
                &segment_file_path,
                trailer.offsets.tli_ptr,
                (0, 0).into(),
                table.clone(),
                Arc::clone(&block_cache),
            )?);

            let last_data_block_handle =
                block_index.get_last_data_block_handle(CachePolicy::Write)?;

            let mut iter = NewSegmentReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                0,
                Some(last_data_block_handle.offset),
            );

            assert_eq!(*b"a", &*iter.next().expect("should exist")?.key);
            assert_eq!(*b"e", &*iter.next_back().expect("should exist")?.key);
            assert_eq!(*b"b", &*iter.next().expect("should exist")?.key);
            assert_eq!(*b"d", &*iter.next_back().expect("should exist")?.key);
            assert_eq!(*b"c", &*iter.next().expect("should exist")?.key);
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_full_scan() -> crate::Result<()> {
        for block_size in [1, 10, 50, 100, 200, 500, 1_000, 2_000, 4_000] {
            let item_count = u64::from(block_size) * 10;

            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
                segment_id: 0,

                folder: folder.clone(),
                evict_tombstones: false,
                block_size,

                #[cfg(feature = "bloom")]
                bloom_fp_rate: 0.01,
            })?;

            let items = (0u64..item_count).map(|i| {
                Value::new(
                    i.to_be_bytes(),
                    *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreezrzsernszsdaadsadsadsadsadsdsensnzersnzers",
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
                &segment_file_path,
                trailer.offsets.tli_ptr,
                (0, 0).into(),
                table.clone(),
                Arc::clone(&block_cache),
            )?);

            let iter = NewSegmentReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                0,
                None,
            );
            assert_eq!(item_count as usize, iter.flatten().count());

            let last_data_block_handle =
                block_index.get_last_data_block_handle(CachePolicy::Write)?;

            let iter = NewSegmentReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                0,
                Some(last_data_block_handle.offset),
            );
            assert_eq!(item_count as usize, iter.rev().flatten().count());
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_full_scan_mini_blocks() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 1_000;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 1,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT).map(|i| {
            Value::new(
                i.to_be_bytes(),
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreezrzsernszsdaadsadsadsadsadsdsensnzersnzers",
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

        let iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            0,
            None,
        );
        assert_eq!(ITEM_COUNT as usize, iter.flatten().count());

        let last_data_block_handle = block_index.get_last_data_block_handle(CachePolicy::Write)?;

        let iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, 0).into(),
            block_cache,
            0,
            Some(last_data_block_handle.offset),
        );
        assert_eq!(ITEM_COUNT as usize, iter.rev().flatten().count());

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_range_lower_bound_mvcc_slab() -> crate::Result<()> {
        let chars = (b'c'..=b'z').collect::<Vec<_>>();

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 250,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        writer.write(Value::new(
            *b"a",
            *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreez",
            0,
            ValueType::Value,
        ))?;

        for seqno in (0..250).rev() {
            writer.write(Value::new(
                *b"b",
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreez",
                seqno,
                ValueType::Value,
            ))?;
        }

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

        let iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            0,
            None,
        );
        assert_eq!(1 + 250 + chars.len(), iter.flatten().count());

        let last_data_block_handle = block_index.get_last_data_block_handle(CachePolicy::Write)?;

        let iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, 0).into(),
            block_cache,
            0,
            Some(last_data_block_handle.offset),
        );
        assert_eq!(1 + 250 + chars.len(), iter.rev().flatten().count());

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_range_lower_bound_mvcc_slab_2() -> crate::Result<()> {
        let chars = (b'c'..=b'z').collect::<Vec<_>>();

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 200,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        for seqno in (0..500).rev() {
            writer.write(Value::new(
                *b"a",
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreez",
                seqno,
                ValueType::Value,
            ))?;
        }

        // IMPORTANT: Force B's to be written in a separate block
        writer.write_block()?;

        for seqno in (0..100).rev() {
            writer.write(Value::new(
                *b"b",
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreez",
                seqno,
                ValueType::Value,
            ))?;
        }

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

        let lower_bound = block_index
            .get_lowest_data_block_handle_containing_item(b"b", CachePolicy::Write)?
            .expect("should exist");

        let mut iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            lower_bound.offset,
            None,
        );
        iter.set_lower_bound(Arc::new(*b"b"));

        assert_eq!(100 + chars.len(), iter.flatten().count());

        let last_data_block_handle = block_index.get_last_data_block_handle(CachePolicy::Write)?;

        let mut iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, 0).into(),
            block_cache,
            lower_bound.offset,
            Some(last_data_block_handle.offset),
        );
        iter.set_lower_bound(Arc::new(*b"b"));

        assert_eq!(100 + chars.len(), iter.rev().flatten().count());

        Ok(())
    }

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_range_lower_bound_mvcc_slab_3() -> crate::Result<()> {
        let chars = (b'c'..=b'z').collect::<Vec<_>>();

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 200,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        for seqno in (0..500).rev() {
            writer.write(Value::new(
                *b"a",
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreez",
                seqno,
                ValueType::Value,
            ))?;
        }

        // IMPORTANT: Force B's to be written in a separate block
        writer.write_block()?;

        for seqno in (0..100).rev() {
            writer.write(Value::new(
                *b"b",
                *b"dsgfgfdsgsfdsgfdgfdfgdsgfdhsnreez",
                seqno,
                ValueType::Value,
            ))?;
        }

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

        let upper_bound = block_index
            .get_lowest_data_block_handle_not_containing_item(b"b", CachePolicy::Write)?
            .expect("should exist");

        let mut iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            0,
            Some(upper_bound.offset),
        );
        iter.set_upper_bound(Arc::new(*b"b"));

        assert_eq!(500 + 100, iter.flatten().count());

        let mut iter = NewSegmentReader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, 0).into(),
            block_cache,
            0,
            Some(upper_bound.offset),
        );
        iter.set_upper_bound(Arc::new(*b"b"));

        assert_eq!(500 + 100, iter.rev().flatten().count());

        Ok(())
    }
}
