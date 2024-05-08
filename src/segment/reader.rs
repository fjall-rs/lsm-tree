use super::{
    block::CachePolicy,
    block_index::{block_handle::KeyedBlockHandle, BlockIndex},
    id::GlobalSegmentId,
    index_block_consumer::IndexBlockConsumer,
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

            consumers: HashMap::with_capacity(2),
            current_lo: None,
            current_hi: None,

            is_initialized: false,

            cache_policy: CachePolicy::Write,
        }
    }

    /// Sets the lower bound block, so that as many blocks as possible can be skipped.
    #[must_use]
    pub fn set_lower_bound(mut self, key: UserKey) -> Self {
        self.start_key = Some(key);
        self
    }

    /// Sets the upper bound block, so that as many blocks as possible can be skipped.
    #[must_use]
    pub fn set_upper(mut self, handle: KeyedBlockHandle) -> Self {
        self.current_lo = Some(handle);
        self
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.cache_policy = policy;
        self
    }

    // TODO: refactor
    fn initialize(&mut self) -> crate::Result<()> {
        if let Some(key) = &self.start_key {
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
                    self.block_index.clone(),
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
        } else {
            // TODO: if no start key, initial block should be loaded lazy

            let block_handle = self.block_index.get_first_index_block_handle();
            let index_block = self
                .block_index
                .load_index_block(block_handle, self.cache_policy)?;

            self.current_lo = Some(block_handle.clone());

            let mut consumer = IndexBlockConsumer::new(
                self.descriptor_table.clone(),
                self.segment_id,
                self.block_cache.clone(),
                self.block_index.clone(),
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

        if let Some(key) = &self.end_key {
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
                        self.block_index.clone(),
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
        } else {
            // TODO: if no end key, initial block should be loaded lazy

            let block_handle = self.block_index.get_last_block_handle();

            self.current_hi = Some(block_handle.clone());

            if self.current_hi != self.current_lo {
                let index_block = self
                    .block_index
                    .load_index_block(block_handle, self.cache_policy)?;

                let mut consumer = IndexBlockConsumer::new(
                    self.descriptor_table.clone(),
                    self.segment_id,
                    self.block_cache.clone(),
                    self.block_index.clone(),
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
        }

        self.is_initialized = true;

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

        // TODO: if !current_lo, load first block

        'outer: loop {
            if let Some(current_lo) = &self.current_lo {
                if let Some(consumer) = self.consumers.get_mut(current_lo) {
                    let next_item = consumer.next();

                    if let Some(item) = next_item {
                        let item = match item {
                            Ok(v) => v,
                            Err(e) => return Some(Err(e)),
                        };

                        // log::trace!("INSPECTING {item:?}");

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

                        //   log::debug!("RETURNING {item:?}");
                        return Some(Ok(item));
                    }

                    // NOTE: Consumer is empty, load next one

                    let next_index_block_handle =
                        self.block_index.get_next_index_block_handle(current_lo)?;

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
                    self.consumers.remove(current_lo);

                    let mut consumer = IndexBlockConsumer::new(
                        self.descriptor_table.clone(),
                        self.segment_id,
                        self.block_cache.clone(),
                        self.block_index.clone(),
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
            } else {
                // TODO: what if initialize does not setup current_lo??
                panic!("no current lo");
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

        // TODO: if !current_hi, load last block

        'outer: loop {
            if let Some(current_hi) = &self.current_hi {
                if let Some(consumer) = self.consumers.get_mut(current_hi) {
                    let next_item = consumer.next_back();

                    if let Some(item) = next_item {
                        let item = match item {
                            Ok(v) => v,
                            Err(e) => return Some(Err(e)),
                        };

                        // log::trace!("INSPECTING {item:?}");

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

                        //    log::debug!("RETURNING {item:?}");
                        return Some(Ok(item));
                    }

                    // NOTE: Consumer is empty, load next one

                    let prev_index_block_handle =
                        self.block_index.get_prev_index_block_handle(current_hi)?;

                    // IMPORTANT: We are going past the lower bound, we're done
                    if let Some(current_lo) = &self.current_lo {
                        if prev_index_block_handle < current_lo {
                            return None;
                        }
                    }

                    log::warn!("Load prev index block {prev_index_block_handle:?}");

                    // IMPORTANT: If we already have a consumer open with that block handle
                    // just use that in the next iteration
                    if self.consumers.contains_key(prev_index_block_handle) {
                        log::error!("consuming from lo");
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
                    self.consumers.remove(current_hi);

                    let mut consumer = IndexBlockConsumer::new(
                        self.descriptor_table.clone(),
                        self.segment_id,
                        self.block_cache.clone(),
                        self.block_index.clone(),
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
            } else {
                // TODO: what if initialize does not setup current_hi??
                panic!("no current hi");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        file::BLOCKS_FILE,
        segment::{
            block_index::BlockIndex,
            meta::Metadata,
            reader::Reader,
            writer::{Options, Writer},
        },
        value::ValueType,
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    // TODO: rev test with seqnos...

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_full_scan() -> crate::Result<()> {
        for block_size in [1, 10, 50, 100, 200, 500, 1_000, 2_000, 4_000] {
            let item_count = u64::from(block_size) * 10;

            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
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

            let iter = Reader::new(
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
            );
            assert_eq!(item_count as usize, iter.flatten().count());

            let iter = Reader::new(
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                block_index.clone(),
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

        let iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
        );
        assert_eq!(ITEM_COUNT as usize, iter.flatten().count());

        let iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
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

        let iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
        );
        assert_eq!(1 + 250 + chars.len(), iter.flatten().count());

        let iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
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

        /* let iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
        )
        .set_lower_bound(Arc::new(*b"b"));

        assert_eq!(100 + chars.len(), iter.flatten().count()); */

        let iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            block_cache.clone(),
            block_index.clone(),
        )
        .set_lower_bound(Arc::new(*b"b"));

        assert_eq!(100 + chars.len(), iter.rev().flatten().count());

        Ok(())
    }

    // TODO: test upper bound

    #[test]
    #[allow(clippy::expect_used)]
    fn segment_reader_memory_big_scan() -> crate::Result<()> {
        const ITEM_COUNT: u64 = 1_000_000;

        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = (0u64..ITEM_COUNT)
            .map(|i| Value::new(i.to_be_bytes(), *b"asd", 1000 + i, ValueType::Value));

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

        let mut iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
        );

        for key in (0u64..ITEM_COUNT).map(u64::to_be_bytes) {
            let item = iter.next().expect("item should exist")?;
            assert_eq!(key, &*item.key);
            assert!(iter.consumers.len() <= 2); // TODO: should be 1
            assert!(iter.consumers.capacity() <= 5);
            assert!(
                iter.consumers
                    .values()
                    .next()
                    .expect("should exist")
                    .data_blocks
                    .len()
                    <= 1
            );
        }

        let mut iter = Reader::new(
            table.clone(),
            (0, 0).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
        );

        for key in (0u64..ITEM_COUNT).rev().map(u64::to_be_bytes) {
            let item = iter.next_back().expect("item should exist")?;
            assert_eq!(key, &*item.key);
            assert!(iter.consumers.len() <= 2); // TODO: should be 1
            assert!(iter.consumers.capacity() <= 5);
            assert!(
                iter.consumers
                    .values()
                    .next()
                    .expect("should exist")
                    .data_blocks
                    .len()
                    <= 2
            );
        }

        // TODO: ping pong
        /*  let mut iter = Reader::new(
            table,
            (0, 0).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
        );

        for i in 0u64..ITEM_COUNT {
            if i % 2 == 0 {
                iter.next().expect("item should exist")?
            } else {
                iter.next_back().expect("item should exist")?
            };

            assert!(iter.consumers.len() <= 2);
            assert!(iter.consumers.capacity() <= 5);
            assert!(
                iter.consumers
                    .values()
                    .next()
                    .expect("should exist")
                    .data_blocks
                    .len()
                    <= 2
            );
            assert!(
                iter.consumers
                    .values()
                    .next_back()
                    .expect("should exist")
                    .data_blocks
                    .len()
                    <= 2
            );
        } */

        Ok(())
    }
}
