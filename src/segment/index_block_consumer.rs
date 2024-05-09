use super::{
    block::CachePolicy,
    block_index::{block_handle::KeyedBlockHandle, BlockIndex},
};
use crate::{
    descriptor_table::FileDescriptorTable, segment::block::load_by_block_handle, BlockCache,
    GlobalSegmentId, UserKey, Value,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

/// Takes an index block handle, and allows consuming all
/// data blocks it points to
pub struct IndexBlockConsumer {
    descriptor_table: Arc<FileDescriptorTable>,
    block_index: Arc<BlockIndex>,
    segment_id: GlobalSegmentId,
    block_cache: Arc<BlockCache>,

    start_key: Option<UserKey>,
    end_key: Option<UserKey>,

    /// Index block that is being consumed from both ends
    data_block_handles: VecDeque<KeyedBlockHandle>,

    /// Keep track of lower and upper bounds
    current_lo: Option<KeyedBlockHandle>,
    current_hi: Option<KeyedBlockHandle>,

    /// Data block buffers that have been loaded and are being consumed
    pub(crate) data_blocks: HashMap<KeyedBlockHandle, VecDeque<Value>>,
    // TODO: ^ maybe change to (MinBuf, MaxBuf)
    //
    cache_policy: CachePolicy,

    is_initialized: bool,
}

impl IndexBlockConsumer {
    #[must_use]
    pub fn new(
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        data_block_handles: VecDeque<KeyedBlockHandle>,
    ) -> Self {
        Self {
            descriptor_table,
            segment_id,
            block_cache,
            block_index,

            start_key: None,
            end_key: None,

            data_block_handles,
            current_lo: None,
            current_hi: None,
            data_blocks: HashMap::with_capacity(2),

            cache_policy: CachePolicy::Write,

            is_initialized: false,
        }
    }

    /// Sets the lower bound block, so that as many blocks as possible can be skipped.
    ///
    /// # Caveat
    ///
    /// That does not mean, the consumer will not return keys before the searched key
    /// as it works on a per-block basis, consider:
    ///
    /// [a, b, c] [d, e, f] [g, h, i]
    ///
    /// If we searched for 'f', we would get:
    ///
    /// [a, b, c] [d, e, f] [g, h, i]
    ///           ~~~~~~~~~~~~~~~~~~~
    ///           iteration
    #[must_use]
    pub fn set_lower_bound(mut self, key: UserKey) -> Self {
        self.start_key = Some(key);
        self
    }

    /// Sets the lower bound block, so that as many blocks as possible can be skipped.
    ///
    /// # Caveat
    ///
    /// That does not mean, the consumer will not return keys before the searched key
    /// as it works on a per-block basis.
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

    fn load_data_block(
        &mut self,
        block_handle: &KeyedBlockHandle,
    ) -> crate::Result<Option<VecDeque<Value>>> {
        let block = load_by_block_handle(
            &self.descriptor_table,
            &self.block_cache,
            self.segment_id,
            block_handle,
            self.cache_policy,
        )?;
        Ok(block.map(|block| block.items.clone().to_vec().into()))
    }

    // TODO: see TLI
    fn get_start_block(&self, key: &[u8]) -> Option<(usize, &KeyedBlockHandle)> {
        let idx = self
            .data_block_handles
            .partition_point(|x| &*x.start_key < key);
        let idx = idx.saturating_sub(1);

        let block = self.data_block_handles.get(idx)?;

        if &*block.start_key > key {
            None
        } else {
            Some((idx, block))
        }
    }

    // TODO: see TLI
    fn get_end_block(&self, key: &[u8]) -> Option<(usize, &KeyedBlockHandle)> {
        let idx = self
            .data_block_handles
            .partition_point(|x| &*x.start_key <= key);

        let block = self.data_block_handles.get(idx)?;
        Some((idx, block))
    }

    // TODO: reader.rs should be correct - index block consumer needs rewrite...

    fn initialize(&mut self) {
        if let Some(key) = &self.start_key {
            // TODO: unit test

            // TODO: only return index
            let result = self.get_start_block(key);

            if let Some((idx, _)) = result {
                // IMPORTANT: Remove all handles lower and including eligible block handle
                //
                // If our block handles look like this:
                //
                // [a, b, c, d, e, f]
                //
                // and we want start at 'c', we would load data block 'c'
                // and get rid of a, b, resulting in:
                //
                // current_lo = c
                //
                // [d, e, f]
                self.data_block_handles.drain(..idx);
            }
        }

        if let Some(key) = &self.end_key {
            // TODO: unit test

            // TODO: only return index
            let result = self.get_end_block(key);

            if let Some((idx, _)) = result {
                // IMPORTANT: Remove all handles higher and including eligible block handle
                //
                // If our block handles look like this:
                //
                // [a, b, c, d, e, f]
                //
                // and we want end at 'c', we would load data block 'c'
                // and get rid of d, e, f, resulting in:
                //
                // current_hi = c
                //
                // [a, b, c]
                self.data_block_handles.drain((idx + 1)..);
            }
        }

        self.is_initialized = true;
    }
}

impl Iterator for IndexBlockConsumer {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            self.initialize();
        }

        if self.current_lo.is_none() && !self.data_block_handles.is_empty() {
            let first_data_block_handle = self.data_block_handles.pop_front()?;

            self.current_lo = Some(first_data_block_handle.clone());

            if Some(&first_data_block_handle) == self.current_hi.as_ref() {
                // If the high bound is already at this block
                // Read from the block that was already loaded by hi
            } else {
                let data_block = match self.load_data_block(&first_data_block_handle) {
                    Ok(block) => block,
                    Err(e) => return Some(Err(e)),
                };
                debug_assert!(data_block.is_some());

                if let Some(data_block) = data_block {
                    self.data_blocks.insert(first_data_block_handle, data_block);
                }
            }
        }

        if self.data_block_handles.is_empty() && self.data_blocks.len() == 1 {
            // We've reached the final block
            // Just consume from it instead
            let block = self.data_blocks.values_mut().next();
            return block.and_then(VecDeque::pop_front).map(Ok);
        }

        let current_lo = self.current_lo.as_ref().expect("lower bound uninitialized");

        let block = self.data_blocks.get_mut(current_lo);

        if let Some(block) = block {
            let item = block.pop_front();

            if block.is_empty() {
                // Load next block
                self.data_blocks.remove(current_lo);

                if let Some(next_data_block_handle) = self.data_block_handles.pop_front() {
                    self.current_lo = Some(next_data_block_handle.clone());

                    if Some(&next_data_block_handle) == self.current_hi.as_ref() {
                        // Do nothing
                        // Next item consumed will use the existing higher block
                    } else {
                        let data_block = match self.load_data_block(&next_data_block_handle) {
                            Ok(block) => block,
                            Err(e) => return Some(Err(e)),
                        };
                        debug_assert!(data_block.is_some());

                        if let Some(data_block) = data_block {
                            self.data_blocks.insert(next_data_block_handle, data_block);
                        }
                    }
                }
            }

            item.map(Ok)
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for IndexBlockConsumer {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.is_initialized {
            self.initialize();
        }

        if self.current_hi.is_none() && !self.data_block_handles.is_empty() {
            let last_data_block_handle = self.data_block_handles.pop_back()?;

            self.current_hi = Some(last_data_block_handle.clone());

            if Some(&last_data_block_handle) == self.current_lo.as_ref() {
                // If the low bound is already at this block
                // Read from the block that was already loaded by lo
            } else {
                let data_block = match self.load_data_block(&last_data_block_handle) {
                    Ok(block) => block,
                    Err(e) => return Some(Err(e)),
                };
                debug_assert!(data_block.is_some());

                if let Some(data_block) = data_block {
                    self.data_blocks.insert(last_data_block_handle, data_block);
                }
            }
        }

        if self.data_block_handles.is_empty() && self.data_blocks.len() == 1 {
            // We've reached the final block
            // Just consume from it instead
            let block = self.data_blocks.values_mut().next();
            return block.and_then(VecDeque::pop_back).map(Ok);
        }

        let current_hi = self.current_hi.as_ref().expect("upper bound uninitialized");

        let block = self.data_blocks.get_mut(current_hi);

        if let Some(block) = block {
            let item = block.pop_back();

            if block.is_empty() {
                // Load next block
                self.data_blocks.remove(current_hi);

                if let Some(prev_data_block_handle) = self.data_block_handles.pop_back() {
                    self.current_hi = Some(prev_data_block_handle.clone());

                    if Some(&prev_data_block_handle) == self.current_lo.as_ref() {
                        // Do nothing
                        // Next item consumed will use the existing lower block
                    } else {
                        let data_block = match self.load_data_block(&prev_data_block_handle) {
                            Ok(block) => block,
                            Err(e) => return Some(Err(e)),
                        };
                        debug_assert!(data_block.is_some());

                        if let Some(data_block) = data_block {
                            self.data_blocks.insert(prev_data_block_handle, data_block);
                        }
                    }
                }
            }

            item.map(Ok)
        } else {
            None
        }
    }
}
