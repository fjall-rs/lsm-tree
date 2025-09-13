// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod block_handle;
mod iter;

pub use block_handle::{BlockHandle, KeyedBlockHandle};
pub use iter::Iter;

use super::{
    block::{BlockOffset, Encoder, Trailer},
    Block,
};
use crate::segment::{
    block::{Decoder, ParsedItem},
    util::{compare_prefixed_slice, SliceIndexes},
};
use crate::Slice;

#[derive(Debug)]
pub struct IndexBlockParsedItem {
    pub offset: BlockOffset,
    pub size: u32,
    pub prefix: Option<SliceIndexes>,
    pub end_key: SliceIndexes,
}

impl ParsedItem<KeyedBlockHandle> for IndexBlockParsedItem {
    fn compare_key(&self, needle: &[u8], bytes: &[u8]) -> std::cmp::Ordering {
        if let Some(prefix) = &self.prefix {
            let prefix = unsafe { bytes.get_unchecked(prefix.0..prefix.1) };
            let rest_key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            compare_prefixed_slice(prefix, rest_key, needle)
        } else {
            let key = unsafe { bytes.get_unchecked(self.end_key.0..self.end_key.1) };
            key.cmp(needle)
        }
    }

    fn key_offset(&self) -> usize {
        self.end_key.0
    }

    fn materialize(&self, bytes: &Slice) -> KeyedBlockHandle {
        // NOTE: We consider the prefix and key slice indexes to be trustworthy
        #[allow(clippy::indexing_slicing)]
        let key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.end_key.0..self.end_key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.end_key.0..self.end_key.1)
        };

        KeyedBlockHandle::new(key, self.offset, self.size)
    }
}

/// Block that contains block handles (file offset + size)
#[derive(Clone)]
pub struct IndexBlock {
    pub inner: Block,
}

impl IndexBlock {
    #[must_use]
    pub fn new(inner: Block) -> Self {
        Self { inner }
    }

    /// Returns the amount of items in the block.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        Trailer::new(&self.inner).item_count()
    }

    #[must_use]
    #[allow(clippy::iter_without_into_iter)]
    pub fn iter(&self) -> Iter<'_> {
        Iter::new(Decoder::<KeyedBlockHandle, IndexBlockParsedItem>::new(
            &self.inner,
        ))
    }

    #[cfg(test)]
    pub fn encode_into_vec(items: &[KeyedBlockHandle]) -> crate::Result<Vec<u8>> {
        let mut buf = vec![];

        Self::encode_into(&mut buf, items)?;

        Ok(buf)
    }

    pub fn encode_into(
        writer: &mut Vec<u8>,
        items: &[KeyedBlockHandle],
        // restart_interval: u8, // TODO: support prefix truncation + delta encoding
    ) -> crate::Result<()> {
        let first_key = items.first().expect("chunk should not be empty").end_key();

        let mut serializer = Encoder::<'_, BlockOffset, KeyedBlockHandle>::new(
            writer,
            items.len(),
            1,   // TODO: hard coded for now
            0.0, // NOTE: Index blocks do not support hash index
            first_key,
        );

        for item in items {
            serializer.write(item)?;
        }

        serializer.finish()
    }
}
