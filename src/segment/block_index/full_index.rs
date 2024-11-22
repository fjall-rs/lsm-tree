use super::{block_handle::KeyedBlockHandle, BlockIndex};
use crate::segment::{
    block_index::IndexBlock,
    value_block::{BlockOffset, CachePolicy},
};
use std::{fs::File, io::Seek, path::Path};

/// Index that translates item keys to block handles
///
/// The index is fully loaded into memory.
pub struct FullBlockIndex(Box<[KeyedBlockHandle]>);

impl FullBlockIndex {
    pub fn from_file<P: AsRef<Path>>(
        path: P,
        metadata: &crate::segment::meta::Metadata,
        offsets: &crate::segment::file_offsets::FileOffsets,
    ) -> crate::Result<Self> {
        let path = path.as_ref();
        let cnt = metadata.index_block_count as usize;

        log::trace!(
            "reading full block index from {path:?} at idx_ptr={} ({cnt} index blocks)",
            offsets.index_block_ptr,
        );

        let mut file = File::open(path)?;
        file.seek(std::io::SeekFrom::Start(*offsets.index_block_ptr))?;

        let mut block_handles = Vec::with_capacity(cnt);

        for _ in 0..cnt {
            let idx_block = IndexBlock::from_reader(&mut file)?.items;
            // TODO: 1.80? IntoIter impl for Box<[T]>
            block_handles.extend(idx_block.into_vec());
        }

        debug_assert!(!block_handles.is_empty());
        debug_assert_eq!(cnt, block_handles.len());

        Ok(Self(block_handles.into_boxed_slice()))
    }
}

impl BlockIndex for FullBlockIndex {
    fn get_lowest_block_containing_key(
        &self,
        key: &[u8],
        _: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        use super::RawBlockIndex;

        self.0
            .get_lowest_block_containing_key(key, CachePolicy::Read)
            .map(|x| x.map(|x| x.offset))
    }

    /// Gets the last block handle that may contain the given item
    fn get_last_block_containing_key(
        &self,
        key: &[u8],
        cache_policy: CachePolicy,
    ) -> crate::Result<Option<BlockOffset>> {
        use super::RawBlockIndex;

        self.0
            .get_last_block_containing_key(key, cache_policy)
            .map(|x| x.map(|x| x.offset))
    }

    fn get_last_block_handle(&self, _: CachePolicy) -> crate::Result<BlockOffset> {
        use super::RawBlockIndex;

        self.0
            .get_last_block_handle(CachePolicy::Read)
            .map(|x| x.offset)
    }
}
