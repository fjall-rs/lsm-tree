// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, DataBlock};
use crate::{
    direct_io::ChunkedReader,
    table::{block::BlockType, iter::OwnedDataBlockIter},
    CompressionType, InternalValue, SeqNo,
};
use std::path::Path;

/// Table reader that is optimized for consuming an entire table
pub struct Scanner {
    reader: ChunkedReader,
    iter: OwnedDataBlockIter,

    compression: CompressionType,
    block_count: usize,
    read_count: usize,

    global_seqno: SeqNo,
}

impl Scanner {
    /// Opens a table file for sequential scanning. When `use_direct_io` is
    /// `true` the underlying file is opened with platform direct I/O; the
    /// compaction worker passes [`crate::Config::use_direct_io_for_compaction_reads`].
    pub fn new(
        path: &Path,
        block_count: usize,
        compression: CompressionType,
        global_seqno: SeqNo,
        use_direct_io: bool,
    ) -> crate::Result<Self> {
        let mut reader = ChunkedReader::open(path, use_direct_io)?;

        let block = Self::fetch_next_block(&mut reader, compression)?;
        let iter = OwnedDataBlockIter::new(block, DataBlock::iter);

        Ok(Self {
            reader,
            iter,

            compression,
            block_count,
            read_count: 1,

            global_seqno,
        })
    }

    fn fetch_next_block(
        reader: &mut ChunkedReader,
        compression: CompressionType,
    ) -> crate::Result<DataBlock> {
        let block = Block::from_reader(reader, compression);

        match block {
            Ok(block) => {
                if block.header.block_type != BlockType::Data {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }

                Ok(DataBlock::new(block))
            }
            Err(e) => Err(e),
        }
    }
}

impl Iterator for Scanner {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(mut item) = self.iter.next() {
                item.key.seqno += self.global_seqno;
                return Some(Ok(item));
            }

            if self.read_count >= self.block_count {
                return None;
            }

            // Init new block
            let block = fail_iter!(Self::fetch_next_block(&mut self.reader, self.compression));
            self.iter = OwnedDataBlockIter::new(block, DataBlock::iter);

            self.read_count += 1;
        }
    }
}
