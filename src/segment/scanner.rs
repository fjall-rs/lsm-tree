// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, DataBlock};
use crate::{
    segment::{block::BlockType, iter::OwnedDataBlockIter},
    CompressionType, InternalValue,
};
use std::{fs::File, io::BufReader, path::Path};

/// Segment reader that is optimized for consuming an entire segment
pub struct Scanner {
    reader: BufReader<File>,
    iter: OwnedDataBlockIter,

    compression: CompressionType,
    block_count: usize,
    read_count: usize,
}

impl Scanner {
    pub fn new(
        path: &Path,
        block_count: usize,
        compression: CompressionType,
    ) -> crate::Result<Self> {
        // TODO: a larger buffer size may be better for HDD, maybe make this configurable
        let mut reader = BufReader::with_capacity(8 * 4_096, File::open(path)?);

        let block = Self::fetch_next_block(&mut reader, compression)?;
        let iter = OwnedDataBlockIter::new(block, DataBlock::iter);

        Ok(Self {
            reader,
            iter,

            compression,
            block_count,
            read_count: 1,
        })
    }

    fn fetch_next_block(
        reader: &mut BufReader<File>,
        compression: CompressionType,
    ) -> crate::Result<DataBlock> {
        let block = Block::from_reader(reader, compression);

        match block {
            Ok(block) => {
                if block.header.block_type != BlockType::Data {
                    return Err(crate::Error::Decode(crate::DecodeError::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    ))));
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
            if let Some(item) = self.iter.next() {
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
