// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{Block, DataBlock};
use crate::{
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    table::{block::BlockType, iter::OwnedDataBlockIter},
    CompressionType, InternalValue, SeqNo,
};
use std::{fs::File, io::BufReader, path::Path, sync::Arc};

/// Table reader that is optimized for consuming an entire table
pub struct Scanner {
    reader: BufReader<File>,
    iter: OwnedDataBlockIter,

    compression: CompressionType,
    block_count: usize,
    read_count: usize,

    global_seqno: SeqNo,

    encryption: Option<Arc<dyn EncryptionProvider>>,
    comparator: SharedComparator,

    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
}

impl Scanner {
    pub fn new(
        path: &Path,
        block_count: usize,
        compression: CompressionType,
        global_seqno: SeqNo,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
    ) -> crate::Result<Self> {
        // TODO: a larger buffer size may be better for HDD, maybe make this configurable
        let mut reader = BufReader::with_capacity(8 * 4_096, File::open(path)?);

        let block = Self::fetch_next_block(
            &mut reader,
            compression,
            encryption.as_deref(),
            #[cfg(zstd_any)]
            zstd_dictionary.as_deref(),
        )?;
        let cmp = comparator.clone();
        let iter = OwnedDataBlockIter::new(block, |b| b.iter(cmp));

        Ok(Self {
            reader,
            iter,

            compression,
            block_count,
            read_count: 1,

            global_seqno,
            encryption,
            comparator,

            #[cfg(zstd_any)]
            zstd_dictionary,
        })
    }

    fn fetch_next_block(
        reader: &mut BufReader<File>,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        let block = Block::from_reader(
            reader,
            compression,
            encryption,
            #[cfg(zstd_any)]
            zstd_dict,
        );

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
            let block = fail_iter!(Self::fetch_next_block(
                &mut self.reader,
                self.compression,
                self.encryption.as_deref(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            ));
            let cmp = self.comparator.clone();
            self.iter = OwnedDataBlockIter::new(block, |b| b.iter(cmp));

            self.read_count += 1;
        }
    }
}
