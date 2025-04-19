// Copyright (c) 2025-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{IndexBlock, NewKeyedBlockHandle};
use crate::{segment::BlockOffset, Slice};
use std::io::Cursor;

#[derive(Default, Debug)]
struct LoScanner {
    offset: usize,
    remaining_in_interval: usize,
    base_key_offset: Option<usize>,
}

/// Specialized reader to scan an index block only in forwards direction
///
/// Is less expensive than a double ended iterator.
pub struct ForwardReader<'a> {
    block: &'a IndexBlock,
    restart_interval: usize,

    lo_scanner: LoScanner,
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct ParsedSlice(pub usize, pub usize);

#[derive(Debug)]
pub struct ParsedItem {
    pub offset: BlockOffset,
    pub size: u32,
    pub prefix: Option<ParsedSlice>,
    pub end_key: ParsedSlice,
}

impl ParsedItem {
    pub fn materialize(&self, bytes: &Slice) -> NewKeyedBlockHandle {
        let end_key = if let Some(prefix) = &self.prefix {
            let prefix_key = &bytes[prefix.0..prefix.1];
            let rest_key = &bytes[self.end_key.0..self.end_key.1];
            Slice::fused(prefix_key, rest_key)
        } else {
            bytes.slice(self.end_key.0..self.end_key.1)
        };

        NewKeyedBlockHandle::new(end_key, self.offset, self.size)
    }
}

impl<'a> ForwardReader<'a> {
    pub fn new(block: &'a IndexBlock) -> Self {
        let restart_interval = block.restart_interval.into();

        Self {
            block,

            restart_interval,

            lo_scanner: LoScanner::default(),
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.lo_scanner.offset = offset;
        self
    }

    fn parse_restart_item(
        block: &IndexBlock,
        offset: &mut usize,
        base_key_offset: &mut Option<usize>,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = IndexBlock::parse_restart_item(&mut reader, *offset)?;

        *offset += reader.position() as usize;
        *base_key_offset = Some(item.end_key.0);

        Some(item)
    }

    fn parse_truncated_item(
        block: &IndexBlock,
        offset: &mut usize,
        base_key_offset: usize,
    ) -> Option<ParsedItem> {
        let bytes = block.bytes();

        // SAFETY: The cursor is advanced by read_ operations which check for EOF,
        // And the cursor starts at 0 - the slice is never empty
        #[warn(unsafe_code)]
        let mut reader = Cursor::new(unsafe { bytes.get_unchecked(*offset..) });

        let item = IndexBlock::parse_truncated_item(&mut reader, *offset, base_key_offset)?;

        *offset += reader.position() as usize;

        Some(item)
    }
}

impl Iterator for ForwardReader<'_> {
    type Item = ParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        let is_restart = self.lo_scanner.remaining_in_interval == 0;

        let item = if is_restart {
            self.lo_scanner.remaining_in_interval = self.restart_interval;

            Self::parse_restart_item(
                self.block,
                &mut self.lo_scanner.offset,
                &mut self.lo_scanner.base_key_offset,
            )
        } else {
            Self::parse_truncated_item(
                self.block,
                &mut self.lo_scanner.offset,
                self.lo_scanner.base_key_offset.expect("should exist"),
            )
        };

        self.lo_scanner.remaining_in_interval -= 1;

        item
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{block::Header, Block, Checksum};
    use test_log::test;

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_simple() -> crate::Result<()> {
        let items = [
            NewKeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            NewKeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            NewKeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(block.item_count(), items.len());

        let iter = block.forward_reader(b"a").unwrap();
        assert_eq!(&items, &*iter.collect::<Vec<_>>());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn v3_index_block_seek() -> crate::Result<()> {
        let items = [
            NewKeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            NewKeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            NewKeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_items(&items, 1)?;
        eprintln!("{bytes:?}");
        eprintln!("{}", String::from_utf8_lossy(&bytes));
        /* eprintln!("encoded into {} bytes", bytes.len()); */

        let block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(block.item_count(), items.len());

        {
            let iter = block.forward_reader(b"a").unwrap();
            assert_eq!(&items, &*iter.into_iter().collect::<Vec<_>>());
        }

        {
            let iter = block.forward_reader(b"b").unwrap();
            assert_eq!(&items, &*iter.into_iter().collect::<Vec<_>>());
        }

        {
            let iter = block.forward_reader(b"c").unwrap();
            assert_eq!(
                items.iter().skip(2).cloned().collect::<Vec<_>>(),
                &*iter.collect::<Vec<_>>(),
            );
        }

        {
            let iter = block.forward_reader(b"def").unwrap();
            assert_eq!(
                items.iter().skip(2).cloned().collect::<Vec<_>>(),
                &*iter.collect::<Vec<_>>(),
            );
        }

        {
            let iter = block.forward_reader(b"zzz");
            assert!(iter.is_none(), "iterator should seek past index block");
        }

        Ok(())
    }
}
