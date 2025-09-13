// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt},
    segment::{block::Decoder, index_block::IndexBlockParsedItem, KeyedBlockHandle},
};

pub struct Iter<'a> {
    decoder: DoubleEndedPeekable<
        IndexBlockParsedItem,
        Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>,
    >,
}

impl<'a> Iter<'a> {
    #[must_use]
    pub fn new(decoder: Decoder<'a, KeyedBlockHandle, IndexBlockParsedItem>) -> Self {
        let decoder = decoder.double_ended_peekable();
        Self { decoder }
    }

    pub fn seek(&mut self, needle: &[u8]) -> bool {
        self.decoder
            .inner_mut()
            .seek(|end_key| end_key < needle, true)
    }

    pub fn seek_upper(&mut self, needle: &[u8]) -> bool {
        self.decoder
            .inner_mut()
            .seek_upper(|end_key| end_key <= needle, true)
    }
}

impl Iterator for Iter<'_> {
    type Item = IndexBlockParsedItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.decoder.next()
    }
}

impl DoubleEndedIterator for Iter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.decoder.next_back()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        segment::{
            block::{BlockType, Header, ParsedItem},
            Block, BlockOffset, IndexBlock, KeyedBlockHandle,
        },
        Checksum,
    };
    use test_log::test;

    #[test]
    fn v3_index_block_iter_seek_before_start() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(iter.seek(b"a"), "should seek");

        let iter = index_block
            .iter()
            .map(|item| item.materialize(&index_block.inner.data));

        let real_items: Vec<_> = iter.collect();

        assert_eq!(items, &*real_items);

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_seek_start() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(iter.seek(b"b"), "should seek");

        let real_items: Vec<_> = iter
            .map(|item| item.materialize(&index_block.inner.data))
            .collect();

        assert_eq!(items, &*real_items);

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_seek_middle() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(iter.seek(b"c"), "should seek");

        let real_items: Vec<_> = iter
            .map(|item| item.materialize(&index_block.inner.data))
            .collect();

        assert_eq!(
            items.iter().skip(2).cloned().collect::<Vec<_>>(),
            &*real_items,
        );

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_rev_seek() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(iter.seek_upper(b"c"), "should seek");

        let real_items: Vec<_> = iter
            .map(|item| item.materialize(&index_block.inner.data))
            .collect();

        assert_eq!(items.to_vec(), &*real_items);

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_rev_seek_2() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(iter.seek_upper(b"e"), "should seek");

        let real_items: Vec<_> = iter
            .map(|item| item.materialize(&index_block.inner.data))
            .collect();

        assert_eq!(items.to_vec(), &*real_items);

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_rev_seek_3() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(iter.seek_upper(b"b"), "should seek");

        let real_items: Vec<_> = iter
            .map(|item| item.materialize(&index_block.inner.data))
            .collect();

        assert_eq!(
            items.iter().take(2).cloned().collect::<Vec<_>>(),
            &*real_items,
        );

        Ok(())
    }

    #[test]
    #[ignore]
    fn v3_index_block_iter_too_far() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"b".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"bcdef".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"def".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        let mut iter = index_block.iter();
        assert!(!iter.seek(b"zzz"), "should not seek");

        let real_items: Vec<_> = iter
            .map(|item| item.materialize(&index_block.inner.data))
            .collect();

        assert_eq!(&[] as &[KeyedBlockHandle], &*real_items);

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_span() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"a".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"a".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"b".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        {
            let mut iter = index_block.iter();
            assert!(iter.seek(b"a"), "should seek");

            let real_items: Vec<_> = iter
                .map(|item| item.materialize(&index_block.inner.data))
                .collect();

            assert_eq!(items.to_vec(), &*real_items);
        }

        {
            let mut iter = index_block.iter();
            assert!(iter.seek(b"b"), "should seek");

            let real_items: Vec<_> = iter
                .map(|item| item.materialize(&index_block.inner.data))
                .collect();

            assert_eq!(
                items.iter().skip(2).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_rev_span() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"a".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"a".into(), BlockOffset(6_000), 7_000),
            KeyedBlockHandle::new(b"b".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        {
            let mut iter = index_block.iter();
            assert!(iter.seek_upper(b"a"), "should seek");

            let real_items: Vec<_> = iter
                .map(|item| item.materialize(&index_block.inner.data))
                .collect();

            assert_eq!(items.to_vec(), &*real_items);
        }

        {
            let mut iter = index_block.iter();
            assert!(iter.seek_upper(b"b"), "should seek");

            let real_items: Vec<_> = iter
                .map(|item| item.materialize(&index_block.inner.data))
                .collect();

            assert_eq!(items.to_vec(), &*real_items);
        }

        Ok(())
    }

    #[test]
    fn v3_index_block_iter_range_1() -> crate::Result<()> {
        let items = [
            KeyedBlockHandle::new(b"a".into(), BlockOffset(0), 6_000),
            KeyedBlockHandle::new(b"b".into(), BlockOffset(13_000), 5_000),
            KeyedBlockHandle::new(b"c".into(), BlockOffset(13_000), 5_000),
            KeyedBlockHandle::new(b"d".into(), BlockOffset(13_000), 5_000),
            KeyedBlockHandle::new(b"e".into(), BlockOffset(13_000), 5_000),
        ];

        let bytes = IndexBlock::encode_into_vec(&items)?;

        let index_block = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
                previous_block_offset: BlockOffset(0),
            },
        });

        assert_eq!(index_block.len(), items.len());

        {
            let mut iter = index_block.iter();
            assert!(iter.seek(b"b"), "should seek");
            assert!(iter.seek_upper(b"c"), "should seek");

            let real_items: Vec<_> = iter
                .map(|item| item.materialize(&index_block.inner.data))
                .collect();

            assert_eq!(
                items.iter().skip(1).take(3).cloned().collect::<Vec<_>>(),
                &*real_items,
            );
        }

        {
            let mut iter = index_block.iter();
            assert!(iter.seek(b"b"), "should seek");
            assert!(iter.seek_upper(b"c"), "should seek");

            let real_items: Vec<_> = iter
                .map(|item| item.materialize(&index_block.inner.data))
                .collect();

            assert_eq!(
                items
                    .iter()
                    .skip(1)
                    .take(3)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>(),
                &*real_items,
            );
        }

        Ok(())
    }
}
