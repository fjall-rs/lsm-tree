use super::value_block::ValueBlock;
use crate::InternalValue;
use std::{collections::VecDeque, fs::File, io::BufReader, path::Path};

/// Segment reader that is optimized for consuming an entire segment
pub struct Scanner {
    reader: BufReader<File>,

    block_count: usize,
    read_count: usize,

    buffer: VecDeque<InternalValue>,
}

impl Scanner {
    pub fn new(path: &Path, block_count: usize) -> crate::Result<Self> {
        // TODO: a larger buffer size may be better for HDD
        let reader = BufReader::with_capacity(8 * 4_096, File::open(path)?);

        Ok(Self {
            reader,
            block_count,
            read_count: 0,
            buffer: VecDeque::new(),
        })
    }
}

impl Iterator for Scanner {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.buffer.pop_front() {
                return Some(Ok(item));
            }

            if self.read_count >= self.block_count {
                return None;
            }

            let block = ValueBlock::from_reader(&mut self.reader);
            let block = fail_iter!(block);

            // TODO: 1.80? IntoIter impl for Box<[T]>
            self.buffer.extend(block.items.into_vec());

            self.read_count += 1;
        }
    }
}

pub type CompactionReader<'a> = Box<dyn Iterator<Item = crate::Result<InternalValue>> + 'a>;
