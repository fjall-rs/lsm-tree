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
    pub fn new<P: AsRef<Path>>(path: P, block_count: usize) -> crate::Result<Self> {
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
            self.buffer.extend(block.items);
            self.read_count += 1;
        }
    }
}

pub type CompactionReader<'a> = Box<dyn Iterator<Item = crate::Result<InternalValue>> + 'a>;

#[derive(Eq)]
struct HeapItem(usize, InternalValue);

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.1.key == other.1.key
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.key.cmp(&other.1.key)
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.1.key.cmp(&other.1.key))
    }
}

use interval_heap::IntervalHeap;

/// Merges multiple KV iterators
pub struct CompactionMerger<'a> {
    iterators: Vec<CompactionReader<'a>>,
    heap: IntervalHeap<HeapItem>,

    initialized_lo: bool,
}

impl<'a> CompactionMerger<'a> {
    #[must_use]
    pub fn new(iterators: Vec<CompactionReader<'a>>) -> Self {
        let heap = IntervalHeap::with_capacity(iterators.len());

        let iterators = iterators.into_iter().collect::<Vec<_>>();

        Self {
            iterators,
            heap,
            initialized_lo: false,
        }
    }

    #[allow(clippy::indexing_slicing)]
    fn initialize_lo(&mut self) -> crate::Result<()> {
        for idx in 0..self.iterators.len() {
            if let Some(item) = self.iterators[idx].next() {
                let item = item?;
                self.heap.push(HeapItem(idx, item));
            }
        }
        self.initialized_lo = true;
        Ok(())
    }
}

impl<'a> Iterator for CompactionMerger<'a> {
    type Item = crate::Result<InternalValue>;

    #[allow(clippy::indexing_slicing)]
    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized_lo {
            fail_iter!(self.initialize_lo());
        }

        let min_item = self.heap.pop_min()?;

        if let Some(next_item) = self.iterators[min_item.0].next() {
            let next_item = fail_iter!(next_item);
            self.heap.push(HeapItem(min_item.0, next_item));
        }

        Some(Ok(min_item.1))
    }
}
