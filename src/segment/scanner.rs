use super::value_block::ValueBlock;
use crate::InternalValue;
use std::{collections::VecDeque, fs::File, io::BufReader, iter::Peekable, path::Path};

/// Segment reader that is optimized for consuming an entire segment
pub struct Scanner {
    reader: BufReader<File>,

    block_count: usize,
    read_count: usize,

    buffer: VecDeque<InternalValue>,
}

impl Scanner {
    pub fn new<P: AsRef<Path>>(path: P, block_count: usize) -> crate::Result<Self> {
        let reader = BufReader::with_capacity(64_000, File::open(path)?);

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

/// Merges multiple KV iterators
pub struct CompactionMerger<'a> {
    iterators: Vec<Peekable<CompactionReader<'a>>>,
}

impl<'a> CompactionMerger<'a> {
    #[must_use]
    pub fn new(iterators: Vec<CompactionReader<'a>>) -> Self {
        let iterators = iterators
            .into_iter()
            .map(std::iter::Iterator::peekable)
            .collect::<Vec<_>>();

        Self { iterators }
    }
}

impl<'a> Iterator for CompactionMerger<'a> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut idx_with_err = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                if val.is_err() {
                    idx_with_err = Some(idx);
                }
            }
        }

        if let Some(idx) = idx_with_err {
            let err = self
                .iterators
                .get_mut(idx)
                .expect("should exist")
                .next()
                .expect("should not be empty");

            if let Err(e) = err {
                return Some(Err(e));
            }

            panic!("logic error");
        }

        let mut min: Option<(usize, &InternalValue)> = None;

        for (idx, val) in self.iterators.iter_mut().map(|x| x.peek()).enumerate() {
            if let Some(val) = val {
                match val {
                    Ok(val) => {
                        if let Some((_, min_val)) = min {
                            if val.key < min_val.key {
                                min = Some((idx, val));
                            }
                        } else {
                            min = Some((idx, val));
                        }
                    }
                    _ => panic!("already checked for errors"),
                }
            }
        }

        if let Some((idx, _)) = min {
            let value = self
                .iterators
                .get_mut(idx)?
                .next()?
                .expect("should not be error");

            Some(Ok(value))
        } else {
            None
        }
    }
}
