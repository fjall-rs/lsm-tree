use super::Reader as SegmentReader;
use crate::Value;
use std::collections::VecDeque;

#[allow(unused)]
use crate::merge::MergeIterator;

/// Reads through a disjoint, sorted set of segment readers
pub struct MultiReader {
    readers: VecDeque<SegmentReader>, //  TODO: maybe Vec of BoxedIterators...
}

impl MultiReader {
    #[must_use]
    pub fn new(readers: VecDeque<SegmentReader>) -> Self {
        Self { readers }
    }
}

impl Iterator for MultiReader {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.readers.front_mut()?.next() {
                return Some(item);
            }

            // NOTE: Current reader has no more items, load next reader if it exists and try again
            self.readers.pop_front();
        }
    }
}

impl DoubleEndedIterator for MultiReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.readers.back_mut()?.next_back() {
                return Some(item);
            }

            // NOTE: Current reader has no more items, load next reader if it exists and try again
            self.readers.pop_back();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        descriptor_table::FileDescriptorTable,
        file::BLOCKS_FILE,
        segment::{
            block_index::BlockIndex,
            meta::Metadata,
            writer::{Options, Writer},
        },
        BlockCache, Segment,
    };
    use nanoid::nanoid;
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn segment_multi_reader_basic() -> crate::Result<()> {
        let tempdir = tempfile::tempdir()?;

        let descriptor_table = Arc::new(FileDescriptorTable::new(100, 1));
        let block_cache = Arc::new(BlockCache::with_capacity_bytes(0));

        let ids = [
            ["a", "b", "c"],
            ["d", "e", "f"],
            ["g", "h", "i"],
            ["j", "k", "l"],
        ];

        let segments = ids
            .into_iter()
            .map(|keys| {
                let segment_id: Arc<str> = nanoid!().into();

                let folder = tempdir.path().join(&*segment_id);
                std::fs::create_dir_all(&folder)?;

                let mut writer = Writer::new(Options {
                    block_size: 4_096,
                    evict_tombstones: false,
                    path: folder.clone(),

                    #[cfg(feature = "bloom")]
                    bloom_fp_rate: 0.01,
                })?;

                for key in keys {
                    writer.write(Value {
                        key: (*key.as_bytes()).into(),
                        value: vec![].into(),
                        seqno: 0,
                        value_type: crate::ValueType::Value,
                    })?;
                }
                writer.finish()?;
                let metadata = Metadata::from_writer(segment_id.clone(), writer)?;
                metadata.write_to_file(&folder)?;

                descriptor_table.insert(folder.join(BLOCKS_FILE), segment_id.clone());

                Ok::<_, crate::Error>(Segment {
                    block_cache: block_cache.clone(),
                    block_index: Arc::new(BlockIndex::from_file(
                        segment_id,
                        descriptor_table.clone(),
                        folder,
                        block_cache.clone(),
                    )?),
                    metadata,
                    descriptor_table: descriptor_table.clone(),

                    #[cfg(feature = "bloom")]
                    bloom_filter: crate::bloom::BloomFilter::with_fp_rate(1, 0.1),
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        #[allow(clippy::unwrap_used)]
        {
            let readers = segments
                .iter()
                .map(|segment| {
                    SegmentReader::new(
                        descriptor_table.clone(),
                        segment.metadata.id.clone(),
                        None,
                        segment.block_index.clone(),
                        None,
                        None,
                    )
                })
                .collect::<Vec<_>>();

            let multi_reader = MultiReader::new(readers.into());

            let mut iter = multi_reader.into_iter().flatten();

            assert_eq!(Arc::from(*b"a"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"b"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"c"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"d"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"e"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"f"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"g"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"h"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"i"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"j"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"k"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"l"), iter.next().unwrap().key);
        }

        #[allow(clippy::unwrap_used)]
        {
            let readers = segments
                .iter()
                .map(|segment| {
                    SegmentReader::new(
                        descriptor_table.clone(),
                        segment.metadata.id.clone(),
                        None,
                        segment.block_index.clone(),
                        None,
                        None,
                    )
                })
                .collect::<Vec<_>>();

            let multi_reader = MultiReader::new(readers.into());

            let mut iter = multi_reader.into_iter().rev().flatten();

            assert_eq!(Arc::from(*b"l"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"k"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"j"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"i"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"h"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"g"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"f"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"e"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"d"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"c"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"b"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"a"), iter.next().unwrap().key);
        }

        #[allow(clippy::unwrap_used)]
        {
            let readers = segments
                .iter()
                .map(|segment| {
                    SegmentReader::new(
                        descriptor_table.clone(),
                        segment.metadata.id.clone(),
                        None,
                        segment.block_index.clone(),
                        None,
                        None,
                    )
                })
                .collect::<Vec<_>>();

            let multi_reader = MultiReader::new(readers.into());

            let mut iter = multi_reader.into_iter().flatten();

            assert_eq!(Arc::from(*b"a"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"l"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"b"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"k"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"c"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"j"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"d"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"i"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"e"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"h"), iter.next_back().unwrap().key);
            assert_eq!(Arc::from(*b"f"), iter.next().unwrap().key);
            assert_eq!(Arc::from(*b"g"), iter.next_back().unwrap().key);
        }

        Ok(())
    }
}
