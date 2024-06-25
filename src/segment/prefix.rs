use super::{block_index::BlockIndex, id::GlobalSegmentId, range::Range, value_block::CachePolicy};
use crate::{
    block_cache::BlockCache, descriptor_table::FileDescriptorTable, value::UserKey, Value,
};
use std::{
    ops::Bound::{self, Excluded, Included, Unbounded},
    sync::Arc,
};

#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn prefix_to_range(prefix: &[u8]) -> (Bound<UserKey>, Bound<UserKey>) {
    if prefix.is_empty() {
        return (Unbounded, Unbounded);
    }

    let mut end = prefix.to_vec();

    for i in (0..end.len()).rev() {
        let byte = end.get_mut(i).expect("should be in bounds");

        if *byte < 255 {
            *byte += 1;
            end.truncate(i + 1);
            return (Included(prefix.into()), Excluded(end.into()));
        }
    }

    (Included(prefix.into()), Unbounded)
}

#[allow(clippy::module_name_repetitions)]
pub struct PrefixedReader(Range);

impl PrefixedReader {
    pub fn new(
        data_block_boundary: u64,
        descriptor_table: Arc<FileDescriptorTable>,
        segment_id: GlobalSegmentId,
        block_cache: Arc<BlockCache>,
        block_index: Arc<BlockIndex>,
        prefix: &[u8],
    ) -> Self {
        Self(Range::new(
            data_block_boundary,
            descriptor_table,
            segment_id,
            block_cache,
            block_index,
            prefix_to_range(prefix),
        ))
    }

    /// Sets the cache policy
    #[must_use]
    pub fn cache_policy(mut self, policy: CachePolicy) -> Self {
        self.0 = self.0.cache_policy(policy);
        self
    }
}

impl Iterator for PrefixedReader {
    type Item = crate::Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for PrefixedReader {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        block_cache::BlockCache,
        descriptor_table::FileDescriptorTable,
        segment::{
            block_index::BlockIndex,
            prefix::PrefixedReader,
            reader::Reader,
            writer::{Options, Writer},
        },
        value::{SeqNo, ValueType},
        Value,
    };
    use std::sync::Arc;
    use test_log::test;

    #[test]
    fn prefix_to_range_basic() {
        let prefix = b"abc";
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (Included(Arc::from(*b"abc")), Excluded(Arc::from(*b"abd")))
        );
    }

    #[test]
    fn prefix_to_range_empty() {
        let prefix = b"";
        let range = prefix_to_range(prefix);
        assert_eq!(range, (Unbounded, Unbounded));
    }

    #[test]
    fn prefix_to_range_single_char() {
        let prefix = b"a";
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (Included(Arc::from(*b"a")), Excluded(Arc::from(*b"b")))
        );
    }

    #[test]
    fn prefix_to_range_1() {
        let prefix = &[0, 250];
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (Included(Arc::from([0, 250])), Excluded(Arc::from([0, 251])))
        );
    }

    #[test]
    fn prefix_to_range_2() {
        let prefix = &[0, 250, 50];
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (
                Included(Arc::from([0, 250, 50])),
                Excluded(Arc::from([0, 250, 51]))
            )
        );
    }

    #[test]
    fn prefix_to_range_3() {
        let prefix = &[255, 255, 255];
        let range = prefix_to_range(prefix);
        assert_eq!(range, (Included(Arc::from([255, 255, 255])), Unbounded));
    }

    #[test]
    fn prefix_to_range_char_max() {
        let prefix = &[0, 255];
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (Included(Arc::from([0, 255])), Excluded(Arc::from([1])))
        );
    }

    #[test]
    fn prefix_to_range_char_max_2() {
        let prefix = &[0, 2, 255];
        let range = prefix_to_range(prefix);
        assert_eq!(
            range,
            (
                Included(Arc::from([0, 2, 255])),
                Excluded(Arc::from([0, 3]))
            )
        );
    }

    #[test]
    fn segment_prefix_lots_of_prefixes() -> crate::Result<()> {
        for item_count in [1, 10, 100, 1_000, 10_000] {
            let folder = tempfile::tempdir()?.into_path();

            let mut writer = Writer::new(Options {
                segment_id: 0,

                folder: folder.clone(),
                evict_tombstones: false,
                block_size: 4096,

                #[cfg(feature = "bloom")]
                bloom_fp_rate: 0.01,
            })?;

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/a/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    0,
                    ValueType::Value,
                );
                writer.write(item)?;
            }

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/b/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    0,
                    ValueType::Value,
                );
                writer.write(item)?;
            }

            for x in 0_u64..item_count {
                let item = Value::new(
                    {
                        let mut v = b"a/c/".to_vec();
                        v.extend_from_slice(&x.to_be_bytes());
                        v
                    },
                    nanoid::nanoid!().as_bytes(),
                    0,
                    ValueType::Value,
                );
                writer.write(item)?;
            }

            let trailer = writer.finish()?.expect("should exist");

            let segment_file_path = folder.join("0");

            let table = Arc::new(FileDescriptorTable::new(512, 1));
            table.insert(&segment_file_path, (0, 0).into());

            let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
            let block_index = Arc::new(BlockIndex::from_file(
                segment_file_path,
                trailer.offsets.tli_ptr,
                (0, 0).into(),
                table.clone(),
                Arc::clone(&block_cache),
            )?);

            let iter = Reader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                block_cache.clone(),
                0,
                None,
            );
            assert_eq!(iter.count() as u64, item_count * 3);

            let iter = PrefixedReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                b"a/b/",
            );

            assert_eq!(iter.count() as u64, item_count);

            let iter = PrefixedReader::new(
                trailer.offsets.index_block_ptr,
                table,
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                b"a/b/",
            );

            assert_eq!(iter.rev().count() as u64, item_count);
        }

        Ok(())
    }

    #[test]
    fn segment_prefix_reader_prefixed_items() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = [
            b"a".to_vec(),
            b"a/a".to_vec(),
            b"a/b".to_vec(),
            b"a/b/a".to_vec(),
            b"a/b/z".to_vec(),
            b"a/z/a".to_vec(),
            b"aaa".to_vec(),
            b"aaa/a".to_vec(),
            b"aaa/z".to_vec(),
            b"b/a".to_vec(),
            b"b/b".to_vec(),
        ]
        .into_iter()
        .enumerate()
        .map(|(idx, key)| {
            Value::new(
                key,
                nanoid::nanoid!().as_bytes(),
                idx as SeqNo,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, 0).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

        let expected = [
            (b"a".to_vec(), 9),
            (b"a/".to_vec(), 5),
            (b"b".to_vec(), 2),
            (b"b/".to_vec(), 2),
            (b"a".to_vec(), 9),
            (b"a/".to_vec(), 5),
            (b"b".to_vec(), 2),
            (b"b/".to_vec(), 2),
        ];

        for (prefix_key, item_count) in &expected {
            let iter = PrefixedReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                prefix_key,
            );

            assert_eq!(iter.count(), *item_count);
        }

        for (prefix_key, item_count) in &expected {
            let iter = PrefixedReader::new(
                trailer.offsets.index_block_ptr,
                table.clone(),
                (0, 0).into(),
                Arc::clone(&block_cache),
                Arc::clone(&block_index),
                prefix_key,
            );

            assert_eq!(iter.rev().count(), *item_count);
        }

        Ok(())
    }

    #[test]
    fn segment_prefix_ping_pong() -> crate::Result<()> {
        let folder = tempfile::tempdir()?.into_path();

        let mut writer = Writer::new(Options {
            segment_id: 0,

            folder: folder.clone(),
            evict_tombstones: false,
            block_size: 4096,

            #[cfg(feature = "bloom")]
            bloom_fp_rate: 0.01,
        })?;

        let items = [
            b"aa", b"ab", b"ac", b"ba", b"bb", b"bc", b"ca", b"cb", b"cc", b"da", b"db", b"dc",
        ]
        .into_iter()
        .enumerate()
        .map(|(idx, key)| {
            Value::new(
                key.to_vec(),
                nanoid::nanoid!().as_bytes(),
                idx as SeqNo,
                ValueType::Value,
            )
        });

        for item in items {
            writer.write(item)?;
        }

        let trailer = writer.finish()?.expect("should exist");

        let segment_file_path = folder.join("0");

        let table = Arc::new(FileDescriptorTable::new(512, 1));
        table.insert(&segment_file_path, (0, 0).into());

        let block_cache = Arc::new(BlockCache::with_capacity_bytes(10 * 1_024 * 1_024));
        let block_index = Arc::new(BlockIndex::from_file(
            segment_file_path,
            trailer.offsets.tli_ptr,
            (0, 0).into(),
            table.clone(),
            Arc::clone(&block_cache),
        )?);

        let iter = PrefixedReader::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
            b"d",
        );
        assert_eq!(3, iter.count());

        let iter = PrefixedReader::new(
            trailer.offsets.index_block_ptr,
            table.clone(),
            (0, 0).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
            b"d",
        );
        assert_eq!(3, iter.rev().count());

        let mut iter = PrefixedReader::new(
            trailer.offsets.index_block_ptr,
            table,
            (0, 0).into(),
            Arc::clone(&block_cache),
            Arc::clone(&block_index),
            b"d",
        );

        assert_eq!(Arc::from(*b"da"), iter.next().expect("should exist")?.key);
        assert_eq!(
            Arc::from(*b"dc"),
            iter.next_back().expect("should exist")?.key
        );
        assert_eq!(Arc::from(*b"db"), iter.next().expect("should exist")?.key);

        assert!(iter.next().is_none());

        Ok(())
    }
}
