#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Result, Unstructured};
use lsm_tree::{InternalValue, SeqNo, SequenceNumberCounter, ValueType};
use std::sync::Arc;

#[derive(Arbitrary, Eq, PartialEq, Debug, Copy, Clone)]
enum IndexType {
    Full,
    Volatile,
    TwoLevel,
}

#[derive(Arbitrary, Eq, PartialEq, Debug, Copy, Clone)]
enum FilterType {
    Full,
    Volatile,
    Partitioned,
}

#[derive(Arbitrary, Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum FuzzyValueType {
    Value,
    Tombstone,
    // TODO: single delete
}

impl Into<ValueType> for FuzzyValueType {
    fn into(self) -> ValueType {
        match self {
            Self::Value => ValueType::Value,
            Self::Tombstone => ValueType::Tombstone,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FuzzyValue(InternalValue);

impl Ord for FuzzyValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.key.cmp(&other.0.key)
    }
}

impl PartialOrd for FuzzyValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.key.partial_cmp(&other.0.key)
    }
}

impl<'a> Arbitrary<'a> for FuzzyValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let key = Vec::<u8>::arbitrary(u)?;
        let value = Vec::<u8>::arbitrary(u)?;

        // Seqnos never have a leading 1 (they are 63-bit numbers, not 64)
        let seqno = u64::arbitrary(u)?;
        let seqno = seqno & 0x7FFF_FFFF_FFFF_FFFF;

        let vtype = FuzzyValueType::arbitrary(u)?;

        let key = if key.is_empty() { vec![0] } else { key };

        Ok(Self(InternalValue::from_components(
            key,
            value,
            seqno,
            vtype.into(),
        )))
    }
}

fn generate_ping_pong_code(seed: u64, len: usize) -> Vec<u8> {
    use rand::SeedableRng;
    use rand::prelude::*;
    use rand_chacha::ChaCha8Rng;

    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    (0..len).map(|_| rng.random_range(0..=1)).collect()
}

fn main() {
    use rand::SeedableRng;
    use rand::prelude::*;

    fuzz!(|data: &[u8]| {
        /*  let data = &[
            117, 3, 0, 42, 117, 147, 87, 255, 253, 43, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            251, 45,
        ]; */

        let mut unstructured = Unstructured::new(data);

        let seed = u64::arbitrary(&mut unstructured).unwrap();
        let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);

        let restart_interval = u8::arbitrary(&mut unstructured).unwrap();
        let restart_interval = restart_interval.max(1);

        let index_type = IndexType::arbitrary(&mut unstructured).unwrap();
        let filter_type = FilterType::arbitrary(&mut unstructured).unwrap();

        let data_block_size = rng.random_range(1..64_000);
        let index_block_size = rng.random_range(1..64_000);

        let item_count = rng.random_range(1..200);

        let hash_ratio = rng.random_range(0.0..8.0);

        // eprintln!("restart_interval={restart_interval}, hash_ratio={hash_ratio}");

        let mut items = (0..item_count)
            .map(|_| FuzzyValue::arbitrary(&mut unstructured).unwrap())
            .collect::<Vec<_>>();

        assert!(!items.is_empty());

        items.sort();
        items.dedup();

        /* eprintln!("-- items --");
        for item in items.iter().map(|value| &value.0) {
            eprintln!(
                r#"InternalValue::from_components({:?}, {:?}, {}, {:?}),"#,
                item.key.user_key, item.value, item.key.seqno, item.key.value_type,
            );
        }

        if items.len() > 100 {
            eprintln!("================== {}. ", items.len());
        } */

        let dir = tempfile::tempdir_in("/king/fuzz").unwrap();
        let file = dir.path().join("table_fuzz");

        {
            let mut writer = lsm_tree::table::Writer::new(file.clone(), 0, 0)
                .unwrap()
                .use_data_block_restart_interval(restart_interval)
                .use_data_block_size(data_block_size)
                .use_data_block_hash_ratio(hash_ratio);

            if index_type == IndexType::TwoLevel {
                writer = writer.use_partitioned_index();
            }
            if filter_type == FilterType::Partitioned {
                writer = writer.use_partitioned_filter();
            }

            for item in items.iter().cloned() {
                writer.write(item.0).unwrap();
            }

            let _trailer = writer.finish().unwrap();
        }

        let table = lsm_tree::Table::recover(
            file,
            lsm_tree::Checksum::from_raw(0),
            0,
            0,
            Arc::new(lsm_tree::Cache::with_capacity_bytes(0)),
            Arc::new(lsm_tree::DescriptorTable::new(10)),
            filter_type == FilterType::Full,
            index_type == IndexType::Full,
        )
        .unwrap();

        assert_eq!(table.metadata.item_count as usize, items.len());

        assert_eq!(items.len(), table.metadata.item_count as usize);
        // assert_eq!(1, table.metadata.data_block_count);
        // assert_eq!(1, table.metadata.index_block_count); // 1 because we use a full index
        // assert!(
        //     table.regions.index.is_none(),
        //     "should use full index, so only TLI exists",
        // );
        // assert!(
        //     table.pinned_block_index_size() > 0,
        //     "should pin block index",
        // );
        // assert!(table.pinned_filter_size() > 0, "should pin filter");

        // assert_eq!(items, &*table.iter().flatten().collect::<Vec<_>>());
        // assert_eq!(
        //     items.iter().rev().cloned().collect::<Vec<_>>(),
        //     &*table.iter().rev().flatten().collect::<Vec<_>>(),
        // );

        let items = items.into_iter().map(|value| value.0).collect::<Vec<_>>();

        assert_eq!(items, table.iter().collect::<Result<Vec<_>, _>>().unwrap());
        assert_eq!(
            items.iter().rev().cloned().collect::<Vec<_>>(),
            table.iter().rev().collect::<Result<Vec<_>, _>>().unwrap(),
        );

        // for restart_interval in 1..=16 {

        // assert_eq!(data_block.len(), items.len());

        // if data_block.binary_index_len() > 254 {
        //     assert!(data_block.hash_bucket_count().is_none());
        // } else if hash_ratio > 0.0 {
        //     assert!(data_block.hash_bucket_count().unwrap() > 0);
        // }

        /*
        eprintln!("{items:?}"); */

        for needle in &items {
            if needle.key.seqno == SeqNo::MAX {
                continue;
            }

            // eprintln!("needle: {:?}", needle.key);

            let key_hash =
                lsm_tree::table::filter::standard_bloom::Builder::get_hash(&needle.key.user_key);

            assert_eq!(
                Some(needle.clone()),
                table
                    .get(&needle.key.user_key, needle.key.seqno + 1, key_hash)
                    .unwrap(),
            );

            assert_eq!(
                table
                    .get(&needle.key.user_key, u64::MAX, key_hash)
                    .unwrap()
                    .unwrap(),
                items
                    .iter()
                    .find(|item| item.key.user_key == needle.key.user_key
                        && item.key.seqno < u64::MAX)
                    .cloned()
                    .unwrap(),
            );
        }

        assert_eq!(
            items,
            table
                .scan()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );

        {
            let ping_pongs = generate_ping_pong_code(seed, items.len());

            // eprintln!("{ping_pongs:?}");

            let expected_ping_ponged_items = {
                let mut iter = items.iter();
                let mut v = vec![];

                for &x in &ping_pongs {
                    if x == 0 {
                        v.push(iter.next().cloned().unwrap());
                    } else {
                        v.push(iter.next_back().cloned().unwrap());
                    }
                }

                v
            };

            let real_ping_ponged_items = {
                let mut iter = table.iter();

                let mut v = vec![];

                for &x in &ping_pongs {
                    if x == 0 {
                        v.push(iter.next().unwrap().unwrap());
                    } else {
                        v.push(iter.next_back().unwrap().unwrap());
                    }
                }

                v
            };

            assert_eq!(expected_ping_ponged_items, real_ping_ponged_items);
        }

        {
            let ping_pongs = generate_ping_pong_code(seed, items.len());

            let expected_ping_ponged_items = {
                let mut iter = items.iter().rev();
                let mut v = vec![];

                for &x in &ping_pongs {
                    if x == 0 {
                        v.push(iter.next().cloned().unwrap());
                    } else {
                        v.push(iter.next_back().cloned().unwrap());
                    }
                }

                v
            };

            let real_ping_ponged_items = {
                let mut iter = table.iter().rev();

                let mut v = vec![];

                for &x in &ping_pongs {
                    if x == 0 {
                        v.push(iter.next().unwrap().unwrap());
                    } else {
                        v.push(iter.next_back().unwrap().unwrap());
                    }
                }

                v
            };

            assert_eq!(expected_ping_ponged_items, real_ping_ponged_items);
        }

        {
            use rand::SeedableRng;
            use rand::prelude::*;
            use rand_chacha::ChaCha8Rng;

            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut lo = rng.random_range(0..items.len());
            let mut hi = rng.random_range(0..items.len());

            if lo > hi {
                std::mem::swap(&mut lo, &mut hi);
            }

            // Expand `lo` backward to the first entry with the same key
            let lo_key = &items[lo].key.user_key;
            let lo = items
                .iter()
                .position(|it| &it.key.user_key == lo_key)
                .unwrap();

            // Expand `hi` forward to the last entry with the same key
            let hi_key = &items[hi].key.user_key;
            let hi = items
                .iter()
                .rposition(|it| &it.key.user_key == hi_key)
                .unwrap();

            let expected_range: Vec<_> = items[lo..=hi].iter().cloned().collect();

            let iter = table.range(lo_key..=hi_key);

            assert_eq!(expected_range, iter.collect::<Result<Vec<_>, _>>().unwrap(),);
        }
    });
}
