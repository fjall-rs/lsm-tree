#![no_main]
use arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use lsm_tree::{
    segment::{block::BlockOffset, Block, DataBlock},
    InternalValue, SeqNo, ValueType,
};

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

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
struct FuzzyValue(InternalValue);

impl<'a> Arbitrary<'a> for FuzzyValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let key = Vec::<u8>::arbitrary(u)?;
        let value = Vec::<u8>::arbitrary(u)?;
        let seqno = u64::arbitrary(u)?;
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
    use rand::prelude::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    (0..len).map(|_| rng.random_range(0..=1)).collect()
}

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);

    let restart_interval = u8::arbitrary(&mut unstructured).unwrap().max(1);

    let seed = u64::arbitrary(&mut unstructured).unwrap();

    // eprintln!("restart_interval={restart_interval}, hash_ratio={hash_ratio}");

    let item_count = {
        use rand::prelude::*;
        use rand::SeedableRng;
        use rand_chacha::ChaCha8Rng;

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        rng.random_range(1..1_000)
    };

    let hash_ratio = {
        use rand::prelude::*;
        use rand::SeedableRng;
        use rand_chacha::ChaCha8Rng;

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        rng.random_range(0.0..4.0)
    };

    let mut items = (0..item_count)
        .map(|_| FuzzyValue::arbitrary(&mut unstructured).unwrap())
        .collect::<Vec<_>>();

    assert!(!items.is_empty());

    items.sort();
    items.dedup();

    /*  eprintln!("-- items --");
    for item in items.iter().map(|value| &value.0) {
        eprintln!(
            r#"InternalValue::from_components({:?}, {:?}, {}, {:?}),"#,
            item.key.user_key, item.value, item.key.seqno, item.key.value_type,
        );
    } */

    /* if items.len() > 100 {
        eprintln!("================== {}. ", items.len());
    } */

    let items = items.into_iter().map(|value| value.0).collect::<Vec<_>>();

    for restart_interval in 1..=u8::MAX {
        let bytes = DataBlock::encode_items(&items, restart_interval.into(), hash_ratio).unwrap();

        let data_block = DataBlock::new(Block {
        data: bytes.into(),
        header: lsm_tree::segment::block::Header {
            checksum: lsm_tree::segment::Checksum::from_raw(0),
            data_length: 0,
            uncompressed_length: 0,
            previous_block_offset: BlockOffset(0),
        },
    });

    assert_eq!(data_block.len(), items.len());
    assert!(!data_block.is_empty());

    if data_block.binary_index_len() > 254 {
        assert!(data_block.hash_bucket_count().is_none());
    } else if hash_ratio > 0.0 {
        assert!(data_block.hash_bucket_count().unwrap() > 0);
    }

    // eprintln!("{items:?}");

    for needle in &items {
        if needle.key.seqno == SeqNo::MAX {
            continue;
        }

        // eprintln!("needle: {:?}", needle.key);

        assert_eq!(
            Some(needle.clone()),
            data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
        );

        assert_eq!(
            data_block.point_read(&needle.key.user_key, None).unwrap(),
            items
                .iter()
                .find(|item| item.key.user_key == needle.key.user_key)
                .cloned()
                .unwrap(),
        );
    }

    assert_eq!(items, data_block.iter().collect::<Vec<_>>());

    assert_eq!(
        items.iter().rev().cloned().collect::<Vec<_>>(),
        data_block.iter().rev().collect::<Vec<_>>(),
    );

    {
        let ping_pongs = generate_ping_pong_code(seed, items.len());

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
            let mut iter = data_block.iter();
            let mut v = vec![];

            for &x in &ping_pongs {
                if x == 0 {
                    v.push(iter.next().unwrap());
                } else {
                    v.push(iter.next_back().unwrap());
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
            let mut iter = data_block.iter().rev();
            let mut v = vec![];

            for &x in &ping_pongs {
                if x == 0 {
                    v.push(iter.next().unwrap());
                } else {
                    v.push(iter.next_back().unwrap());
                }
            }

            v
        };

        assert_eq!(expected_ping_ponged_items, real_ping_ponged_items);
    }

    {
        use rand::prelude::*;
        use rand::SeedableRng;
        use rand_chacha::ChaCha8Rng;

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut lo = rng.random_range(0..items.len());
        let mut hi = rng.random_range(0..items.len());

        if lo > hi {
            std::mem::swap(&mut lo, &mut hi);
        }

        let lo_key = &items[lo].key.user_key;
        let hi_key = &items[hi].key.user_key;

        let expected_range: Vec<_> = items
            .iter()
            .filter(|kv| kv.key.user_key >= lo_key && kv.key.user_key <= hi_key)
            .cloned()
            .collect();

        assert_eq!(
            expected_range,
            data_block
                    .range::<&[u8], _>(&(lo_key.as_ref()..=hi_key.as_ref()))
                    .collect::<Vec<_>>(),
            );
        }
    }
});
