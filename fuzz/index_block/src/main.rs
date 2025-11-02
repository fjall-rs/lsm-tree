#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Result, Unstructured};
use lsm_tree::table::{
    block::decoder::ParsedItem, block::BlockOffset, Block, IndexBlock, KeyedBlockHandle,
};

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
struct FuzzyValue(KeyedBlockHandle);

impl<'a> Arbitrary<'a> for FuzzyValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let key = Vec::<u8>::arbitrary(u)?;

        let key = if key.is_empty() { vec![0] } else { key };

        Ok(Self(KeyedBlockHandle::new(
            key.into(),
            BlockOffset(0),
            u32::arbitrary(u)?,
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

fn main() {
    fuzz!(|data: &[u8]| {
        let mut unstructured = Unstructured::new(data);

        let seed = u64::arbitrary(&mut unstructured).unwrap();

        if let Ok(mut items) = <Vec<FuzzyValue> as Arbitrary>::arbitrary(&mut unstructured) {
            if !items.is_empty() {
                items.sort();
                items.dedup();

                let items = items.into_iter().map(|value| value.0).collect::<Vec<_>>();

                let bytes = IndexBlock::encode_into_vec(&items).unwrap();

                let index_block = IndexBlock::new(Block {
                    data: bytes.into(),
                    header: lsm_tree::table::block::Header {
                        checksum: lsm_tree::table::block::Checksum::from_raw(0),
                        data_length: 0,
                        uncompressed_length: 0,
                        block_type: lsm_tree::table::block::BlockType::Index,
                    },
                });

                assert_eq!(index_block.len(), items.len());

                assert_eq!(
                    items,
                    index_block
                        .iter()
                        .map(|x| x.materialize(index_block.as_slice()))
                        .collect::<Vec<_>>()
                );

                assert_eq!(
                    items.iter().rev().cloned().collect::<Vec<_>>(),
                    index_block
                        .iter()
                        .map(|x| x.materialize(index_block.as_slice()))
                        .rev()
                        .collect::<Vec<_>>(),
                );

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
                        let mut iter = index_block
                            .iter()
                            .rev()
                            .map(|x| x.materialize(index_block.as_slice()));

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

                    // NOTE: If there is A:1, A:2, B:1
                    // And we select lo as A:2
                    // Our data block will seek to A:1 (correct)
                    // But our model won't...
                    // So seek to the first occurence of a key
                    loop {
                        if lo == 0 {
                            break;
                        }

                        if items[lo - 1].end_key() == items[lo].end_key() {
                            lo -= 1;
                        } else {
                            break;
                        }
                    }

                    // NOTE: Similar to lo
                    loop {
                        if hi == items.len() - 1 {
                            break;
                        }

                        if items[hi + 1].end_key() == items[hi].end_key() {
                            hi += 1;
                        } else {
                            break;
                        }
                    }

                    let lo_key = &items[lo].end_key();
                    let hi_key = &items[hi].end_key();

                    let expected_range: Vec<_> = items[lo..=hi].iter().cloned().collect();

                    let mut iter = index_block.iter();
                    assert!(iter.seek(&lo_key), "should seek");
                    assert!(iter.seek_upper(hi_key), "should seek");

                    assert_eq!(
                        expected_range,
                        iter.map(|x| x.materialize(index_block.as_slice()))
                            .collect::<Vec<_>>(),
                    );
                }
            }
        }
    });
}
