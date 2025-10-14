#[macro_use]
extern crate afl;

use arbitrary::{Arbitrary, Result, Unstructured};
use lsm_tree::segment::{block::BlockOffset, Block, IndexBlock, KeyedBlockHandle};

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

        // eprintln!("restart_interval={restart_interval}");

        if let Ok(mut items) = <Vec<FuzzyValue> as Arbitrary>::arbitrary(&mut unstructured) {
            // let mut items = items.to_vec();

            if !items.is_empty() {
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

                // for restart_interval in 1..=u8::MAX {
                let bytes = IndexBlock::encode_into_vec(
                    &items,
                    // restart_interval.into(),
                )
                .unwrap();

                let index_block = IndexBlock::new(Block {
                    data: bytes.into(),
                    header: lsm_tree::segment::block::Header {
                        checksum: lsm_tree::segment::block::Checksum::from_raw(0),
                        data_length: 0,
                        uncompressed_length: 0,
                        block_type: lsm_tree::segment::block::BlockType::Index,
                    },
                });

                assert_eq!(index_block.len(), items.len());

                /*  if data_block.binary_index_len() > 254 {
                    assert!(data_block.hash_bucket_count().is_none());
                } else if hash_ratio > 0.0 {
                    assert!(data_block.hash_bucket_count().unwrap() > 0);
                } */

                // eprintln!("{items:?}");

                /*  for handle in &items {
                    // eprintln!("needle: {:?}", needle.key);

                    assert_eq!(
                        Some(needle.clone()),
                        data_block.point_read(&handle.end_key).unwrap(),
                    );
                } */

                /*  assert_eq!(
                    items,
                    data_block.iter().map(|x| x.unwrap()).collect::<Vec<_>>(),
                );

                assert_eq!(
                    items.iter().rev().cloned().collect::<Vec<_>>(),
                    data_block
                        .iter()
                        .rev()
                        .map(|x| x.unwrap())
                        .collect::<Vec<_>>(),
                ); */

                // TODO: add ping-pong iters

                // TODO: add range iter too
                // }
            }
        }
    });
}
