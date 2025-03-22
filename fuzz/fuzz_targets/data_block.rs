#![no_main]
use libfuzzer_sys::{
    arbitrary::{Arbitrary, Result, Unstructured},
    fuzz_target,
};
use lsm_tree::{
    segment::block::offset::BlockOffset,
    super_segment::{Block, DataBlock},
    InternalValue, SeqNo, ValueType,
};

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
struct FuzzyValue(InternalValue);

impl<'a> Arbitrary<'a> for FuzzyValue {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let key = Vec::<u8>::arbitrary(u)?;
        let value = Vec::<u8>::arbitrary(u)?;
        let seqno = u64::arbitrary(u)?;

        let key = if key.is_empty() { vec![0] } else { key };

        Ok(Self(InternalValue::from_components(
            key,
            value,
            seqno,
            ValueType::Value,
        )))
    }
}

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);

    let restart_interval = u8::arbitrary(&mut unstructured).unwrap().max(1);
    let hash_ratio = (f32::arbitrary(&mut unstructured).unwrap() / f32::MAX)
        .min(1.0)
        .max(0.0);

    if let Ok(mut items) = <Vec<FuzzyValue> as Arbitrary>::arbitrary(&mut unstructured) {
        if !items.is_empty() {
            items.sort();
            items.dedup();

            let items = items.into_iter().map(|value| value.0).collect::<Vec<_>>();
            let bytes =
                DataBlock::encode_items(&items, restart_interval.into(), hash_ratio).unwrap();

            let data_block = DataBlock {
                inner: Block {
                    data: bytes.into(),
                    header: lsm_tree::segment::block::header::Header {
                        checksum: lsm_tree::segment::block::checksum::Checksum::from_raw(0),
                        compression: lsm_tree::CompressionType::None,
                        data_length: 0,
                        uncompressed_length: 0,
                        previous_block_offset: BlockOffset(0),
                    },
                },
            };

            for needle in items {
                if needle.key.seqno == SeqNo::MAX {
                    continue;
                }

                assert_eq!(
                    Some(needle.clone()),
                    data_block.point_read(&needle.key.user_key, Some(needle.key.seqno + 1)),
                );
            }
        }
    }
});
