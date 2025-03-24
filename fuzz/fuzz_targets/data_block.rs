#![no_main]
use arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use lsm_tree::{
    segment::block::offset::BlockOffset,
    super_segment::{Block, DataBlock},
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

fuzz_target!(|data: &[u8]| {
    let mut unstructured = Unstructured::new(data);

    let restart_interval = u8::arbitrary(&mut unstructured).unwrap().max(1);

    let hash_ratio = ((u16::arbitrary(&mut unstructured).unwrap() / u16::MAX) as f32)
        .min(1.0)
        .max(0.0);

    // eprintln!("restart_interval={restart_interval}, hash_ratio={hash_ratio}");

    if let Ok(mut items) = <Vec<FuzzyValue> as Arbitrary>::arbitrary(&mut unstructured) {
        // let mut items = items.to_vec();

        if !items.is_empty() {
            items.sort();
            items.dedup();

            /* eprintln!("-- items --");
            for item in items.iter().map(|value| &value.0) {
                eprintln!(
                    r#"InternalValue::from_components({:?}, {:?}, {}, {:?}),"#,
                    item.key.user_key, item.value, item.key.seqno, item.key.value_type,
                );
            } */

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

            // eprintln!("{items:?}");

            for needle in items {
                if needle.key.seqno == SeqNo::MAX {
                    continue;
                }

                // eprintln!("needle: {:?}", needle.key);

                assert_eq!(
                    Some(needle.clone()),
                    data_block
                        .point_read(&needle.key.user_key, Some(needle.key.seqno + 1))
                        .unwrap(),
                );
            }
        }
    }
});
