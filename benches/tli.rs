use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{
    table::{
        block::{decoder::ParsedItem, BlockType, Header},
        Block, BlockHandle, BlockOffset, IndexBlock, KeyedBlockHandle,
    },
    Checksum,
};
use rand::Rng;

fn tli_find_item(c: &mut Criterion) {
    let mut group = c.benchmark_group("TLI find item");

    for item_count in [10u64, 100, 1_000, 10_000, 25_000, 100_000] {
        let items = (0..item_count)
            .map(|x| {
                KeyedBlockHandle::new(
                    x.to_be_bytes().into(),
                    0,
                    BlockHandle::new(BlockOffset(x), 0),
                )
            })
            .collect::<Vec<_>>();

        let bytes = IndexBlock::encode_into_vec(&items).unwrap();
        let index = IndexBlock::new(Block {
            data: bytes.into(),
            header: Header {
                block_type: BlockType::Index,
                checksum: Checksum::from_raw(0),
                data_length: 0,
                uncompressed_length: 0,
            },
        });

        let mut rng = rand::rng();

        group.bench_function(
            format!("TLI get_block_containing_item ({item_count} items)"),
            |b| {
                b.iter(|| {
                    let needle = rng.random_range(0..item_count).to_be_bytes();

                    let mut iter = index.iter();
                    assert!(iter.seek(&needle, 0));
                    let item = iter
                        .next()
                        .expect("should exist")
                        .materialize(&index.inner.data);
                    assert_eq!(&needle[..], item.end_key().as_ref());
                })
            },
        );
    }
}

criterion_group!(benches, tli_find_item);
criterion_main!(benches);
