use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::segment::{
    block::offset::BlockOffset, block_index::KeyedBlockIndex, value_block::CachePolicy,
};
use rand::Rng;

fn tli_find_item(c: &mut Criterion) {
    use lsm_tree::segment::block_index::{
        block_handle::KeyedBlockHandle, top_level::TopLevelIndex,
    };

    let mut group = c.benchmark_group("TLI find item");

    for item_count in [10u64, 100, 1_000, 10_000, 25_000, 100_000] {
        let items = {
            let mut items = Vec::with_capacity(item_count as usize);

            for x in 0..item_count {
                items.push(KeyedBlockHandle {
                    end_key: x.to_be_bytes().into(),
                    offset: BlockOffset(x),
                });
            }

            items
        };

        let index = TopLevelIndex::from_boxed_slice(items.into());

        let mut rng = rand::rng();

        group.bench_function(
            format!("TLI get_block_containing_item ({item_count} items)"),
            |b| {
                b.iter(|| {
                    let needle = rng.random_range(0..item_count).to_be_bytes();

                    assert_eq!(
                        needle,
                        &*index
                            .get_lowest_block_containing_key(&needle, CachePolicy::Read)
                            .unwrap()
                            .unwrap()
                            .end_key,
                    );
                })
            },
        );
    }
}

criterion_group!(benches, tli_find_item);
criterion_main!(benches);
