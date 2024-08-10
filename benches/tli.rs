use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::Slice;

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
                    offset: x,
                });
            }

            items
        };

        let index = TopLevelIndex::from_boxed_slice(items.into());

        group.bench_function(
            format!("TLI get_next_block_handle ({item_count} items)"),
            |b| {
                let key = (item_count / 10 * 6).to_be_bytes();
                let expected: Slice = (item_count / 10 * 6 + 1).to_be_bytes().into();

                let block = index.get_lowest_block_containing_key(&key).unwrap();

                b.iter(|| {
                    assert_eq!(
                        expected,
                        index.get_next_block_handle(block.offset).unwrap().end_key
                    );
                })
            },
        );

        group.bench_function(
            format!("TLI get_block_containing_item ({item_count} items)"),
            |b| {
                let key = (item_count / 10 * 6).to_be_bytes();

                b.iter(|| {
                    assert_eq!(
                        key,
                        &*index.get_lowest_block_containing_key(&key).unwrap().end_key
                    );
                })
            },
        );
    }
}

criterion_group!(benches, tli_find_item,);
criterion_main!(benches);
