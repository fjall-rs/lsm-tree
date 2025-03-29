use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::binary_search::partition_point;

fn bench_partition_point(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_point");

    for item_count in [10, 100, 1_000, 10_000, 100_000, 1_000_000] {
        let items = (0..item_count).collect::<Vec<_>>();

        // TODO: replace search key with random integer

        group.bench_function(format!("native {item_count}"), |b| {
            b.iter(|| items.partition_point(|&x| x <= 5_000))
        });

        group.bench_function(format!("rewrite {item_count}"), |b| {
            b.iter(|| partition_point(&items, |&x| x <= 5_000))
        });
    }
}

criterion_group!(benches, bench_partition_point);
criterion_main!(benches);
