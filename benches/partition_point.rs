use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;

fn partition_point<T, F>(slice: &[T], mut pred: F) -> usize
where
    F: FnMut(&T) -> bool,
{
    let mut left = 0;
    let mut right = slice.len();

    while left < right {
        let mid = left + (right - left) / 2;
        if pred(&slice[mid]) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    left
}

fn bench_partition_point(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_point");

    let mut rng = rand::rng();

    for item_count in [10, 100, 1_000, 10_000, 100_000, 1_000_000] {
        let items = (0..item_count).collect::<Vec<_>>();

        group.bench_function(format!("native {item_count}"), |b| {
            b.iter(|| {
                let needle = rng.random_range(0..item_count);
                items.partition_point(|&x| x <= needle)
            })
        });

        group.bench_function(format!("rewrite {item_count}"), |b| {
            b.iter(|| {
                let needle = rng.random_range(0..item_count);
                partition_point(&items, |&x| x <= needle)
            })
        });
    }
}

criterion_group!(benches, bench_partition_point);
criterion_main!(benches);
