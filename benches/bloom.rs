use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::bloom::{BlockedBloomFilter, StandardBloomFilter};

fn standard_filter_construction(c: &mut Criterion) {
    let mut filter = StandardBloomFilter::with_fp_rate(1_000_000, 0.01);

    c.bench_function("bloom filter add key", |b| {
        b.iter(|| {
            let key = nanoid::nanoid!();
            filter.set_with_hash(StandardBloomFilter::get_hash(key.as_bytes()));
        });
    });
}

fn standard_filter_contains(c: &mut Criterion) {
    let keys = (0..100_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    for fpr in [0.01, 0.001, 0.0001, 0.00001] {
        let mut filter = StandardBloomFilter::with_fp_rate(100_000, fpr);

        for key in &keys {
            filter.set_with_hash(StandardBloomFilter::get_hash(key));
        }

        let mut rng = rand::rng();

        c.bench_function(
            &format!(
                "bloom filter contains key, true positive ({}%)",
                fpr * 100.0,
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::IndexedRandom;

                    let sample = keys.choose(&mut rng).unwrap();
                    let hash = StandardBloomFilter::get_hash(sample);
                    assert!(filter.contains_hash(hash));
                });
            },
        );
    }
}

fn blocked_filter_construction(c: &mut Criterion) {
    let mut filter = BlockedBloomFilter::with_fp_rate(1_000_000, 0.01);

    c.bench_function("bloom filter add key - blocked bloom filter", |b| {
        b.iter(|| {
            let key = nanoid::nanoid!();
            filter.set_with_hash(BlockedBloomFilter::get_hash(key.as_bytes()));
        });
    });
}

fn blocked_filter_contains(c: &mut Criterion) {
    let keys = (0..100_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    for fpr in [0.01, 0.001, 0.0001, 0.00001] {
        let mut filter = BlockedBloomFilter::with_fp_rate(100_000, fpr);

        for key in &keys {
            filter.set_with_hash(BlockedBloomFilter::get_hash(key));
        }

        let mut rng = rand::rng();

        c.bench_function(
            &format!(
                "bloom filter contains key, true positive ({}%) - blocked bloom filter",
                fpr * 100.0,
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::IndexedRandom;

                    let sample = keys.choose(&mut rng).unwrap();
                    let hash = BlockedBloomFilter::get_hash(sample);
                    assert!(filter.contains_hash(hash));
                });
            },
        );
    }
}

criterion_group!(
    benches,
    standard_filter_construction,
    standard_filter_contains,
    blocked_filter_construction,
    blocked_filter_contains,
);
criterion_main!(benches);
