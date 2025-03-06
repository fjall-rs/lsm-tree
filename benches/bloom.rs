use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::bloom::BloomFilter;

fn filter_construction(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(1_000_000, 0.01);

    c.bench_function("bloom filter add key", |b| {
        b.iter(|| {
            let key = nanoid::nanoid!();
            filter.set_with_hash(BloomFilter::get_hash(key.as_bytes()));
        });
    });
}

fn filter_contains(c: &mut Criterion) {
    let keys = (0..100_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    for fpr in [0.01, 0.001, 0.0001, 0.00001] {
        let mut filter = BloomFilter::with_fp_rate(100_000, fpr);

        for key in &keys {
            filter.set_with_hash(BloomFilter::get_hash(key));
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
                    let hash = BloomFilter::get_hash(sample);
                    assert!(filter.contains_hash(hash));
                });
            },
        );
    }
}

criterion_group!(benches, filter_construction, filter_contains,);
criterion_main!(benches);
