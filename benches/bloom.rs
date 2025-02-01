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
    for fpr in [0.01, 0.001, 0.0001, 0.00001] {
        let mut filter = BloomFilter::with_fp_rate(100_000, fpr);

        let keys: &[&[u8]] = &[
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ];

        for key in keys {
            filter.set_with_hash(BloomFilter::get_hash(key));
        }

        let mut rng = rand::thread_rng();

        c.bench_function(
            &format!(
                "bloom filter contains key, true positive ({}%)",
                fpr * 100.0
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::SliceRandom;

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
