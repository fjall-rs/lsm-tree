use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::bloom::BloomFilter;

fn filter_construction(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(1_000_000, 0.001);

    c.bench_function("bloom filter add key", |b| {
        b.iter(|| {
            let key = nanoid::nanoid!();
            filter.set_with_hash(BloomFilter::get_hash(key.as_bytes()));
        });
    });
}

fn filter_contains(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

    for key in [
        b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7", b"item8",
        b"item9",
    ] {
        filter.set_with_hash(BloomFilter::get_hash(key));

        assert!(!filter.contains(nanoid::nanoid!().as_bytes()));
    }

    c.bench_function("bloom filter contains key, true positive", |b| {
        b.iter(|| filter.contains(b"item4"));
    });

    c.bench_function("bloom filter contains key, true negative", |b| {
        b.iter(|| filter.contains(b"sdfafdas"));
    });
}

criterion_group!(benches, filter_construction, filter_contains,);
criterion_main!(benches);
