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

        for key in [
            b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7",
            b"item8", b"item9",
        ] {
            filter.set_with_hash(BloomFilter::get_hash(key));

            assert!(!filter.contains(nanoid::nanoid!().as_bytes()));
        }

        c.bench_function(
            &format!(
                "bloom filter contains key, true positive ({}%)",
                fpr * 100.0
            ),
            |b| {
                b.iter(|| {
                    // We hash once and then do 4 runs of bloom filter lookups
                    // to simulate hash sharing (https://fjall-rs.github.io/post/bloom-filter-hash-sharing/)
                    //  and L0 having 4 segments + 1 check in L1
                    let hash = BloomFilter::get_hash(b"item4");

                    for _ in 0..5 {
                        assert!(filter.contains_hash(hash));
                    }
                });
            },
        );

        /*
        c.bench_function(
            &format!(
                "bloom filter contains key, true negative ({}%)",
                fpr * 100.0
            ),
            |b| {
                b.iter(|| filter.contains(b"sdfafdas"));
            },
        ); */
    }
}

criterion_group!(benches, filter_construction, filter_contains,);
criterion_main!(benches);
