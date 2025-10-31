use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, RngCore};

// Not really worth it anymore on new CPUs...?
fn fast_block_index(c: &mut Criterion) {
    pub fn fast_impl(h: u64, num_blocks: usize) -> usize {
        // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
        (((h >> 32).wrapping_mul(num_blocks as u64)) >> 32) as usize
    }

    let mut rng = rand::rng();
    let num_blocks = 100_000;

    c.bench_function("block index - mod", |b| {
        b.iter(|| {
            let h: u64 = rng.random();
            criterion::black_box(h % (num_blocks as u64))
        });
    });

    c.bench_function("block index - fast", |b| {
        b.iter(|| {
            let h: u64 = rng.random();
            criterion::black_box(fast_impl(h, num_blocks))
        });
    });
}

fn standard_filter_construction(c: &mut Criterion) {
    use lsm_tree::table::filter::standard_bloom::Builder;

    let mut rng = rand::rng();

    c.bench_function("standard bloom filter add key, 1M", |b| {
        let mut filter = Builder::with_fp_rate(1_000_000, 0.01);

        b.iter(|| {
            let mut key = [0; 16];
            rng.fill_bytes(&mut key);

            filter.set_with_hash(Builder::get_hash(&key));
        });
    });

    c.bench_function("standard bloom filter add key, 10M", |b| {
        let mut filter = Builder::with_fp_rate(10_000_000, 0.01);

        b.iter(|| {
            let mut key = [0; 16];
            rng.fill_bytes(&mut key);

            filter.set_with_hash(Builder::get_hash(&key));
        });
    });
}

fn blocked_filter_construction(c: &mut Criterion) {
    use lsm_tree::table::filter::blocked_bloom::Builder;

    let mut rng = rand::rng();

    c.bench_function("blocked bloom filter add key, 1M", |b| {
        let mut filter = Builder::with_fp_rate(1_000_000, 0.01);

        b.iter(|| {
            let mut key = [0; 16];
            rng.fill_bytes(&mut key);

            filter.set_with_hash(Builder::get_hash(&key));
        });
    });

    c.bench_function("blocked bloom filter add key, 10M", |b| {
        let mut filter = Builder::with_fp_rate(10_000_000, 0.01);

        b.iter(|| {
            let mut key = [0; 16];
            rng.fill_bytes(&mut key);

            filter.set_with_hash(Builder::get_hash(&key));
        });
    });
}

fn standard_filter_contains(c: &mut Criterion) {
    use lsm_tree::table::filter::standard_bloom::Builder;

    let keys = (0..100_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    for fpr in [0.1, 0.01, 0.001, 0.0001, 0.00001] {
        // NOTE: Purposefully bloat bloom filter size to run into more CPU cache misses
        let n = 100_000_000;

        let mut filter = Builder::with_fp_rate(n, fpr);

        for key in &keys {
            filter.set_with_hash(Builder::get_hash(key));
        }

        let mut rng = rand::rng();

        let filter_bytes = filter.build();

        c.bench_function(
            &format!(
                "standard bloom filter contains key, true positive ({}%)",
                fpr * 100.0,
            ),
            |b| {
                b.iter(|| {
                    use rand::seq::IndexedRandom;
                    use lsm_tree::table::filter::standard_bloom::StandardBloomFilterReader as Reader;

                    // NOTE: To make the costs more realistic, we
                    // pretend we are reading the filter straight from the block
                    let filter = Reader::new(&filter_bytes).unwrap();

                    let sample = keys.choose(&mut rng).unwrap();
                    let hash = Builder::get_hash(sample);
                    assert!(filter.contains_hash(hash));
                });
            },
        );
    }
}

fn blocked_filter_contains(c: &mut Criterion) {
    use lsm_tree::table::filter::blocked_bloom::Builder;

    let keys = (0..100_000u128)
        .map(|x| x.to_be_bytes().to_vec())
        .collect::<Vec<_>>();

    for fpr in [0.1, 0.01, 0.001, 0.0001, 0.00001] {
        // NOTE: Purposefully bloat bloom filter size to run into more CPU cache misses
        let n = 100_000_000;

        let mut filter = Builder::with_fp_rate(n, fpr);

        for key in &keys {
            filter.set_with_hash(Builder::get_hash(key));
        }

        let mut rng = rand::rng();

        let filter_bytes = filter.build();

        c.bench_function(
            &format!(
                "blocked bloom filter contains key, true positive ({}%)",
                fpr * 100.0,
            ),
            |b| {
                b.iter(|| {
                    use lsm_tree::table::filter::blocked_bloom::BlockedBloomFilterReader as Reader;
                    use rand::seq::IndexedRandom;

                    // NOTE: To make the costs more realistic, we
                    // pretend we are reading the filter straight from the block
                    let filter = Reader::new(&filter_bytes).unwrap();

                    let sample = keys.choose(&mut rng).unwrap();
                    let hash = Builder::get_hash(sample);
                    assert!(filter.contains_hash(hash));
                });
            },
        );
    }
}

criterion_group!(
    benches,
    fast_block_index,
    standard_filter_construction,
    blocked_filter_construction,
    standard_filter_contains,
    blocked_filter_contains,
);
criterion_main!(benches);
