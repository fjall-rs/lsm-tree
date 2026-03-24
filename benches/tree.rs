use criterion::{criterion_group, criterion_main, Criterion};
// Guard trait import required for .into_inner() on IterGuardImpl
use lsm_tree::{AbstractTree, Cache, Config, Guard, SeqNo, SequenceNumberCounter};
use std::sync::Arc;
use tempfile::tempdir;

const MAX: SeqNo = SeqNo::MAX;

fn full_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan all");
    group.sample_size(10);

    for item_count in [10_000, 100_000] {
        group.bench_function(format!("scan all uncached, {item_count} items"), |b| {
            let path = tempdir().unwrap();

            let tree = Config::new(
                &path,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
            .open()
            .unwrap();

            for x in 0_u32..item_count {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value, 0);
            }

            tree.flush_active_memtable(0).unwrap();

            b.iter(|| {
                assert_eq!(tree.len(MAX, None).unwrap(), item_count as usize);
            })
        });

        group.bench_function(format!("scan all cached, {item_count} items"), |b| {
            let path = tempdir().unwrap();

            let tree = Config::new(
                &path,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .use_cache(Arc::new(Cache::with_capacity_bytes(100_000_000)))
            .open()
            .unwrap();

            for x in 0_u32..item_count {
                let key = x.to_be_bytes();
                let value = nanoid::nanoid!();
                tree.insert(key, value, 0);
            }

            tree.flush_active_memtable(0).unwrap();
            assert_eq!(tree.len(MAX, None).unwrap(), item_count as usize);

            b.iter(|| {
                assert_eq!(tree.len(MAX, None).unwrap(), item_count as usize);
            })
        });
    }
}

fn scan_vs_query(c: &mut Criterion) {
    use std::ops::Bound::*;

    let mut group = c.benchmark_group("scan vs query");

    for size in [10_000, 100_000] {
        // Query window must fit within the key space [0, size).
        let query_end = size as u64;
        let query_start = query_end - 10;

        let path = tempdir().unwrap();

        let tree = Config::new(
            &path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
        .open()
        .unwrap();

        for x in 0..size as u64 {
            let key = x.to_be_bytes().to_vec();
            let value = nanoid::nanoid!().as_bytes().to_vec();
            tree.insert(key, value, 0);
        }

        tree.flush_active_memtable(0).unwrap();
        assert_eq!(tree.len(MAX, None).unwrap(), size);

        group.sample_size(10);
        group.bench_function(format!("scan {} (uncached)", size), |b| {
            b.iter(|| {
                let count = tree
                    .iter(MAX, None)
                    .map(|guard| guard.into_inner().unwrap())
                    .filter(|(key, _)| {
                        let buf = &key[..8];
                        let (int_bytes, _rest) = buf.split_at(std::mem::size_of::<u64>());
                        let num = u64::from_be_bytes(int_bytes.try_into().unwrap());
                        (query_start..query_end).contains(&num)
                    })
                    .count();
                assert_eq!(count, 10);
            })
        });
        group.bench_function(format!("query {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.range(
                    (
                        Included(query_start.to_be_bytes().to_vec()),
                        Excluded(query_end.to_be_bytes().to_vec()),
                    ),
                    MAX,
                    None,
                );
                assert_eq!(iter.count(), 10);
            })
        });
        group.bench_function(format!("query rev {}", size), |b| {
            b.iter(|| {
                let iter = tree.range(
                    (
                        Included(query_start.to_be_bytes().to_vec()),
                        Excluded(query_end.to_be_bytes().to_vec()),
                    ),
                    MAX,
                    None,
                );
                assert_eq!(iter.rev().count(), 10);
            })
        });
    }
}

fn scan_vs_prefix(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan vs prefix");

    for size in [10_000, 100_000] {
        let path = tempdir().unwrap();

        let tree = Config::new(
            &path,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
        .open()
        .unwrap();

        for _ in 0..size {
            let key = nanoid::nanoid!();
            let value = nanoid::nanoid!();
            tree.insert(key, value, 0);
        }

        let prefix = "hello$$$";

        for _ in 0..10_u64 {
            let key = format!("{}:{}", prefix, nanoid::nanoid!());
            let value = nanoid::nanoid!();
            tree.insert(key, value, 0);
        }

        tree.flush_active_memtable(0).unwrap();
        assert_eq!(tree.len(MAX, None).unwrap() as u64, size + 10);

        group.sample_size(10);
        group.bench_function(format!("scan {} (uncached)", size), |b| {
            b.iter(|| {
                let count = tree
                    .iter(MAX, None)
                    .map(|guard| guard.into_inner().unwrap())
                    .filter(|(key, _)| key.starts_with(prefix.as_bytes()))
                    .count();
                assert_eq!(count, 10);
            });
        });
        group.bench_function(format!("prefix {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.prefix(prefix, MAX, None);
                assert_eq!(iter.count(), 10);
            });
        });
        group.bench_function(format!("prefix rev {} (uncached)", size), |b| {
            b.iter(|| {
                let iter = tree.prefix(prefix, MAX, None);
                assert_eq!(iter.rev().count(), 10);
            });
        });
    }
}

fn tree_get_pairs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Get pairs");
    group.sample_size(10);

    for segment_count in [1, 4, 16, 64, 128] {
        {
            let folder = tempfile::tempdir().unwrap();
            let tree = Config::new(
                &folder,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
            .open()
            .unwrap();

            let mut x = 0_u64;

            for _ in 0..segment_count {
                for _ in 0..10 {
                    let key = x.to_be_bytes();
                    x += 1;
                    tree.insert(key, key, 0);
                }
                tree.flush_active_memtable(0).unwrap();
            }

            group.bench_function(
                &format!("Tree::first_key_value (disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        let guard = tree.first_key_value(MAX, None).unwrap();
                        assert!(guard.into_inner().is_ok());
                    });
                },
            );

            group.bench_function(
                &format!("Tree::last_key_value (disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        let guard = tree.last_key_value(MAX, None).unwrap();
                        assert!(guard.into_inner().is_ok());
                    });
                },
            );
        }

        {
            let folder = tempfile::tempdir().unwrap();
            let tree = Config::new(
                &folder,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
            .open()
            .unwrap();

            let mut x = 0_u64;

            for _ in 0..segment_count {
                for _ in 0..10 {
                    let key = x.to_be_bytes();
                    x += 1;
                    tree.insert(key, key, 0);
                }
                tree.insert("a", vec![], 0);
                tree.insert(u64::MAX.to_be_bytes(), vec![], 0);
                tree.flush_active_memtable(0).unwrap();
            }

            group.bench_function(
                &format!("Tree::first_key_value (non-disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        let guard = tree.first_key_value(MAX, None).unwrap();
                        assert!(guard.into_inner().is_ok());
                    });
                },
            );

            group.bench_function(
                &format!("Tree::last_key_value (non-disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        let guard = tree.last_key_value(MAX, None).unwrap();
                        assert!(guard.into_inner().is_ok());
                    });
                },
            );
        }
    }
}

fn disk_point_read(c: &mut Criterion) {
    let folder = tempdir().unwrap();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
    .open()
    .unwrap();

    for seqno in 0..5 {
        tree.insert("a", "b", seqno);
    }
    tree.flush_active_memtable(0).unwrap();

    for seqno in 5..10 {
        tree.insert("a", "b", seqno);
    }
    tree.flush_active_memtable(0).unwrap();

    c.bench_function("point read latest (uncached)", |b| {
        let tree = tree.clone();

        b.iter(|| {
            tree.get("a", MAX).unwrap().unwrap();
        });
    });

    // TODO: snapshot() API was removed — re-enable when snapshot
    // benchmarking is possible through the public API.
}

fn disjoint_tree_minmax(c: &mut Criterion) {
    let mut group = c.benchmark_group("Disjoint tree");

    let folder = tempfile::tempdir().unwrap();

    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .use_cache(Arc::new(Cache::with_capacity_bytes(0)))
    .open()
    .unwrap();

    tree.insert("a", "a", 0);
    tree.flush_active_memtable(0).unwrap();
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 6)), 0)
        .unwrap();

    tree.insert("b", "b", 0);
    tree.flush_active_memtable(0).unwrap();
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 5)), 0)
        .unwrap();

    tree.insert("c", "c", 0);
    tree.flush_active_memtable(0).unwrap();
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 4)), 0)
        .unwrap();

    tree.insert("d", "d", 0);
    tree.flush_active_memtable(0).unwrap();
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 3)), 0)
        .unwrap();

    tree.insert("e", "e", 0);
    tree.flush_active_memtable(0).unwrap();
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 2)), 0)
        .unwrap();

    tree.insert("f", "f", 0);
    tree.flush_active_memtable(0).unwrap();
    tree.compact(Arc::new(lsm_tree::compaction::PullDown(0, 1)), 0)
        .unwrap();

    tree.insert("g", "g", 0);
    tree.flush_active_memtable(0).unwrap();

    group.bench_function("Tree::first_key_value".to_string(), |b| {
        b.iter(|| {
            let (_, v) = tree
                .first_key_value(MAX, None)
                .unwrap()
                .into_inner()
                .unwrap();
            assert_eq!(&*v, b"a");
        });
    });

    group.bench_function("Tree::last_key_value".to_string(), |b| {
        b.iter(|| {
            let (_, v) = tree
                .last_key_value(MAX, None)
                .unwrap()
                .into_inner()
                .unwrap();
            assert_eq!(&*v, b"g");
        });
    });
}

// TODO: blob_tree_get — open_as_blob_tree() was removed from Config.
// BlobTree is now created via kv_separation_opts. Rewrite needed.

// TODO: benchmark point read disjoint vs non-disjoint level vs disjoint *tree*
// TODO: benchmark .prefix().next() and .next_back(), disjoint and non-disjoint

criterion_group!(
    benches,
    disjoint_tree_minmax,
    disk_point_read,
    full_scan,
    scan_vs_query,
    scan_vs_prefix,
    tree_get_pairs,
);
criterion_main!(benches);
