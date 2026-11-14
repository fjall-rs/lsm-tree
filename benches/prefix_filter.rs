use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use lsm_tree::{
    config::{BloomConstructionPolicy, FilterPolicy, FilterPolicyEntry},
    prefix::FixedPrefixExtractor,
    AbstractTree, AnyTree, Config, SeqNo, SequenceNumberCounter,
};
use std::{hint::black_box, sync::Arc};

/// Keys are `p{prefix:05}_{n:05}`, so each has the 6-byte prefix `p{prefix:05}`
fn key(prefix: usize, n: usize) -> String {
    format!("p{prefix:05}_{n:05}")
}

#[derive(Clone, Copy)]
enum Filter {
    WholeKey,
    Prefix { whole_key_filtering: bool },
}

impl Filter {
    fn name(self) -> &'static str {
        match self {
            Self::WholeKey => "whole key",
            Self::Prefix {
                whole_key_filtering: true,
            } => "prefix + whole key",
            Self::Prefix {
                whole_key_filtering: false,
            } => "prefix only",
        }
    }
}

fn open(dir: &std::path::Path, filter: Filter) -> AnyTree {
    let mut config = Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .filter_policy(FilterPolicy::all(FilterPolicyEntry::Bloom(
        BloomConstructionPolicy::BitsPerKey(10.0),
    )));

    if let Filter::Prefix {
        whole_key_filtering,
    } = filter
    {
        config = config
            .prefix_extractor(Arc::new(FixedPrefixExtractor::new(6)))
            .whole_key_filtering(whole_key_filtering);
    }

    config.open().unwrap()
}

/// `tables` L0 tables that each hold every even prefix, so every table's key
/// range covers every odd prefix and only a filter can rule a table out.
fn overlapping_tables(dir: &std::path::Path, filter: Filter, tables: usize) -> AnyTree {
    let tree = open(dir, filter);

    for table in 0..tables {
        for prefix in (0..200).step_by(2) {
            for n in 0..10 {
                tree.insert(key(prefix, table * 10 + n), "v", 0);
            }
        }
        tree.flush_active_memtable(0).unwrap();
    }

    assert_eq!(tables, tree.table_count());
    tree
}

fn absent_prefix_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix scan, absent prefix, 32 overlapping tables");

    for filter in [
        Filter::WholeKey,
        Filter::Prefix {
            whole_key_filtering: false,
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let tree = overlapping_tables(dir.path(), filter, 32);

        group.bench_function(filter.name(), |b| {
            b.iter(|| {
                assert_eq!(0, tree.prefix("p00101", SeqNo::MAX, None).count());
            });
        });
    }

    group.finish();
}

fn present_prefix_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix scan, present prefix, 32 overlapping tables");

    for filter in [
        Filter::WholeKey,
        Filter::Prefix {
            whole_key_filtering: false,
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let tree = overlapping_tables(dir.path(), filter, 32);

        group.bench_function(filter.name(), |b| {
            b.iter(|| {
                assert_eq!(320, tree.prefix("p00100", SeqNo::MAX, None).count());
            });
        });
    }

    group.finish();
}

fn absent_point_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("point read, absent key, 32 overlapping tables");

    for filter in [
        Filter::WholeKey,
        Filter::Prefix {
            whole_key_filtering: true,
        },
        Filter::Prefix {
            whole_key_filtering: false,
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let tree = overlapping_tables(dir.path(), filter, 32);

        // In an absent prefix, and in a present prefix
        for (case, key) in [
            ("absent prefix", key(101, 0)),
            ("present prefix", key(100, 9_999)),
        ] {
            group.bench_function(format!("{}, {case}", filter.name()), |b| {
                b.iter(|| {
                    assert!(tree.get(black_box(&key), SeqNo::MAX).unwrap().is_none());
                });
            });
        }
    }

    group.finish();
}

fn flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush 100k keys, 10 per prefix");
    group.sample_size(20);

    for filter in [
        Filter::WholeKey,
        Filter::Prefix {
            whole_key_filtering: true,
        },
        Filter::Prefix {
            whole_key_filtering: false,
        },
    ] {
        group.bench_function(filter.name(), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let tree = open(dir.path(), filter);
                    for prefix in 0..10_000 {
                        for n in 0..10 {
                            tree.insert(key(prefix, n), "v", 0);
                        }
                    }
                    (dir, tree)
                },
                |(dir, tree)| {
                    tree.flush_active_memtable(0).unwrap();
                    (dir, tree)
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    absent_prefix_scan,
    present_prefix_scan,
    absent_point_read,
    flush
);
criterion_main!(benches);
