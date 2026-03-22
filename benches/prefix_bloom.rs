use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{AbstractTree, Config, PrefixExtractor, SequenceNumberCounter};
use std::sync::Arc;

struct ColonSeparatedPrefix;

impl PrefixExtractor for ColonSeparatedPrefix {
    fn prefixes<'a>(&self, key: &'a [u8]) -> Box<dyn Iterator<Item = &'a [u8]> + 'a> {
        Box::new(
            key.iter()
                .enumerate()
                .filter(|(_, b)| **b == b':')
                .map(move |(i, _)| &key[..=i]),
        )
    }
}

fn setup_tree(
    path: &std::path::Path,
    with_prefix_bloom: bool,
    segment_count: u32,
    keys_per_segment: u32,
) -> lsm_tree::Tree {
    let mut config = Config::new(
        path,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );

    if with_prefix_bloom {
        config = config.prefix_extractor(Arc::new(ColonSeparatedPrefix));
    }

    let tree = match config.open().unwrap() {
        lsm_tree::AnyTree::Standard(t) => t,
        _ => panic!("expected standard tree"),
    };

    let mut seqno = 0u64;

    for seg in 0..segment_count {
        for i in 0..keys_per_segment {
            let key = format!("ns{seg}:sub{i}:key{i:04}");
            tree.insert(key, "value_data", seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0).unwrap();
    }

    // Compact to L1 for single-table runs (bloom skip applies here).
    // 64 KiB target produces multiple reasonably-sized tables so the
    // benchmark measures actual bloom-skip benefits across segments.
    tree.major_compact(64 * 1024, 0).unwrap();
    tree
}

fn prefix_scan_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefix scan");
    group.sample_size(20);

    for segment_count in [10, 50] {
        let keys_per_segment = 100;

        // With prefix bloom
        group.bench_function(
            format!("with prefix bloom, {segment_count} segments"),
            |b| {
                let path = tempfile::tempdir().unwrap();
                let tree = setup_tree(path.path(), true, segment_count, keys_per_segment);
                let seqno = (segment_count * keys_per_segment) as u64;

                b.iter(|| {
                    let results: Vec<_> = tree
                        .create_prefix("ns0:", seqno, None)
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_eq!(results.len(), keys_per_segment as usize);
                });
            },
        );

        // Without prefix bloom
        group.bench_function(
            format!("without prefix bloom, {segment_count} segments"),
            |b| {
                let path = tempfile::tempdir().unwrap();
                let tree = setup_tree(path.path(), false, segment_count, keys_per_segment);
                let seqno = (segment_count * keys_per_segment) as u64;

                b.iter(|| {
                    let results: Vec<_> = tree
                        .create_prefix("ns0:", seqno, None)
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    assert_eq!(results.len(), keys_per_segment as usize);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, prefix_scan_benchmark);
criterion_main!(benches);
