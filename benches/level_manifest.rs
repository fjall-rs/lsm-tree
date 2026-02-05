use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{config::BlockSizePolicy, AbstractTree, Config, SequenceNumberCounter};

type StdConfig = Config<lsm_tree::StdFileSystem>;

fn iterate_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("Iterate level manifest");
    group.sample_size(10);

    std::fs::create_dir_all(".bench").unwrap();

    for segment_count in [0, 1, 5, 10, 100, 500, 1_000, 2_000, 4_000] {
        group.bench_function(format!("iterate {segment_count} segments"), |b| {
            let folder = tempfile::tempdir_in(".bench").unwrap();
            let tree = StdConfig::new(
                folder,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .data_block_size_policy(BlockSizePolicy::all(1_024))
            .open()
            .unwrap();

            for x in 0_u64..segment_count {
                tree.insert("a", "b", x);
                tree.flush_active_memtable(0).unwrap();
            }

            b.iter(|| {
                let run_count = tree.current_version().l0().run_count();
                assert_eq!(run_count, segment_count as usize);
            });
        });
    }
}

fn find_segment(c: &mut Criterion) {
    let mut group = c.benchmark_group("Find segment in disjoint level");
    group.sample_size(10);

    std::fs::create_dir_all(".bench").unwrap();

    for segment_count in [1u16, 2, 3, 4, 5, 10, 100, 1_000] {
        let folder = tempfile::tempdir_in(".bench").unwrap();
        let tree = StdConfig::new(
            folder,
            SequenceNumberCounter::default(),
            SequenceNumberCounter::default(),
        )
        .data_block_size_policy(BlockSizePolicy::all(1_024))
        .open()
        .unwrap();

        for x in 0..segment_count {
            tree.insert(x.to_be_bytes(), "", x.into());
            tree.flush_active_memtable(0).unwrap();
        }

        let key = (segment_count / 2).to_be_bytes();

        group.bench_function(
            format!("find segment in {segment_count} segments - binary search"),
            |b| {
                let version = tree.current_version();
                let level = version.level(0).expect("level should exist");
                let mut tables = level
                    .iter()
                    .flat_map(|run| run.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                tables.sort_by(|a, b| a.metadata.key_range.min().cmp(b.metadata.key_range.min()));

                b.iter(|| {
                    let idx = tables.partition_point(|table| table.metadata.key_range.max() < &key);
                    let table = tables
                        .get(idx)
                        .filter(|table| table.metadata.key_range.min() <= &key)
                        .expect("should exist");
                    table.id()
                });
            },
        );

        group.bench_function(
            format!("find segment in {segment_count} segments - linear search"),
            |b| {
                let version = tree.current_version();
                let level = version.level(0).expect("level should exist");
                let mut tables = level
                    .iter()
                    .flat_map(|run| run.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                tables.sort_by(|a, b| a.metadata.key_range.min().cmp(b.metadata.key_range.min()));

                b.iter(|| {
                    tables
                        .iter()
                        .find(|table| table.metadata.key_range.contains_key(&key))
                        .expect("should exist");
                });
            },
        );
    }
}

criterion_group!(benches, iterate_segments, find_segment);
criterion_main!(benches);
