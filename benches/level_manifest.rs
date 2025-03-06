use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{AbstractTree, Config};

fn iterate_segments(c: &mut Criterion) {
    let mut group = c.benchmark_group("Iterate level manifest");
    group.sample_size(10);

    std::fs::create_dir_all(".bench").unwrap();

    for segment_count in [0, 1, 5, 10, 100, 500, 1_000, 2_000, 4_000] {
        group.bench_function(format!("iterate {segment_count} segments"), |b| {
            let folder = tempfile::tempdir_in(".bench").unwrap();
            let tree = Config::new(folder).data_block_size(1_024).open().unwrap();

            for x in 0_u64..segment_count {
                tree.insert("a", "b", x);
                tree.flush_active_memtable(0).unwrap();
            }

            let levels = tree.levels.read().unwrap();

            b.iter(|| {
                assert_eq!(levels.iter().count(), segment_count as usize);
            });
        });
    }
}

fn find_segment(c: &mut Criterion) {
    let mut group = c.benchmark_group("Find segment in disjoint level");
    group.sample_size(10);

    std::fs::create_dir_all(".bench").unwrap();

    for segment_count in [1u16, 4, 5, 10, 100, 1_000] {
        let folder = tempfile::tempdir_in(".bench").unwrap();
        let tree = Config::new(folder).data_block_size(1_024).open().unwrap();

        for x in 0..segment_count {
            tree.insert(x.to_be_bytes(), "", x.into());
            tree.flush_active_memtable(0).unwrap();
        }

        let key = (segment_count / 2).to_be_bytes();

        group.bench_function(
            format!("find segment in {segment_count} segments - binary search"),
            |b| {
                let levels = tree.levels.read().unwrap();
                let first_level = levels.levels.first().expect("should exist");

                b.iter(|| {
                    first_level
                        .as_disjoint()
                        .expect("should be disjoint")
                        .get_segment_containing_key(&key)
                        .expect("should exist")
                });
            },
        );

        group.bench_function(
            format!("find segment in {segment_count} segments - linear search"),
            |b| {
                let levels = tree.levels.read().unwrap();
                let first_level = levels.levels.first().expect("should exist");

                b.iter(|| {
                    first_level
                        .iter()
                        .find(|x| x.metadata.key_range.contains_key(&key))
                        .expect("should exist");
                });
            },
        );
    }
}

criterion_group!(benches, iterate_segments, find_segment);
criterion_main!(benches);
