use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn file_descriptor_table(c: &mut Criterion) {
    use std::fs::File;

    let file = tempfile::NamedTempFile::new().unwrap();

    let mut group = c.benchmark_group("Get file descriptor");

    let id = (0, 523).into();
    let descriptor_table = lsm_tree::descriptor_table::DescriptorTable::new(100);
    descriptor_table.insert_for_table(id, Arc::new(file.into_file()));

    group.bench_function("descriptor table", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            let _guard = descriptor_table.access_for_table(&id).unwrap();
        });
    });

    let file = tempfile::NamedTempFile::new().unwrap();

    group.bench_function("fopen", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            File::open(file.path()).unwrap();
        });
    });
}

fn file_descriptor_table_threading(c: &mut Criterion) {
    use std::fs::File;

    let files_to_open = 1_000;

    let file = tempfile::NamedTempFile::new().unwrap();
    let file = Arc::new(file.into_file());

    let mut group = c.benchmark_group("Get file descriptor (threaded)");
    group.throughput(criterion::Throughput::Elements(files_to_open as u64));

    for thread_count in [1, 2, 4, 8, 16] {
        let id = (0, 523).into();
        let descriptor_table = Arc::new(lsm_tree::descriptor_table::DescriptorTable::new(100));
        descriptor_table.insert_for_table(id, file.clone());

        group.bench_function(
            format!("descriptor table - {thread_count} threads"),
            |b: &mut criterion::Bencher<'_>| {
                b.iter(|| {
                    let threads = (0..thread_count)
                        .map(|_| {
                            let table = descriptor_table.clone();

                            std::thread::spawn(move || {
                                for _ in 0..(files_to_open / thread_count) {
                                    let _guard = table.access_for_table(&id).unwrap();
                                }
                            })
                        })
                        .collect::<Vec<_>>();

                    for thread in threads {
                        thread.join().unwrap();
                    }
                });
            },
        );

        let file = Box::leak(Box::new(tempfile::NamedTempFile::new().unwrap()));

        group.bench_function(
            format!("fopen - {thread_count} threads"),
            |b: &mut criterion::Bencher<'_>| {
                b.iter(|| {
                    let threads = (0..thread_count)
                        .map(|_| {
                            let path = file.path();

                            std::thread::spawn(move || {
                                for _ in 0..(files_to_open / thread_count) {
                                    File::open(path).unwrap();
                                }
                            })
                        })
                        .collect::<Vec<_>>();

                    for thread in threads {
                        thread.join().unwrap();
                    }
                });
            },
        );
    }
}

criterion_group!(
    benches,
    file_descriptor_table,
    file_descriptor_table_threading,
);
criterion_main!(benches);
