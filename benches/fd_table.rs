use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

fn file_descriptor_table(c: &mut Criterion) {
    use std::fs::File;

    let file = tempfile::NamedTempFile::new().unwrap();

    let mut group = c.benchmark_group("Get file descriptor");

    let id = (0, 523).into();
    let descriptor_table = lsm_tree::descriptor_table::FileDescriptorTable::new(1, 1);
    descriptor_table.insert(file.path(), id);

    group.bench_function("descriptor table", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            let guard = descriptor_table.access(&id).unwrap().unwrap();
            let _fd = guard.file.lock().unwrap();
        });
    });

    group.bench_function("fopen", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            File::open(file.path()).unwrap();
        });
    });
}

fn file_descriptor_table_threading(c: &mut Criterion) {
    use std::fs::File;

    let files_to_open = 1_000;

    let file = Box::leak(Box::new(tempfile::NamedTempFile::new().unwrap()));

    let mut group = c.benchmark_group("Get file descriptor (threaded)");
    group.throughput(criterion::Throughput::Elements(files_to_open as u64));

    for thread_count in [1, 2, 4, 8, 16] {
        let id = (0, 523).into();
        let descriptor_table = Arc::new(lsm_tree::descriptor_table::FileDescriptorTable::new(
            thread_count,
            thread_count,
        ));
        descriptor_table.insert(file.path(), id);

        group.bench_function(
            format!("descriptor table - {thread_count} threads"),
            |b: &mut criterion::Bencher<'_>| {
                b.iter(|| {
                    let threads = (0..thread_count)
                        .map(|_| {
                            let table = descriptor_table.clone();

                            std::thread::spawn(move || {
                                for _ in 0..(files_to_open / thread_count) {
                                    let guard = table.access(&id).unwrap().unwrap();
                                    let _fd = guard.file.lock().unwrap();
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
