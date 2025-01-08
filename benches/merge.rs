use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::merge::{BoxedIterator, Merger};
use lsm_tree::{mvcc_stream::MvccStream, InternalValue, Memtable};
use nanoid::nanoid;

fn merger(c: &mut Criterion) {
    for num in [2, 4, 8, 16, 30] {
        c.bench_function(&format!("Merge {num}"), |b| {
            let memtables = (0..num)
                .map(|_| {
                    let table = Memtable::default();

                    for _ in 0..100 {
                        table.insert(InternalValue::from_components(
                            nanoid!(),
                            vec![],
                            0,
                            lsm_tree::ValueType::Value,
                        ));
                    }

                    table
                })
                .collect::<Vec<_>>();

            b.iter_with_large_drop(|| {
                let iters = memtables
                    .iter()
                    .map(|x| x.iter().map(Ok))
                    .map(|x| Box::new(x) as BoxedIterator<'_>)
                    .collect();

                let merger = Merger::new(iters);

                assert_eq!(num * 100, merger.count());
            })
        });
    }
}

fn mvcc_stream(c: &mut Criterion) {
    for num in [2, 4, 8, 16, 30] {
        c.bench_function(&format!("MVCC stream {num} versions"), |b| {
            let memtables = (0..num)
                .map(|_| {
                    let table = Memtable::default();

                    for key in 'a'..='z' {
                        table.insert(InternalValue::from_components(
                            key.to_string(),
                            vec![],
                            num,
                            lsm_tree::ValueType::Value,
                        ));
                    }

                    table
                })
                .collect::<Vec<_>>();

            b.iter_with_large_drop(|| {
                let iters = memtables
                    .iter()
                    .map(|x| x.iter().map(Ok))
                    .map(|x| Box::new(x) as BoxedIterator<'_>)
                    .collect();

                let merger = MvccStream::new(Merger::new(iters));

                assert_eq!(26, merger.count());
            })
        });
    }
}

criterion_group!(benches, merger, mvcc_stream);
criterion_main!(benches);
