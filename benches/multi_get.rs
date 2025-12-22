use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{get_tmp_folder, AbstractTree, Config, SeqNo, SequenceNumberCounter};

fn multi_get_vs_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_get_vs_get");
    group.sample_size(10);

    for num_keys in [1_000, 10_000, 50_000] {
        let folder = get_tmp_folder();

        {
            let tree = Config::new(
                &folder,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()
            .unwrap();

            for i in 0..num_keys {
                tree.insert(i.to_string(), i.to_string(), i as u64);
            }
            tree.flush_active_memtable((num_keys - 1) as u64).unwrap();
        }

        let keys: Vec<_> = (0..num_keys).map(|i| i.to_string().into_bytes()).collect();
        let key_slices: Vec<_> = keys.iter().map(|k| k.as_slice()).collect();

        group.bench_function(format!("multi_get {} keys", num_keys), |b| {
            let tree = Config::new(
                &folder,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()
            .unwrap();
            b.iter(|| {
                tree.multi_get(&key_slices, SeqNo::MAX).unwrap();
            })
        });

        group.bench_function(format!("get {} keys", num_keys), |b| {
            let tree = Config::new(
                &folder,
                SequenceNumberCounter::default(),
                SequenceNumberCounter::default(),
            )
            .open()
            .unwrap();
            b.iter(|| {
                for key in &key_slices {
                    tree.get(key, SeqNo::MAX).unwrap();
                }
            })
        });
    }
}

criterion_group!(benches, multi_get_vs_get);
criterion_main!(benches);
